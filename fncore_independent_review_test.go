package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fncoreCommittedSentinelWriter struct {
	started chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
	calls   atomic.Int32
}

func (w *fncoreCommittedSentinelWriter) Write([]byte) (int, error) {
	w.calls.Add(1)
	w.once.Do(func() { close(w.started) })
	<-w.release
	return 0, w.err
}

func TestFNCORECommittedWriterErrorWinsCallerAndClientCancellation(t *testing.T) {
	for _, mode := range []string{"caller cancellation", "client shutdown"} {
		t.Run(mode, func(t *testing.T) {
			writerErr := errors.New("committed writer sentinel: " + mode)
			name := strings.ReplaceAll(mode, " ", "-")
			primary, provider := breakerProvider(
				"committed-writer-"+name,
				"committed-writer-"+name+".invalid:119",
				func(int, string) []byte {
					return yencSinglePart([]byte("committed writer payload"), "writer.bin")
				},
			)
			backup, backupProvider := breakerProvider(
				provider.ID+"-backup",
				provider.Host+"-backup",
				func(int, string) []byte {
					return yencSinglePart([]byte("must not replay"), "backup.bin")
				},
			)
			backupProvider.Backup = true
			client := newBreakerClient(t, newBreakerFakeClock(), provider, backupProvider)
			group := fncoreProviderGroup(t, client, provider.ID)
			providerErrors := group.stats.Errors.Load()
			writer := &fncoreCommittedSentinelWriter{
				started: make(chan struct{}),
				release: make(chan struct{}),
				err:     writerErr,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := client.BodyStream(ctx, "fixture@example.invalid", writer)
				result <- err
			}()
			select {
			case <-writer.started:
			case <-time.After(5 * time.Second):
				t.Fatal("caller writer did not reach committed blocking call")
			}

			closeDone := make(chan error, 1)
			switch mode {
			case "caller cancellation":
				cancel()
			case "client shutdown":
				go func() { closeDone <- client.Close() }()
				select {
				case <-client.ctx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("client shutdown did not publish lifecycle cancellation")
				}
			}
			select {
			case earlyErr := <-result:
				close(writer.release)
				t.Fatalf("%s returned before committed writer settled: %v", mode, earlyErr)
			default:
			}
			close(writer.release)

			var terminalErr error
			select {
			case terminalErr = <-result:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not settle after writer returned", mode)
			}
			transportErr := requireFNCORELocalWriterError(t, terminalErr, writerErr)
			attempt := transportErr.Attempts[0]
			if attempt.Operation != OperationBody || attempt.ProviderID != provider.ID || attempt.Outcome != OutcomeLocalFailure {
				t.Fatalf("%s attempt = %+v, want one factual local BODY failure", mode, attempt)
			}
			if got := writer.calls.Load(); got != 1 {
				t.Fatalf("%s writer calls = %d, want exactly one", mode, got)
			}
			if got := primary.commandCount("BODY"); got != 1 {
				t.Fatalf("%s primary BODY commands = %d, want one", mode, got)
			}
			if got := backup.commandCount("BODY"); got != 0 {
				t.Fatalf("%s backup BODY commands = %d, want zero", mode, got)
			}
			if got := group.stats.Errors.Load(); got != providerErrors {
				t.Fatalf("%s provider errors = %d, want unchanged %d", mode, got, providerErrors)
			}
			breakerStats := group.breaker.snapshot()
			if breakerStats.State != CircuitBreakerClosed || breakerStats.ProbeInFlight || breakerStats.QualifyingFailures != 0 {
				t.Fatalf("%s local writer failure changed breaker: %+v", mode, breakerStats)
			}
			if mode == "client shutdown" {
				select {
				case err := <-closeDone:
					if err != nil {
						t.Fatalf("client Close() error = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("client Close() did not join committed writer settlement")
				}
			}
		})
	}
}

func TestFNCOREHeadBodyCancellationDrainsWithinBudgetAndReusesSocket(t *testing.T) {
	payload := bytes.Repeat([]byte("d"), 32*1024)
	response := yencSinglePart(payload, "bounded-drain.bin")
	split := len(response) / 2
	clientConn, serverConn := net.Pipe()
	prefixConsumed := make(chan struct{})
	releaseTail := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write(response[:split]); err != nil {
			serverDone <- err
			return
		}
		close(prefixConsumed)
		<-releaseTail
		if _, err := serverConn.Write(response[split:]); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 2 <reuse@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	stats := &providerStats{}
	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), clientConn, 1, reqCh, nil, Auth{}, "", nil, stats,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	request := &Request{
		Ctx:          ctx,
		Payload:      []byte("BODY <bounded-drain@example.invalid>\r\n"),
		RespCh:       make(chan Response, 1),
		BodyWriter:   io.Discard,
		ValidateBody: true,
		submittedAt:  time.Now(),
	}
	go connection.Run()
	reqCh <- request
	select {
	case <-prefixConsumed:
	case <-time.After(5 * time.Second):
		t.Fatal("BODY response did not reach the response-head drain seam")
	}
	cancel()
	close(releaseTail)
	responseResult := awaitFNCOREPhaseResponse(t, request.RespCh, "bounded BODY cancellation")
	if !errors.Is(responseResult.Err, context.Canceled) {
		t.Fatalf("bounded BODY cancellation error = %v, want context cancellation", responseResult.Err)
	}
	if responseResult.StatusCode != 222 || len(responseResult.Attempts) != 1 || responseResult.Attempts[0].Outcome != OutcomeCancellation {
		t.Fatalf("bounded BODY cancellation response = %+v, want complete framing and one cancellation attempt", responseResult)
	}
	if got := stats.Errors.Load(); got != 0 {
		t.Fatalf("provider errors after bounded cancellation drain = %d, want zero", got)
	}

	reuse := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <reuse@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- reuse
	if reuseResponse := awaitFNCOREPhaseResponse(t, reuse.RespCh, "post-drain reuse"); reuseResponse.Err != nil || reuseResponse.StatusCode != 223 {
		t.Fatalf("post-drain same-socket response = %+v, want success", reuseResponse)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("bounded-drain server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bounded-drain server did not observe safe reuse")
	}
	select {
	case <-connection.Done():
		t.Fatal("bounded complete BODY drain retired the synchronized socket")
	default:
	}
	if got := stats.PipelineInUse.Load(); got != 0 {
		t.Fatalf("pipeline occupancy after bounded drain and reuse = %d, want zero", got)
	}
}

type fncoreNthBlockingWriteConn struct {
	net.Conn
	blockAt int32
	writes  atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *fncoreNthBlockingWriteConn) Write(p []byte) (int, error) {
	if c.writes.Add(1) == c.blockAt {
		c.once.Do(func() { close(c.started) })
		<-c.release
	}
	return c.Conn.Write(p)
}

func TestFNCOREAttemptDurationsPartitionTransportPhasesExactly(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	secondWriteStarted := make(chan struct{})
	releaseSecondWrite := make(chan struct{})
	blockedConn := &fncoreNthBlockingWriteConn{
		Conn:    clientConn,
		blockAt: 2,
		started: secondWriteStarted,
		release: releaseSecondWrite,
	}
	secondCommandSeen := make(chan struct{})
	releaseFirstResponse := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		close(secondCommandSeen)
		<-releaseFirstResponse
		if _, err := serverConn.Write([]byte("223 1 <first@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 2 <second@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	reqCh := make(chan *Request, 2)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), blockedConn, 2, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	first := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <first@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	second := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <second@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- first
	reqCh <- second
	go connection.Run()
	select {
	case <-secondWriteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second command did not reach blocked Flush")
	}
	close(releaseSecondWrite)
	select {
	case <-secondCommandSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the second flushed command")
	}
	deadline := time.After(5 * time.Second)
	for second.writtenAt.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("second request was not marked written after Flush")
		default:
			runtime.Gosched()
		}
	}
	close(releaseFirstResponse)
	if response := awaitFNCOREPhaseResponse(t, first.RespCh, "first timing request"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("first timing response = %+v, want success", response)
	}
	secondResponse := awaitFNCOREPhaseResponse(t, second.RespCh, "second timing request")
	if secondResponse.Err != nil || secondResponse.StatusCode != 223 || len(secondResponse.Attempts) != 1 {
		t.Fatalf("second timing response = %+v, want one successful attempt", secondResponse)
	}
	writtenAt := second.writtenAt.Load()
	headAt := second.responseHeadAt.Load()
	if writtenAt == 0 || headAt < writtenAt {
		t.Fatalf("timing boundaries written=%d head=%d, want ordered non-zero timestamps", writtenAt, headAt)
	}
	attempt := secondResponse.Attempts[0]
	wantPool := time.Unix(0, writtenAt).Sub(second.submittedAt)
	wantHead := time.Duration(headAt - writtenAt)
	if attempt.PoolQueueDuration != wantPool {
		t.Fatalf("PoolQueueDuration = %v, want submitted→written %v", attempt.PoolQueueDuration, wantPool)
	}
	if attempt.PipelineHeadWaitDuration != wantHead {
		t.Fatalf("PipelineHeadWaitDuration = %v, want written→responseHead %v", attempt.PipelineHeadWaitDuration, wantHead)
	}
	partitioned := attempt.PoolQueueDuration + attempt.PipelineHeadWaitDuration + attempt.ResponseServiceDuration
	if elapsed := time.Since(second.submittedAt); partitioned < 0 || partitioned > elapsed {
		t.Fatalf("partitioned attempt duration = %v, elapsed before receipt = %v; phases overlap or are invalid", partitioned, elapsed)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("timing server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timing server did not complete")
	}
}
