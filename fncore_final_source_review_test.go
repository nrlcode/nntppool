package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fncoreCancellationBarrierWriter establishes decoded progress while keeping
// the response reader behind a deterministic test-owned barrier. Release is
// idempotent so every failure path can unwind its net.Pipe fixture.
type fncoreCancellationBarrierWriter struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newFNCORECancellationBarrierWriter() *fncoreCancellationBarrierWriter {
	return &fncoreCancellationBarrierWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *fncoreCancellationBarrierWriter) Write(p []byte) (int, error) {
	w.startedOnce.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

func (w *fncoreCancellationBarrierWriter) Started() <-chan struct{} { return w.started }
func (w *fncoreCancellationBarrierWriter) Release() {
	w.releaseOnce.Do(func() { close(w.release) })
}

type fncoreOneShotPostReader struct {
	mu       sync.Mutex
	data     []byte
	offset   int
	eofReads int
}

func (r *fncoreOneShotPostReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.offset == len(r.data) {
		r.eofReads++
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *fncoreOneShotPostReader) snapshot() (offset, eofReads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.offset, r.eofReads
}

func TestFNCORECommittedPostBodyIsNeverReplayedAcrossMainProviders(t *testing.T) {
	article := []byte("From: regression@example.invalid\r\n\r\none-shot article\r\n.\r\n")

	t.Run("final response loss is terminal", func(t *testing.T) {
		firstBody := make(chan []byte, 1)
		secondCommand := make(chan struct{})
		var secondCommandOnce sync.Once

		firstFactory := func(context.Context) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go func() {
				defer func() { _ = serverConn.Close() }()
				if _, err := serverConn.Write([]byte("200 first POST server ready\r\n")); err != nil {
					return
				}
				reader := bufio.NewReader(serverConn)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				if _, err := serverConn.Write([]byte("340 send article\r\n")); err != nil {
					return
				}
				received := make([]byte, len(article))
				if _, err := io.ReadFull(reader, received); err != nil {
					return
				}
				firstBody <- received
				// Lose the transport only after accepting the complete one-shot
				// article. There is deliberately no final 240/441 response.
			}()
			return clientConn, nil
		}
		secondFactory := func(context.Context) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go func() {
				defer func() { _ = serverConn.Close() }()
				if _, err := serverConn.Write([]byte("200 second POST server ready\r\n")); err != nil {
					return
				}
				reader := bufio.NewReader(serverConn)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				secondCommandOnce.Do(func() { close(secondCommand) })
				if _, err := serverConn.Write([]byte("340 send article\r\n")); err != nil {
					return
				}
				// An incorrect replay sees an exhausted reader. Sending 240 now
				// makes evidence masking deterministic instead of hanging the test.
				_, _ = serverConn.Write([]byte("240 incorrectly accepted replay\r\n"))
				_, _ = io.Copy(io.Discard, reader)
			}()
			return clientConn, nil
		}

		client := fncoreClient(t,
			Provider{ID: "post-main-1", Host: "post-main-1.invalid:119", Factory: firstFactory, Connections: 1, Inflight: 1, SkipPing: true},
			Provider{ID: "post-main-2", Host: "post-main-2.invalid:119", Factory: secondFactory, Connections: 1, Inflight: 1, SkipPing: true},
		)
		body := &fncoreOneShotPostReader{data: article}
		response := awaitFNCOREPhaseResponse(t, client.sendPost(context.Background(), body), "committed POST final-response loss")

		select {
		case received := <-firstBody:
			if !bytes.Equal(received, article) {
				t.Fatalf("first provider article = %q, want complete one-shot body", received)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("first provider did not consume and flush the complete article")
		}
		if response.Err == nil || response.ProviderID != "post-main-1" {
			t.Fatalf("committed POST response = %+v, want terminal first-provider transport evidence", response)
		}
		if len(response.Attempts) != 1 || response.Attempts[0].ProviderID != "post-main-1" ||
			response.Attempts[0].Operation != OperationPost || response.Attempts[0].Outcome != OutcomeTransportFailure {
			t.Fatalf("committed POST attempts = %+v, want factual first-provider failure", response.Attempts)
		}
		select {
		case <-secondCommand:
			t.Fatal("complete one-shot POST body was replayed to a second main provider")
		default:
		}
		if offset, eofReads := body.snapshot(); offset != len(article) || eofReads != 1 {
			t.Fatalf("one-shot reader state = offset %d EOF reads %d, want %d/1", offset, eofReads, len(article))
		}
	})

	t.Run("stopped before transport may fall through", func(t *testing.T) {
		firstDialFailure := errors.New("deterministic pretransport dial failure")
		var firstDials atomic.Int32
		firstFactory := func(context.Context) (net.Conn, error) {
			firstDials.Add(1)
			return nil, firstDialFailure
		}
		secondBody := make(chan []byte, 1)
		secondFactory := func(context.Context) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go func() {
				defer func() { _ = serverConn.Close() }()
				if _, err := serverConn.Write([]byte("200 fallback POST server ready\r\n")); err != nil {
					return
				}
				reader := bufio.NewReader(serverConn)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				if _, err := serverConn.Write([]byte("340 send article\r\n")); err != nil {
					return
				}
				received := make([]byte, len(article))
				if _, err := io.ReadFull(reader, received); err != nil {
					return
				}
				secondBody <- received
				_, _ = serverConn.Write([]byte("240 article posted\r\n"))
			}()
			return clientConn, nil
		}

		client := fncoreClient(t,
			Provider{ID: "post-pretransport", Host: "post-pretransport.invalid:119", Factory: firstFactory, Connections: 1, Inflight: 1, SkipPing: true},
			Provider{ID: "post-safe-fallback", Host: "post-safe-fallback.invalid:119", Factory: secondFactory, Connections: 1, Inflight: 1, SkipPing: true},
		)
		body := &fncoreOneShotPostReader{data: article}
		response := awaitFNCOREPhaseResponse(t, client.sendPost(context.Background(), body), "safe pretransport POST fallback")
		if response.Err != nil || response.StatusCode != 240 || response.ProviderID != "post-safe-fallback" {
			t.Fatalf("pretransport POST fallback response = %+v, want second-provider success", response)
		}
		select {
		case received := <-secondBody:
			if !bytes.Equal(received, article) {
				t.Fatalf("safe fallback article = %q, want complete one-shot body", received)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("safe fallback provider did not receive the article")
		}
		if firstDials.Load() == 0 {
			t.Fatal("first provider did not exercise the pretransport failure guard")
		}
		if offset, eofReads := body.snapshot(); offset != len(article) || eofReads != 1 {
			t.Fatalf("safe fallback reader state = offset %d EOF reads %d, want %d/1", offset, eofReads, len(article))
		}
	})
}

type fncoreStaleStallDeadlineConn struct {
	net.Conn
	providerSelected chan struct{}
	releaseProvider  chan struct{}
	drainApplied     chan struct{}
	providerOnce     sync.Once
	releaseOnce      sync.Once
	drainOnce        sync.Once
}

func (c *fncoreStaleStallDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		return c.Conn.SetReadDeadline(deadline)
	}
	remaining := time.Until(deadline)
	if remaining > 500*time.Millisecond {
		blocked := false
		c.providerOnce.Do(func() {
			blocked = true
			close(c.providerSelected)
		})
		if blocked {
			<-c.releaseProvider
		}
		return c.Conn.SetReadDeadline(deadline)
	}
	err := c.Conn.SetReadDeadline(deadline)
	if err == nil {
		c.drainOnce.Do(func() { close(c.drainApplied) })
	}
	return err
}

func (c *fncoreStaleStallDeadlineConn) release() {
	c.releaseOnce.Do(func() { close(c.releaseProvider) })
}

func TestFNCOREProgressedBodyCancellationCannotBeOverwrittenByStaleStallDeadline(t *testing.T) {
	payload := bytes.Repeat([]byte("s"), 32*1024)
	response := yencSinglePart(payload, "stale-stall.bin")
	split := len(response) / 2
	clientConn, serverConn := net.Pipe()
	controlled := &fncoreStaleStallDeadlineConn{
		Conn:             clientConn,
		providerSelected: make(chan struct{}),
		releaseProvider:  make(chan struct{}),
		drainApplied:     make(chan struct{}),
	}
	t.Cleanup(controlled.release)
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 stale-deadline server ready\r\n")); err != nil {
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
		_, err := io.Copy(io.Discard, reader)
		serverDone <- err
	}()

	stats := &providerStats{}
	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), controlled, 1, reqCh, nil, Auth{}, "", nil, stats,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "stale-stall-provider"
	connection.stallTimeout = 2 * time.Second
	connection.abandonedDrainTimeout = 75 * time.Millisecond
	connection.abandonedDrainBytes = 64 * 1024
	t.Cleanup(func() {
		controlled.release()
		_ = serverConn.Close()
		_ = connection.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	request := &Request{
		Ctx:          ctx,
		Payload:      []byte("BODY <stale-stall@example.invalid>\r\n"),
		RespCh:       make(chan Response, 1),
		BodyWriter:   io.Discard,
		ValidateBody: true,
		submittedAt:  time.Now(),
	}
	go connection.Run()
	reqCh <- request
	select {
	case <-controlled.providerSelected:
	case <-time.After(5 * time.Second):
		t.Fatal("progressed BODY did not reach the controlled stale stall-deadline application")
	}

	cancelStarted := time.Now()
	cancel()
	// The broken implementation applies the short drain deadline concurrently;
	// a serialized implementation may defer it until the stale call returns.
	// Release in either case, but only the public bounded-settlement assertion
	// below determines the result.
	select {
	case <-controlled.drainApplied:
		controlled.release()
	case <-time.After(200 * time.Millisecond):
		controlled.release()
	}

	var result Response
	select {
	case result = <-request.RespCh:
	case <-time.After(600 * time.Millisecond):
		_ = serverConn.Close()
		_ = connection.Close()
		t.Fatal("progressed BODY cancellation exceeded its abandoned-drain bound after a stale deadline decision")
	}
	if elapsed := time.Since(cancelStarted); elapsed > 600*time.Millisecond {
		t.Fatalf("progressed BODY cancellation settled in %v, want bounded drain", elapsed)
	}
	if !errors.Is(result.Err, context.Canceled) || len(result.Attempts) != 1 ||
		result.Attempts[0].Outcome != OutcomeCancellation || result.Attempts[0].ProviderID != "stale-stall-provider" {
		t.Fatalf("stale-deadline cancellation response = %+v", result)
	}
	if got := stats.Errors.Load(); got != 0 {
		t.Fatalf("stale-deadline caller cancellation provider errors = %d, want zero", got)
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("incomplete cancellation drain did not retire the transport")
	}
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale-deadline server fixture did not unwind")
	}
}

type fncoreTrackedClosableBody struct {
	readCalled chan struct{}
	closed     chan struct{}
	readOnce   sync.Once
	closeOnce  sync.Once
}

func newFNCORETrackedClosableBody() *fncoreTrackedClosableBody {
	return &fncoreTrackedClosableBody{
		readCalled: make(chan struct{}),
		closed:     make(chan struct{}),
	}
}

func (b *fncoreTrackedClosableBody) Read([]byte) (int, error) {
	b.readOnce.Do(func() { close(b.readCalled) })
	return 0, io.EOF
}

func (b *fncoreTrackedClosableBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func waitFNCOREChannelDepth(t *testing.T, ch chan *Request, want int, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(ch) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s channel depth = %d, want %d", label, len(ch), want)
		}
		runtime.Gosched()
	}
}

func TestFNCOREQueuedPostCancellationDoesNotWaitForUnrelatedBody(t *testing.T) {
	barrier := newFNCORECancellationBarrierWriter()
	t.Cleanup(barrier.Release)
	serverDone := make(chan error, 1)
	factory := func(context.Context) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		go func() {
			defer func() { _ = serverConn.Close() }()
			if _, err := serverConn.Write([]byte("200 queued-cancel server ready\r\n")); err != nil {
				serverDone <- err
				return
			}
			reader := bufio.NewReader(serverConn)
			if _, err := reader.ReadString('\n'); err != nil {
				serverDone <- err
				return
			}
			if _, err := serverConn.Write(yencSinglePart([]byte("unrelated body"), "unrelated.bin")); err != nil {
				serverDone <- err
				return
			}
			_ = serverConn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			command, err := reader.ReadString('\n')
			if command != "" {
				serverDone <- errors.New("canceled queued POST reached the transport")
				return
			}
			if err == nil {
				serverDone <- errors.New("queued-cancel server read unexpectedly succeeded")
				return
			}
			serverDone <- nil
		}()
		return clientConn, nil
	}
	provider := Provider{
		ID:           "queued-post-provider",
		Host:         "queued-post-provider.invalid:119",
		Factory:      factory,
		Connections:  1,
		Inflight:     1,
		StatInflight: 1,
		SkipPing:     true,
	}
	client := fncoreClient(t, provider)
	unrelatedDone := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "unrelated@example.invalid", barrier)
		unrelatedDone <- err
	}()
	select {
	case <-barrier.Started():
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated BODY did not occupy the sole provider inflight slot")
	}

	group := fncoreProviderGroup(t, client, provider.ID)
	postCtx, cancelPost := context.WithCancel(context.Background())
	body := newFNCORETrackedClosableBody()
	t.Cleanup(func() { _ = body.Close() })
	postResponse := client.sendPost(postCtx, body)
	waitFNCOREChannelDepth(t, group.reqCh, 1, "queued POST")
	cancelStarted := time.Now()
	cancelPost()

	var response Response
	responseSeen := false
	closedSeen := false
	responseEvents := postResponse
	closedEvents := body.closed
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for !responseSeen || !closedSeen {
		select {
		case next, ok := <-responseEvents:
			if !ok {
				barrier.Release()
				t.Fatal("queued POST response channel closed without a response")
			}
			response = next
			responseSeen = true
			responseEvents = nil
		case <-closedEvents:
			closedSeen = true
			closedEvents = nil
		case <-body.readCalled:
			barrier.Release()
			t.Fatal("queued canceled POST body was read before transport ownership")
		case <-timer.C:
			barrier.Release()
			t.Fatalf("queued POST cancellation waited for unrelated BODY: response=%t bodyClosed=%t", responseSeen, closedSeen)
		}
	}
	if elapsed := time.Since(cancelStarted); elapsed > 500*time.Millisecond {
		barrier.Release()
		t.Fatalf("queued POST cancellation settled in %v, want prompt pretransport completion", elapsed)
	}
	if !errors.Is(response.Err, context.Canceled) {
		barrier.Release()
		t.Fatalf("queued POST cancellation response = %+v", response)
	}

	barrier.Release()
	select {
	case err := <-unrelatedDone:
		if err != nil {
			t.Fatalf("unrelated BODY error after queued cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated BODY did not settle after fixture release")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("queued-cancel server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued-cancel server fixture did not finish")
	}
}

type fncoreBlockingNonClosableBody struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newFNCOREBlockingNonClosableBody() *fncoreBlockingNonClosableBody {
	return &fncoreBlockingNonClosableBody{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *fncoreBlockingNonClosableBody) Read([]byte) (int, error) {
	b.startedOnce.Do(func() { close(b.started) })
	<-b.release
	return 0, io.EOF
}

func (b *fncoreBlockingNonClosableBody) Release() {
	b.releaseOnce.Do(func() { close(b.release) })
}

// fncorePostResponseReadBarrierConn blocks the second transport Read after it
// is armed. The first read consumes the deliberately released earlier FIFO
// response; the second is therefore the POST response read whose status the
// server is still withholding.
type fncorePostResponseReadBarrierConn struct {
	net.Conn
	armed       atomic.Bool
	reads       atomic.Int32
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newFNCOREPostResponseReadBarrierConn(conn net.Conn) *fncorePostResponseReadBarrierConn {
	return &fncorePostResponseReadBarrierConn{
		Conn:    conn,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *fncorePostResponseReadBarrierConn) Read(p []byte) (int, error) {
	if c.armed.Load() && c.reads.Add(1) == 2 {
		c.enteredOnce.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.Conn.Read(p)
}

func (c *fncorePostResponseReadBarrierConn) Arm() { c.armed.Store(true) }
func (c *fncorePostResponseReadBarrierConn) Release() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func TestFNCOREPendingCancelledPostNeverReadsNonClosableBody(t *testing.T) {
	for _, status := range []struct {
		name string
		line string
	}{
		{name: "continue 340", line: "340 send article\r\n"},
		{name: "reject 440", line: "440 posting not permitted\r\n"},
	} {
		t.Run(status.name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			controlled := newFNCOREPostResponseReadBarrierConn(clientConn)
			commandsSeen := make(chan struct{})
			releaseEarlier := make(chan struct{})
			releasePostStatus := make(chan struct{})
			var releaseEarlierOnce sync.Once
			var releasePostStatusOnce sync.Once
			releaseEarlierResponse := func() { releaseEarlierOnce.Do(func() { close(releaseEarlier) }) }
			releasePOSTResponse := func() { releasePostStatusOnce.Do(func() { close(releasePostStatus) }) }
			t.Cleanup(releaseEarlierResponse)
			t.Cleanup(releasePOSTResponse)
			t.Cleanup(controlled.Release)
			serverDone := make(chan error, 1)
			go func() {
				defer func() { _ = serverConn.Close() }()
				if _, err := serverConn.Write([]byte("200 pending POST server ready\r\n")); err != nil {
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
				close(commandsSeen)
				<-releaseEarlier
				if _, err := serverConn.Write([]byte("223 1 <earlier-post@example.invalid> exists\r\n")); err != nil {
					serverDone <- err
					return
				}
				<-releasePostStatus
				if _, err := serverConn.Write([]byte(status.line)); err != nil {
					serverDone <- err
					return
				}
				if status.line[0:3] == "340" {
					if _, err := serverConn.Write([]byte("240 final response after canceled upload\r\n")); err != nil {
						serverDone <- err
						return
					}
				}
				_ = serverConn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
				_, err := reader.ReadByte()
				serverDone <- err
			}()

			reqCh := make(chan *Request, 2)
			connection, err := newNNTPConnectionFromConn(
				context.Background(), controlled, 2, reqCh, nil, Auth{}, "", nil, nil,
			)
			if err != nil {
				t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
			}
			body := newFNCOREBlockingNonClosableBody()
			t.Cleanup(func() {
				body.Release()
				releaseEarlierResponse()
				releasePOSTResponse()
				controlled.Release()
				_ = serverConn.Close()
				_ = connection.Close()
			})
			controlled.Arm()
			earlier := &Request{
				Ctx:         context.Background(),
				Payload:     []byte("STAT <earlier-post@example.invalid>\r\n"),
				RespCh:      make(chan Response, 1),
				submittedAt: time.Now(),
			}
			postCtx, cancelPost := context.WithCancel(context.Background())
			defer cancelPost()
			post := &Request{
				Ctx:         postCtx,
				Payload:     []byte("POST\r\n"),
				PayloadBody: body,
				PostMode:    true,
				RespCh:      make(chan Response, 1),
				submittedAt: time.Now(),
			}
			go connection.Run()
			reqCh <- earlier
			reqCh <- post
			select {
			case <-commandsSeen:
			case <-time.After(5 * time.Second):
				t.Fatal("STAT and POST did not become FIFO-pending")
			}
			releaseEarlierResponse()
			if response := awaitFNCOREPhaseResponse(t, earlier.RespCh, "earlier pending-POST STAT"); response.Err != nil || response.StatusCode != 223 {
				t.Fatalf("earlier pending-POST response = %+v", response)
			}
			select {
			case <-controlled.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("client reader did not enter the withheld POST-response read")
			}
			cancelPost()
			releasePOSTResponse()
			controlled.Release()

			var response Response
			responseReceived := false
			responseClosed := false
			serverSettled := false
			var serverResult error
			serverResultReceived := false
			readAfterCancellation := false
			responseEvents := (<-chan Response)(post.RespCh)
			bodyEvents := (<-chan struct{})(body.started)
			serverEvents := (<-chan error)(serverDone)
			settlementTimer := time.NewTimer(2 * time.Second)
			defer settlementTimer.Stop()
			for (!responseReceived && !responseClosed) || !serverSettled {
				select {
				case <-bodyEvents:
					readAfterCancellation = true
					bodyEvents = nil
					body.Release()
				case next, ok := <-responseEvents:
					if !ok {
						responseClosed = true
					} else {
						response = next
						responseReceived = true
					}
					responseEvents = nil
				case result, ok := <-serverEvents:
					if ok {
						serverResult = result
						serverResultReceived = true
					}
					serverSettled = true
					serverEvents = nil
				case <-settlementTimer.C:
					select {
					case <-body.started:
						readAfterCancellation = true
					default:
					}
					body.Release()
					if readAfterCancellation {
						t.Fatalf("canceled FIFO-pending POST began body Read after %s", status.name)
					}
					t.Fatal("canceled FIFO-pending POST request/server settlement timed out")
				}
			}
			// A response channel or server result may win the select immediately
			// after Read begins. Sample the monotonic body event once more before
			// evaluating response settlement so that violation always has priority.
			select {
			case <-body.started:
				readAfterCancellation = true
				body.Release()
			default:
			}
			if readAfterCancellation {
				t.Fatalf("canceled FIFO-pending POST began body Read after %s", status.name)
			}
			if responseClosed {
				t.Fatal("canceled FIFO-pending POST response channel closed without a response")
			}
			if !serverResultReceived {
				t.Fatal("pending POST server result channel closed without a result")
			}
			if serverResult == nil {
				t.Fatal("pending POST server observed unexpected body data")
			}
			if !errors.Is(response.Err, context.Canceled) || len(response.Attempts) != 1 ||
				response.Attempts[0].Outcome != OutcomeCancellation {
				t.Fatalf("canceled pending POST response = %+v", response)
			}
			select {
			case <-body.started:
				t.Fatalf("canceled FIFO-pending POST read non-closable body after settling for %s", status.name)
			default:
			}
		})
	}
}

var errFNCOREDeferredDeadlineClear = errors.New("deterministic deferred-drain deadline clear failure")
var errFNCOREDeadlineApplication = errors.New("deterministic read-deadline application failure")

type fncoreFailDeadlineApplicationConn struct {
	net.Conn
	failed     chan struct{}
	failOnce   sync.Once
	failedCall atomic.Bool
}

func (c *fncoreFailDeadlineApplicationConn) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		failed := false
		c.failOnce.Do(func() {
			failed = true
			c.failedCall.Store(true)
			close(c.failed)
		})
		if failed {
			return errFNCOREDeadlineApplication
		}
	}
	return c.Conn.SetReadDeadline(deadline)
}

func TestFNCOREReadDeadlineApplicationFailureIsTerminal(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	controlled := &fncoreFailDeadlineApplicationConn{
		Conn:   clientConn,
		failed: make(chan struct{}),
	}
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 deadline-application server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		<-controlled.failed
		_, err := serverConn.Write([]byte("223 1 <deadline-application@example.invalid> exists\r\n"))
		serverDone <- err
	}()

	stats := &providerStats{}
	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), controlled, 1, reqCh, nil, Auth{}, "", nil, stats,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "deadline-application-provider"
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	request := &Request{
		Ctx:             context.Background(),
		Payload:         []byte("STAT <deadline-application@example.invalid>\r\n"),
		RespCh:          make(chan Response, 1),
		submittedAt:     time.Now(),
		responseTimeout: 250 * time.Millisecond,
	}
	go connection.Run()
	reqCh <- request
	response := awaitFNCOREPhaseResponse(t, request.RespCh, "read-deadline application failure")
	if !controlled.failedCall.Load() {
		t.Fatal("test fixture did not inject a read-deadline application failure")
	}
	if !errors.Is(response.Err, errFNCOREDeadlineApplication) || response.StatusCode != 0 {
		t.Fatalf("deadline application response = %+v, want terminal syscall failure", response)
	}
	if len(response.Attempts) != 1 || response.Attempts[0].ProviderID != "deadline-application-provider" ||
		response.Attempts[0].Outcome != OutcomeTransportFailure ||
		!errors.Is(response.Attempts[0].Cause, errFNCOREDeadlineApplication) {
		t.Fatalf("deadline application evidence = %+v", response.Attempts)
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("deadline application failure was cached as success instead of retiring the socket")
	}
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline application server did not observe transport settlement")
	}
}

type fncoreDeferredDeadlineClearConn struct {
	net.Conn
	armed       atomic.Bool
	armedSet    chan struct{}
	clearFailed chan struct{}
	armedOnce   sync.Once
	clearOnce   sync.Once
}

func (c *fncoreDeferredDeadlineClearConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.armed.Load() {
		c.clearOnce.Do(func() { close(c.clearFailed) })
		return errFNCOREDeferredDeadlineClear
	}
	err := c.Conn.SetReadDeadline(deadline)
	if err == nil && !deadline.IsZero() {
		c.armed.Store(true)
		c.armedOnce.Do(func() { close(c.armedSet) })
	}
	return err
}

func TestFNCOREDeferredCancellationDeadlineClearFailureRetiresBeforeCollateral(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	controlled := &fncoreDeferredDeadlineClearConn{
		Conn:        clientConn,
		armedSet:    make(chan struct{}),
		clearFailed: make(chan struct{}),
	}
	commandsSeen := make(chan struct{})
	releaseResponses := make(chan struct{})
	var releaseOnce sync.Once
	releaseServerResponses := func() { releaseOnce.Do(func() { close(releaseResponses) }) }
	t.Cleanup(releaseServerResponses)
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 deferred-clear server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		for range 3 {
			if _, err := reader.ReadString('\n'); err != nil {
				serverDone <- err
				return
			}
		}
		close(commandsSeen)
		<-releaseResponses
		if _, err := serverConn.Write([]byte("223 1 <before-deferred@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write(yencSinglePart([]byte("complete canceled response"), "deferred.bin")); err != nil {
			serverDone <- err
			return
		}
		_, err := reader.ReadByte()
		serverDone <- err
	}()

	stats := &providerStats{}
	reqCh := make(chan *Request, 3)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), controlled, 3, reqCh, nil, Auth{}, "", nil, stats,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "deferred-clear-provider"
	connection.abandonedDrainTimeout = 150 * time.Millisecond
	connection.abandonedDrainBytes = 64 * 1024
	t.Cleanup(func() {
		releaseServerResponses()
		_ = serverConn.Close()
		_ = connection.Close()
	})
	first := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <before-deferred@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	deferred := &Request{
		Ctx:         canceledCtx,
		Payload:     []byte("BODY <deferred-cancel@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		BodyWriter:  io.Discard,
		submittedAt: time.Now(),
	}
	collateral := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <deadline-collateral@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	go connection.Run()
	reqCh <- first
	reqCh <- deferred
	reqCh <- collateral
	select {
	case <-commandsSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("three requests did not become FIFO-pending before deferred cancellation")
	}
	cancel()
	releaseServerResponses()

	if response := awaitFNCOREPhaseResponse(t, first.RespCh, "request before deferred cancellation"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("request before deferred cancellation = %+v", response)
	}
	deferredResponse := awaitFNCOREPhaseResponse(t, deferred.RespCh, "deferred canceled BODY")
	if !errors.Is(deferredResponse.Err, context.Canceled) || deferredResponse.StatusCode != 222 ||
		len(deferredResponse.Attempts) != 1 || deferredResponse.Attempts[0].Outcome != OutcomeCancellation {
		t.Fatalf("deferred canceled BODY response = %+v", deferredResponse)
	}
	select {
	case <-controlled.armedSet:
	case <-time.After(5 * time.Second):
		t.Fatal("deferred cancellation drain never applied its bounded deadline")
	}
	select {
	case <-controlled.clearFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("transport never attempted to clear the deferred drain deadline")
	}

	collateralResponse := awaitFNCOREPhaseResponse(t, collateral.RespCh, "deadline-clear collateral")
	if !errors.Is(collateralResponse.Err, ErrConnectionDied) {
		t.Fatalf("deadline-clear collateral error = %v, want connection-died attribution", collateralResponse.Err)
	}
	if collateral.responseHeadAt.Load() != 0 {
		t.Fatalf("deadline-clear collateral became response head before poisoned transport retirement: %d", collateral.responseHeadAt.Load())
	}
	if len(collateralResponse.Attempts) != 1 ||
		collateralResponse.Attempts[0].ProviderID != "deferred-clear-provider" ||
		collateralResponse.Attempts[0].Operation != OperationStat ||
		collateralResponse.Attempts[0].Outcome != OutcomeTransportFailure {
		t.Fatalf("deadline-clear collateral evidence = %+v", collateralResponse.Attempts)
	}
	if completion := classifyCircuitBreakerCompletion(collateralResponse, true, false); completion != circuitBreakerNeutral {
		t.Fatalf("deadline-clear collateral breaker completion = %v, want neutral", completion)
	}
	if got := stats.Errors.Load(); got != 0 {
		t.Fatalf("deferred deadline-clear/collateral provider errors = %d, want zero", got)
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("failed deferred deadline clear did not retire the socket")
	}
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("deferred-clear server observed unexpected reuse bytes")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deferred-clear server did not observe transport retirement")
	}
}

func requireFNCOREPretransportPostClosesPipe(
	t *testing.T,
	client *Client,
	label string,
) Response {
	t.Helper()
	pipeReader, pipeWriter := io.Pipe()
	t.Cleanup(func() { _ = pipeReader.Close() })
	producerDone := make(chan error, 1)
	go func() {
		_, err := pipeWriter.Write(bytes.Repeat([]byte("encoded article data"), 1024))
		_ = pipeWriter.CloseWithError(err)
		producerDone <- err
	}()

	response := awaitFNCOREPhaseResponse(t, client.sendPost(context.Background(), pipeReader), label)
	if response.Err == nil {
		_ = pipeReader.Close()
		<-producerDone
		t.Fatalf("%s response = %+v, want terminal pretransport failure", label, response)
	}
	select {
	case producerErr := <-producerDone:
		if producerErr == nil {
			t.Fatalf("%s pipe producer unexpectedly completed without reader consumption", label)
		}
	case <-time.After(500 * time.Millisecond):
		// Fail-safe release proves the producer was still blocked by the leaked
		// reader while ensuring the test never leaves a PostYenc-style goroutine.
		_ = pipeReader.Close()
		<-producerDone
		t.Fatalf("%s did not close its owned pipe reader; PostYenc producer remained blocked", label)
	}
	return response
}

func TestFNCORETerminalPretransportPostFailureClosesOwnedBody(t *testing.T) {
	t.Run("no providers remain", func(t *testing.T) {
		provider := Provider{
			ID:          "removed-post-provider",
			Host:        "removed-post-provider.invalid:119",
			Factory:     func(context.Context) (net.Conn, error) { return nil, errors.New("unused removed provider") },
			Connections: 1,
			Inflight:    1,
			SkipPing:    true,
		}
		client := fncoreClient(t, provider)
		if err := client.RemoveProvider(provider.Host); err != nil {
			t.Fatalf("RemoveProvider() error = %v", err)
		}
		response := requireFNCOREPretransportPostClosesPipe(t, client, "no-provider POST")
		if response.ProviderID != "" || len(response.Attempts) != 0 {
			t.Fatalf("no-provider POST fabricated transport evidence: %+v", response)
		}
	})

	t.Run("all providers fail before ownership", func(t *testing.T) {
		var firstDials atomic.Int32
		var secondDials atomic.Int32
		client := fncoreClient(t,
			Provider{
				ID: "pretransport-post-1", Host: "pretransport-post-1.invalid:119", Connections: 1, Inflight: 1, SkipPing: true,
				Factory: func(context.Context) (net.Conn, error) {
					firstDials.Add(1)
					return nil, errors.New("first deterministic dial failure")
				},
			},
			Provider{
				ID: "pretransport-post-2", Host: "pretransport-post-2.invalid:119", Connections: 1, Inflight: 1, SkipPing: true,
				Factory: func(context.Context) (net.Conn, error) {
					secondDials.Add(1)
					return nil, errors.New("second deterministic dial failure")
				},
			},
		)
		response := requireFNCOREPretransportPostClosesPipe(t, client, "all-pretransport-failure POST")
		if firstDials.Load() == 0 || secondDials.Load() == 0 {
			t.Fatalf("pretransport dial attempts = %d/%d, want both providers exercised", firstDials.Load(), secondDials.Load())
		}
		if response.ProviderID != "pretransport-post-2" || len(response.Attempts) != 1 ||
			response.Attempts[0].ProviderID != "pretransport-post-2" {
			t.Fatalf("all-pretransport terminal evidence = %+v", response)
		}
	})
}
