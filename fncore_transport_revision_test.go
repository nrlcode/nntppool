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

	"github.com/mnightingale/rapidyenc"
)

func fncoreTargetedStat(ctx context.Context, client *Client, providerID, messageID string) StatManyResult {
	results := client.StatMany(ctx, []string{messageID}, StatManyOptions{
		Concurrency: 1,
		Provider:    providerID,
	})
	result, ok := <-results
	if !ok {
		if err := ctx.Err(); err != nil {
			return StatManyResult{MessageID: messageID, Err: err}
		}
		return StatManyResult{MessageID: messageID, Err: errors.New("targeted STAT returned no result")}
	}
	return result
}

type fncoreRacingErrorWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	err     error
}

func (w *fncoreRacingErrorWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return 0, w.err
}

type fncoreWriteFailConn struct {
	net.Conn
	failAt int32
	writes atomic.Int32
	err    error
}

func (c *fncoreWriteFailConn) Write(p []byte) (int, error) {
	if c.writes.Add(1) == c.failAt {
		return 0, c.err
	}
	return c.Conn.Write(p)
}

type fncoreBlockingWriteConn struct {
	net.Conn
	once              sync.Once
	started           chan struct{}
	release           chan struct{}
	blockedAtNanos    atomic.Int64
	readDeadlineCalls atomic.Int32
	readDeadlineNanos atomic.Int64
}

func (c *fncoreBlockingWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() {
		c.blockedAtNanos.Store(time.Now().UnixNano())
		close(c.started)
		<-c.release
	})
	return c.Conn.Write(p)
}

func (c *fncoreBlockingWriteConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadlineCalls.Add(1)
	c.readDeadlineNanos.Store(deadline.UnixNano())
	return c.Conn.SetReadDeadline(deadline)
}

type fncoreFinalizationDecodeBarrier struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
	errMu       sync.Mutex
	err         error
}

func newFNCOREFinalizationDecodeBarrier() *fncoreFinalizationDecodeBarrier {
	return &fncoreFinalizationDecodeBarrier{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *fncoreFinalizationDecodeBarrier) setError(err error) {
	b.errMu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.errMu.Unlock()
}

func (b *fncoreFinalizationDecodeBarrier) Error() error {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.err
}

func (b *fncoreFinalizationDecodeBarrier) Release() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *fncoreFinalizationDecodeBarrier) Decode(
	dst, src []byte,
	state *rapidyenc.State,
) (nDst, nSrc int, end rapidyenc.End, err error) {
	switch call := b.calls.Add(1); call {
	case 1:
		// Consume only the encoded data. The first decode therefore writes all
		// caller bytes and returns before the already-buffered trailer is parsed.
		trailer := bytes.Index(src, []byte("\r\n=yend "))
		if trailer <= 0 {
			b.setError(errors.New("first decoder invocation did not contain the complete encoded body and trailer"))
			b.enteredOnce.Do(func() { close(b.entered) })
			<-b.release
			return 0, 0, rapidyenc.EndNone, b.Error()
		}
		return rapidyenc.DecodeIncremental(dst[:trailer], src[:trailer], state)
	case 2:
		if !bytes.Contains(src, []byte("=yend ")) {
			b.setError(errors.New("second decoder invocation did not begin with the buffered yEnc trailer"))
		}
		b.enteredOnce.Do(func() { close(b.entered) })
		<-b.release
		return rapidyenc.DecodeIncremental(dst, src, state)
	default:
		return rapidyenc.DecodeIncremental(dst, src, state)
	}
}

func fncoreBufferedTrailerFactory(response []byte) ConnFactory {
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			if _, err := server.Write(response); err != nil {
				return
			}
			// feedUntilDoneControlled asks the socket for more input after a
			// partial decode even when the yEnc trailer remains in readBuffer.
			// This byte satisfies only that read; the second decode invocation
			// still begins at the already-buffered trailer.
			_, _ = server.Write([]byte("x"))
		}()
		return client, nil
	}
}

type fncoreDeadlineTimeout struct{}

func (fncoreDeadlineTimeout) Error() string   { return "deterministic socket deadline" }
func (fncoreDeadlineTimeout) Timeout() bool   { return true }
func (fncoreDeadlineTimeout) Temporary() bool { return true }

type fncoreCallerDeadlineConn struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
	armed    bool
}

func (c *fncoreCallerDeadlineConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.armed = !deadline.IsZero()
	c.mu.Unlock()
	return nil
}

func (c *fncoreCallerDeadlineConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	armed := c.armed
	c.mu.Unlock()
	if armed {
		return 0, fncoreDeadlineTimeout{}
	}
	return c.Conn.Read(p)
}

func (c *fncoreCallerDeadlineConn) lastDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

func TestFNCOREHalfOpenCancellationBeforeTransportAdmissionIsPromptAndNeutral(t *testing.T) {
	clock := newBreakerFakeClock()
	var blockDial atomic.Bool
	var recovering atomic.Bool
	var healthyProbeCommands atomic.Int32
	dialStarted := make(chan struct{})
	dialRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() { releaseOnce.Do(func() { close(dialRelease) }) }
	defer releaseDial()
	wireObservation := make(chan string, 1)
	var dialOnce sync.Once
	factory := func(context.Context) (net.Conn, error) {
		blocked := blockDial.Load()
		if blocked {
			dialOnce.Do(func() { close(dialStarted) })
			<-dialRelease
		}
		client, server := net.Pipe()
		go func(blocked bool) {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if blocked {
				_ = server.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				command, _ := reader.ReadString('\n')
				wireObservation <- command
				return
			}
			if _, err := reader.ReadString('\n'); err == nil {
				if recovering.Load() {
					healthyProbeCommands.Add(1)
					_, _ = server.Write([]byte("223 1 <pre-admission@example.invalid> exists\r\n"))
					return
				}
				_, _ = server.Write([]byte("451 temporary failure\r\n"))
			}
		}(blocked)
		return client, nil
	}
	provider := Provider{
		ID:          "pre-admission-half-open",
		Host:        "pre-admission-half-open.invalid:119",
		Factory:     factory,
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, clock, provider)
	for range providerBreakerFailureThreshold {
		_ = targetedBreakerStat(client, provider.ID)
	}
	clock.Advance(providerBreakerCooldowns[0])
	blockDial.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan StatManyResult, 1)
	go func() {
		result <- fncoreTargetedStat(ctx, client, provider.ID, "pre-admission@example.invalid")
	}()
	select {
	case <-dialStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("half-open request never reached blocked provider admission")
	}
	cancel()
	select {
	case stat := <-result:
		if !errors.Is(stat.Err, context.Canceled) {
			t.Fatalf("pre-admission cancellation error = %v, want context cancellation", stat.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-admission half-open cancellation waited for transport settlement")
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerHalfOpen || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("pre-admission cancellation breaker state = %+v, want neutral released probe", stats)
	}
	releaseDial()
	select {
	case command := <-wireObservation:
		if command != "" {
			t.Fatalf("canceled pre-admission request reached wire: %q", command)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked admission did not settle after release")
	}
	stats = providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerHalfOpen || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("late pre-admission settlement changed breaker state: %+v", stats)
	}

	blockDial.Store(false)
	recovering.Store(true)
	healthy := fncoreTargetedStat(context.Background(), client, provider.ID, "pre-admission@example.invalid")
	if healthy.Err != nil || healthy.Result == nil {
		t.Fatalf("healthy half-open probe result = %+v", healthy)
	}
	if got := healthyProbeCommands.Load(); got != 1 {
		t.Fatalf("healthy half-open wire commands = %d, want exactly one", got)
	}
	stats = providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("healthy probe did not close and reset breaker: %+v", stats)
	}
}

func TestFNCORECommittedFinalWriterToFinalizationRemovalIsTerminal(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	payload := []byte("final writer payload")
	primary := Provider{
		ID:          "finalization-primary",
		Host:        "finalization-primary.invalid:119",
		Factory:     fncoreBufferedTrailerFactory(yencSinglePart(payload, "final.bin")),
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	backup := &regressionProvider{
		host: "finalization-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not replay"), "backup.bin")
		},
	}
	client := fncoreClient(t, primary, backup.provider(true))
	barrier := newFNCOREFinalizationDecodeBarrier()
	defer barrier.Release()
	client.decodeFn = barrier.Decode
	var writer bytes.Buffer
	result := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", &writer)
		result <- err
	}()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("decoder did not reach the buffered finalization seam (calls=%d, error=%v)", barrier.calls.Load(), barrier.Error())
	}
	if err := barrier.Error(); err != nil {
		t.Fatalf("decoder seam error = %v", err)
	}
	if !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("caller bytes at finalization seam = %q, want %q", writer.Bytes(), payload)
	}
	if err := client.RemoveProvider(primary.Host); err != nil {
		t.Fatalf("RemoveProvider() error = %v", err)
	}
	barrier.Release()
	select {
	case err := <-result:
		if !errors.Is(err, ErrConnectionDied) {
			t.Fatalf("provider removal between final write and finalization error = %v, want connection retirement", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed finalization race did not settle")
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after final caller write", got)
	}
	if !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("caller bytes = %q, want only primary payload %q", writer.Bytes(), payload)
	}
}

func TestFNCORECommittedFinalWriterToFinalizationCancellationAndShutdownAreTerminal(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, mode := range []string{"caller cancellation", "client shutdown"} {
		t.Run(mode, func(t *testing.T) {
			payload := []byte("final lifecycle payload")
			primary := Provider{
				ID:          "final-lifecycle-primary",
				Host:        "final-lifecycle-primary.invalid:119",
				Factory:     fncoreBufferedTrailerFactory(yencSinglePart(payload, "final-lifecycle.bin")),
				Connections: 1,
				Inflight:    1,
				SkipPing:    true,
			}
			backup := &regressionProvider{
				host: "final-lifecycle-backup.invalid:119",
				respond: func(int, string) []byte {
					return yencSinglePart([]byte("must not replay"), "backup.bin")
				},
			}
			client := fncoreClient(t, primary, backup.provider(true))
			barrier := newFNCOREFinalizationDecodeBarrier()
			defer barrier.Release()
			client.decodeFn = barrier.Decode
			var writer bytes.Buffer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := client.BodyStream(ctx, "fixture@example.invalid", &writer)
				result <- err
			}()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				t.Fatalf("decoder did not reach the buffered finalization seam (calls=%d, error=%v)", barrier.calls.Load(), barrier.Error())
			}
			if err := barrier.Error(); err != nil {
				t.Fatalf("decoder seam error = %v", err)
			}
			if !bytes.Equal(writer.Bytes(), payload) {
				t.Fatalf("caller bytes at finalization seam = %q, want %q", writer.Bytes(), payload)
			}
			var want error
			switch mode {
			case "caller cancellation":
				cancel()
				want = context.Canceled
			case "client shutdown":
				// Close begins with this exact cancellation. Calling it directly
				// keeps the lifecycle transition and decoder release in one
				// GOMAXPROCS(1) critical sequence, so an API waiter cannot fill in
				// the missing finalization sample on the reader's behalf.
				client.cancel()
				want = context.Canceled
			}
			for range 8 {
				runtime.Gosched()
			}
			select {
			case err := <-result:
				t.Fatalf("%s returned before committed decoder finalization: %v", mode, err)
			default:
			}
			barrier.Release()
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("%s between final write and finalization error = %v, want %v", mode, err, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s finalization race did not settle", mode)
			}
			if got := backup.commandCount("BODY"); got != 0 {
				t.Fatalf("backup BODY commands = %d, want zero after %s", got, mode)
			}
			if !bytes.Equal(writer.Bytes(), payload) {
				t.Fatalf("caller bytes = %q, want only primary payload %q", writer.Bytes(), payload)
			}
		})
	}
}

func TestFNCOREWriterErrorWinsFinalizationRemovalRace(t *testing.T) {
	writerErr := errors.New("final writer local failure")
	primary := &regressionProvider{
		host: "writer-race-primary.invalid:119",
		respond: func(_ int, command string) []byte {
			if bytes.HasPrefix([]byte(command), []byte("STAT ")) {
				return []byte("451 temporary failure\r\n")
			}
			return yencSinglePart([]byte("primary payload"), "primary.bin")
		},
	}
	provider := primary.provider(false)
	provider.ID = "writer-race-primary"
	backup := &regressionProvider{
		host: "writer-race-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not replay"), "backup.bin")
		},
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider, backup.provider(true))
	group := fncoreProviderGroup(t, client, provider.ID)
	preload := fncoreTargetedStat(context.Background(), client, provider.ID, "preload@example.invalid")
	if preload.Err == nil {
		t.Fatal("breaker preload STAT error = nil, want one qualifying provider failure")
	}
	preloadStats := group.breaker.snapshot()
	if preloadStats.State != CircuitBreakerClosed || preloadStats.QualifyingFailures != 1 {
		t.Fatalf("breaker preload state = %+v, want one closed-state failure", preloadStats)
	}
	providerErrors := group.stats.Errors.Load()
	writer := &fncoreRacingErrorWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     writerErr,
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
		result <- err
	}()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("caller writer did not start")
	}
	if err := client.RemoveProvider(primary.host); err != nil {
		t.Fatalf("RemoveProvider() error = %v", err)
	}
	close(writer.release)
	select {
	case err := <-result:
		if !errors.Is(err, writerErr) {
			t.Fatalf("finalization race error = %v, want local writer cause", err)
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || len(transportErr.Attempts) == 0 {
			t.Fatalf("finalization race error = %v, want structured local attempt", err)
		}
		if transportErr.Kind != OutcomeKind("local_failure") {
			t.Fatalf("finalization race transport kind = %q, want local_failure", transportErr.Kind)
		}
		attempt := transportErr.Attempts[len(transportErr.Attempts)-1]
		if attempt.Outcome != OutcomeKind("local_failure") {
			t.Fatalf("finalization race attempt outcome = %q, want local_failure", attempt.Outcome)
		}
		if !errors.Is(attempt.Cause, writerErr) {
			t.Fatalf("finalization race attempt cause = %v, want underlying writer error", attempt.Cause)
		}
		if attempt.ProviderResponseTimeout {
			t.Fatalf("finalization race local writer attempt marked provider timeout: %+v", attempt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer-error finalization race did not settle")
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after writer error", got)
	}
	if got := group.stats.Errors.Load(); got != providerErrors {
		t.Fatalf("provider errors after local writer race = %d, want unchanged %d", got, providerErrors)
	}
	stats := group.breaker.snapshot()
	if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 1 {
		t.Fatalf("local writer race reset or incremented breaker evidence: %+v", stats)
	}
}

func fncoreFlushFailureFactory(failAt int32, failure error) ConnFactory {
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			_, _ = io.Copy(io.Discard, server)
		}()
		return &fncoreWriteFailConn{Conn: client, failAt: failAt, err: failure}, nil
	}
}

func TestFNCORECommandFlushFailureBeforeResponseHeadTripsBreaker(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	failure := errors.New("deterministic command flush failure")
	provider := Provider{
		ID:          "command-flush-failure",
		Host:        "command-flush-failure.invalid:119",
		Factory:     fncoreFlushFailureFactory(1, failure),
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	for request := 1; request <= providerBreakerFailureThreshold; request++ {
		result := fncoreTargetedStat(context.Background(), client, provider.ID, "flush@example.invalid")
		if result.Err == nil {
			t.Fatalf("request %d command flush error = nil", request)
		}
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerOpen || stats.Cooldown != providerBreakerCooldowns[0] {
		t.Fatalf("command flush breaker state = %+v, want open", stats)
	}
}

func TestFNCOREDirectPostFlushFailureIsCurrentProviderTransportFailure(t *testing.T) {
	failure := errors.New("deterministic POST flush failure")
	conn, err := fncoreFlushFailureFactory(1, failure)(context.Background())
	if err != nil {
		t.Fatalf("flush-failure factory error = %v", err)
	}
	reqCh := make(chan *Request)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), conn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "post-flush-failure"
	go connection.Run()
	t.Cleanup(func() {
		_ = connection.Close()
		<-connection.Done()
	})
	req := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("POST\r\n"),
		PayloadBody: bytes.NewReader([]byte("article body\r\n.\r\n")),
		PostMode:    true,
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- req
	var response Response
	select {
	case response = <-req.RespCh:
	case <-time.After(5 * time.Second):
		t.Fatal("direct POST request did not settle after Flush failure")
	}
	if response.Request != req || response.ProviderID != connection.providerID {
		t.Fatalf("POST Flush response attribution = request %p provider %q, want request %p provider %q",
			response.Request, response.ProviderID, req, connection.providerID)
	}
	if !errors.Is(response.Err, failure) {
		t.Fatalf("POST Flush response error = %v, want underlying transport failure", response.Err)
	}
	if writtenAt := req.writtenAt.Load(); writtenAt != 0 {
		t.Fatalf("POST request marked written before failed Flush: %d", writtenAt)
	}
	if headAt := req.responseHeadAt.Load(); headAt != 0 {
		t.Fatalf("POST request reached response head before failed Flush: %d", headAt)
	}
	response.Attempts = []AttemptEvidence{
		buildAttemptEvidence(req, connection.providerID, response, time.Now()),
	}
	attempt := response.Attempts[0]
	if attempt.Operation != OperationPost || attempt.Outcome != OutcomeTransportFailure ||
		attempt.ProviderID != connection.providerID || !errors.Is(attempt.Cause, failure) {
		t.Fatalf("POST Flush attempt evidence = %+v, want factual provider transport failure", attempt)
	}
	if completion := classifyCircuitBreakerCompletion(response, true, false); completion != circuitBreakerFailure {
		t.Fatalf("POST Flush breaker completion = %v, want provider failure", completion)
	}
}

func TestFNCOREPreviouslyFlushedPendingRequestIsCollateralNeutral(t *testing.T) {
	failure := errors.New("third command flush failure")
	clientConn, serverConn := net.Pipe()
	connection := &fncoreWriteFailConn{Conn: clientConn, failAt: 3, err: failure}
	commands := make(chan string, 2)
	go func() {
		defer func() { _ = serverConn.Close() }()
		_, _ = serverConn.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(serverConn)
		for range 2 {
			command, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			commands <- command
		}
		_, _ = io.Copy(io.Discard, reader)
	}()
	reqCh := make(chan *Request, 3)
	nntpConnection, err := newNNTPConnectionFromConn(
		context.Background(), connection, 3, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() { _ = nntpConnection.Close() })
	requests := make([]*Request, 3)
	for index := range requests {
		requests[index] = &Request{
			Ctx:         context.Background(),
			Payload:     []byte("STAT <flush-" + string(rune('1'+index)) + "@example.invalid>\r\n"),
			RespCh:      make(chan Response, 1),
			submittedAt: time.Now(),
		}
	}
	go nntpConnection.Run()
	reqCh <- requests[0]
	select {
	case <-commands:
	case <-time.After(5 * time.Second):
		t.Fatal("first command did not flush")
	}
	deadline := time.Now().Add(5 * time.Second)
	for requests[0].responseHeadAt.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first request did not become response head")
		}
		runtime.Gosched()
	}
	reqCh <- requests[1]
	select {
	case <-commands:
	case <-time.After(5 * time.Second):
		t.Fatal("second command did not flush")
	}
	if head := requests[1].responseHeadAt.Load(); head != 0 {
		t.Fatalf("second request unexpectedly became response head: %d", head)
	}
	reqCh <- requests[2]

	responses := make([]Response, len(requests))
	for index, request := range requests {
		select {
		case responses[index] = <-request.RespCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("request %d did not settle after flush failure", index+1)
		}
		responses[index].Request = request
		responses[index].Attempts = []AttemptEvidence{
			buildAttemptEvidence(request, "flush-collateral", responses[index], time.Now()),
		}
	}
	if completion := classifyCircuitBreakerCompletion(responses[0], true, false); completion != circuitBreakerFailure {
		t.Fatalf("response-head failure completion = %v, want provider failure", completion)
	}
	if completion := classifyCircuitBreakerCompletion(responses[1], true, false); completion != circuitBreakerNeutral {
		t.Fatalf("previously flushed pending completion = %v, want collateral neutral", completion)
	}
	if completion := classifyCircuitBreakerCompletion(responses[2], true, false); completion != circuitBreakerFailure {
		t.Fatalf("current flush failure completion = %v, want provider failure", completion)
	}
}

func TestFNCOREBlockingSuccessfulFlushIsPoolTimeNotResponseTimeout(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	writeStarted := make(chan struct{})
	writeRelease := make(chan struct{})
	clientConn, serverConn := net.Pipe()
	blockedConn := &fncoreBlockingWriteConn{
		Conn:    clientConn,
		started: writeStarted,
		release: writeRelease,
	}
	go func() {
		defer func() { _ = serverConn.Close() }()
		_, _ = serverConn.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err == nil {
			_, _ = serverConn.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
		}
	}()
	reqCh := make(chan *Request)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), blockedConn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "blocking-flush-timing"
	// The unbuffered handoff makes writeStarted proof that readerLoop owns this
	// exact Request while Flush remains blocked.
	connection.pending = make(chan *Request)
	readDeadlineBaseline := blockedConn.readDeadlineCalls.Load()
	go connection.Run()
	t.Cleanup(func() {
		select {
		case <-writeRelease:
		default:
			close(writeRelease)
		}
		_ = connection.Close()
		<-connection.Done()
	})
	req := &Request{
		Ctx:             context.Background(),
		Payload:         []byte("STAT <fixture@example.invalid>\r\n"),
		RespCh:          make(chan Response, 1),
		submittedAt:     time.Now(),
		responseTimeout: time.Hour,
	}
	reqCh <- req
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("command flush did not block")
	}
	// Give the already-rendezvoused reader multiple explicit turns. A correct
	// reader blocks on wire readiness; an eager reader publishes response-head
	// state and arms its service deadline.
	for range 16 {
		runtime.Gosched()
	}
	if writtenAt := req.writtenAt.Load(); writtenAt != 0 {
		t.Fatalf("request marked written while Flush was blocked: %d", writtenAt)
	}
	if headAt := req.responseHeadAt.Load(); headAt != 0 {
		t.Fatalf("response head started while Flush was blocked: %d", headAt)
	}
	if calls := blockedConn.readDeadlineCalls.Load(); calls != readDeadlineBaseline {
		t.Fatalf("response read deadline armed while Flush was blocked: calls %d, baseline %d, deadline %v",
			calls, readDeadlineBaseline, time.Unix(0, blockedConn.readDeadlineNanos.Load()))
	}
	releaseAt := time.Now()
	close(writeRelease)
	var response Response
	select {
	case response = <-req.RespCh:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete after successful flush")
	}
	if response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("blocking-flush response = %+v, want successful STAT", response)
	}
	writtenAt := req.writtenAt.Load()
	headAt := req.responseHeadAt.Load()
	if writtenAt < releaseAt.UnixNano() {
		t.Fatalf("written timestamp %v precedes Flush release %v", time.Unix(0, writtenAt), releaseAt)
	}
	if headAt < writtenAt {
		t.Fatalf("response-head timestamp %v precedes successful write %v", time.Unix(0, headAt), time.Unix(0, writtenAt))
	}
	if calls := blockedConn.readDeadlineCalls.Load(); calls <= readDeadlineBaseline {
		t.Fatalf("response read deadline was not armed after successful Flush: calls %d, baseline %d", calls, readDeadlineBaseline)
	}
	if deadline := blockedConn.readDeadlineNanos.Load(); deadline <= headAt {
		t.Fatalf("response read deadline %v does not follow response head %v", time.Unix(0, deadline), time.Unix(0, headAt))
	}
	if len(response.Attempts) != 1 {
		t.Fatalf("blocking-flush attempts = %+v", response.Attempts)
	}
	attempt := response.Attempts[0]
	blockedAt := time.Unix(0, blockedConn.blockedAtNanos.Load())
	blockedInterval := releaseAt.Sub(blockedAt)
	if blockedInterval <= 0 || attempt.PoolQueueDuration < blockedInterval {
		t.Fatalf("pool/write duration = %v, want complete blocked Flush interval %v", attempt.PoolQueueDuration, blockedInterval)
	}
	// The two atomic transport timestamps are the attribution boundary: since
	// responseHeadAt follows Flush release, none of the blocked interval can be
	// included in ResponseServiceDuration.
	if attempt.ResponseServiceDuration < 0 {
		t.Fatalf("response service duration = %v, want non-negative post-Flush service", attempt.ResponseServiceDuration)
	}
}

func TestFNCORECallerSelectedSocketDeadlineIsCancellationNotBreakerFailure(t *testing.T) {
	connections := make(chan *fncoreCallerDeadlineConn, providerBreakerFailureThreshold)
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		connection := &fncoreCallerDeadlineConn{Conn: client}
		connections <- connection
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			_, _ = bufio.NewReader(server).ReadString('\n')
		}()
		return connection, nil
	}
	provider := Provider{
		ID:             "caller-deadline-provenance",
		Host:           "caller-deadline-provenance.invalid:119",
		Factory:        factory,
		Connections:    1,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 2 * time.Second,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	for request := 1; request <= providerBreakerFailureThreshold; request++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		callerDeadline, _ := ctx.Deadline()
		result := fncoreTargetedStat(ctx, client, provider.ID, "deadline@example.invalid")
		if ctx.Err() != nil {
			cancel()
			t.Fatalf("request %d context expired before attribution: %v", request, ctx.Err())
		}
		connection := <-connections
		if got := connection.lastDeadline(); !got.Equal(callerDeadline) {
			cancel()
			t.Fatalf("request %d socket deadline = %v, want caller deadline %v", request, got, callerDeadline)
		}
		var transportErr *TransportError
		if !errors.As(result.Err, &transportErr) || transportErr.Kind != OutcomeCancellation {
			cancel()
			t.Fatalf("request %d error = %v, want cancellation outcome", request, result.Err)
		}
		if !errors.Is(result.Err, context.DeadlineExceeded) {
			cancel()
			t.Fatalf("request %d error = %v, want caller deadline cause", request, result.Err)
		}
		cancel()
		stats := providerBreakerStats(t, client, provider.ID)
		if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("request %d caller deadline changed breaker: %+v", request, stats)
		}
	}
}
