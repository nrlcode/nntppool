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

type fncoreSignalWriter struct {
	once  sync.Once
	wrote chan struct{}
	bytes bytes.Buffer
}

func (w *fncoreSignalWriter) Write(p []byte) (int, error) {
	n, err := w.bytes.Write(p)
	w.once.Do(func() { close(w.wrote) })
	return n, err
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
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (c *fncoreBlockingWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() {
		close(c.started)
		<-c.release
	})
	return c.Conn.Write(p)
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
	dialStarted := make(chan struct{})
	dialRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() { releaseOnce.Do(func() { close(dialRelease) }) }
	defer releaseDial()
	wireObservation := make(chan string, 1)
	var dialOnce sync.Once
	factory := func(context.Context) (net.Conn, error) {
		if blockDial.Load() {
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
				_, _ = server.Write([]byte("451 temporary failure\r\n"))
			}
		}(blockDial.Load())
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
}

func TestFNCORECommittedFinalWriterToFinalizationRemovalIsTerminal(t *testing.T) {
	payload := []byte("final writer payload")
	response := yencSinglePart(payload, "final.bin")
	trailer := bytes.Index(response, []byte("\r\n=yend"))
	if trailer < 0 {
		t.Fatal("fixture omitted yEnc trailer")
	}
	releaseTrailer := make(chan struct{})
	primaryFactory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			if _, err := server.Write(response[:trailer+2]); err != nil {
				return
			}
			<-releaseTrailer
			_, _ = server.Write(response[trailer+2:])
		}()
		return client, nil
	}
	primary := Provider{
		ID:          "finalization-primary",
		Host:        "finalization-primary.invalid:119",
		Factory:     primaryFactory,
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
	writer := &fncoreSignalWriter{wrote: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
		result <- err
	}()
	select {
	case <-writer.wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("final caller write did not occur")
	}
	if err := client.RemoveProvider(primary.Host); err != nil {
		t.Fatalf("RemoveProvider() error = %v", err)
	}
	close(releaseTrailer)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("provider removal between final write and finalization returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed finalization race did not settle")
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after final caller write", got)
	}
	if !bytes.Equal(writer.bytes.Bytes(), payload) {
		t.Fatalf("caller bytes = %q, want only primary payload %q", writer.bytes.Bytes(), payload)
	}
}

func TestFNCORECommittedFinalWriterToFinalizationCancellationAndShutdownAreTerminal(t *testing.T) {
	for _, mode := range []string{"caller cancellation", "client shutdown"} {
		t.Run(mode, func(t *testing.T) {
			payload := []byte("final lifecycle payload")
			response := yencSinglePart(payload, "final-lifecycle.bin")
			trailer := bytes.Index(response, []byte("\r\n=yend"))
			if trailer < 0 {
				t.Fatal("fixture omitted yEnc trailer")
			}
			releaseTrailer := make(chan struct{})
			factory := func(context.Context) (net.Conn, error) {
				client, server := net.Pipe()
				go func() {
					defer func() { _ = server.Close() }()
					_, _ = server.Write([]byte("200 regression server ready\r\n"))
					reader := bufio.NewReader(server)
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					if _, err := server.Write(response[:trailer+2]); err != nil {
						return
					}
					<-releaseTrailer
					_, _ = server.Write(response[trailer+2:])
				}()
				return client, nil
			}
			primary := Provider{
				ID:          "final-lifecycle-primary",
				Host:        "final-lifecycle-primary.invalid:119",
				Factory:     factory,
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
			writer := &fncoreSignalWriter{wrote: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := client.BodyStream(ctx, "fixture@example.invalid", writer)
				result <- err
			}()
			select {
			case <-writer.wrote:
			case <-time.After(5 * time.Second):
				t.Fatal("final caller write did not occur")
			}
			var closeResult chan error
			switch mode {
			case "caller cancellation":
				cancel()
			case "client shutdown":
				closeResult = make(chan error, 1)
				go func() { closeResult <- client.Close() }()
				select {
				case <-client.ctx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("client shutdown did not become active")
				}
			}
			close(releaseTrailer)
			select {
			case err := <-result:
				if err == nil {
					t.Fatalf("%s between final write and finalization returned success", mode)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s finalization race did not settle", mode)
			}
			if closeResult != nil {
				select {
				case err := <-closeResult:
					if err != nil {
						t.Fatalf("Close() error = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Close() did not settle")
				}
			}
			if got := backup.commandCount("BODY"); got != 0 {
				t.Fatalf("backup BODY commands = %d, want zero after %s", got, mode)
			}
			if !bytes.Equal(writer.bytes.Bytes(), payload) {
				t.Fatalf("caller bytes = %q, want only primary payload %q", writer.bytes.Bytes(), payload)
			}
		})
	}
}

func TestFNCOREWriterErrorWinsFinalizationRemovalRace(t *testing.T) {
	writerErr := errors.New("final writer local failure")
	primary := &regressionProvider{
		host: "writer-race-primary.invalid:119",
		respond: func(int, string) []byte {
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
	client := fncoreClient(t, provider, backup.provider(true))
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
	case <-time.After(5 * time.Second):
		t.Fatal("writer-error finalization race did not settle")
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after writer error", got)
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

func TestFNCOREPostFlushFailureBeforeResponseHeadTripsBreaker(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	failure := errors.New("deterministic POST flush failure")
	provider := Provider{
		ID:          "post-flush-failure",
		Host:        "post-flush-failure.invalid:119",
		Factory:     fncoreFlushFailureFactory(1, failure),
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	headers := PostHeaders{
		From:       "user@example.invalid",
		Subject:    "flush failure",
		Newsgroups: []string{"alt.binaries.test"},
		MessageID:  "<flush@example.invalid>",
	}
	meta := rapidyenc.Meta{
		FileName:   "flush.bin",
		FileSize:   1,
		PartNumber: 1,
		TotalParts: 1,
		PartSize:   1,
	}
	for request := 1; request <= providerBreakerFailureThreshold; request++ {
		if _, err := client.PostYenc(context.Background(), headers, bytes.NewReader([]byte("x")), meta); err == nil {
			t.Fatalf("request %d POST flush error = nil", request)
		}
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerOpen || stats.Cooldown != providerBreakerCooldowns[0] {
		t.Fatalf("POST flush breaker state = %+v, want open", stats)
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
	writeStarted := make(chan struct{})
	writeRelease := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err == nil {
				_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
			}
		}()
		return &fncoreBlockingWriteConn{Conn: client, started: writeStarted, release: writeRelease}, nil
	}
	provider := Provider{
		ID:             "blocking-flush-timing",
		Host:           "blocking-flush-timing.invalid:119",
		Factory:        factory,
		Connections:    1,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 50 * time.Millisecond,
	}
	client := fncoreClient(t, provider)
	result := make(chan StatManyResult, 1)
	go func() {
		result <- fncoreTargetedStat(context.Background(), client, provider.ID, "fixture@example.invalid")
	}()
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("command flush did not block")
	}
	time.Sleep(150 * time.Millisecond)
	select {
	case early := <-result:
		close(writeRelease)
		t.Fatalf("request completed while successful command flush was blocked: %+v", early)
	default:
	}
	close(writeRelease)
	select {
	case stat := <-result:
		if stat.Err != nil || stat.Result == nil {
			t.Fatalf("blocking-flush STAT result = %+v", stat)
		}
		if len(stat.Result.Attempts) != 1 {
			t.Fatalf("blocking-flush attempts = %+v", stat.Result.Attempts)
		}
		attempt := stat.Result.Attempts[0]
		if attempt.PoolQueueDuration < 100*time.Millisecond {
			t.Fatalf("pool/write duration = %v, want blocked flush time", attempt.PoolQueueDuration)
		}
		if attempt.ResponseServiceDuration >= 50*time.Millisecond {
			t.Fatalf("response service duration = %v, included successful flush delay", attempt.ResponseServiceDuration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete after successful flush")
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
