package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type removeProviderWriter struct {
	once       sync.Once
	client     *Client
	provider   string
	bytes      bytes.Buffer
	removeErr  error
	writeCount atomic.Int32
}

func (w *removeProviderWriter) Write(p []byte) (int, error) {
	w.writeCount.Add(1)
	n, err := w.bytes.Write(p)
	w.once.Do(func() { w.removeErr = w.client.RemoveProvider(w.provider) })
	return n, err
}

type zeroProgressWriter struct{}

func (zeroProgressWriter) Write([]byte) (int, error) { return 0, nil }

func fncoreClient(t *testing.T, providers ...Provider) *Client {
	t.Helper()
	client, err := NewClient(
		context.Background(),
		providers,
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
		WithSpeedAwareDispatch(false),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestFNCORECommittedWriterRemovalIsTerminal(t *testing.T) {
	firstPayload := []byte("first-provider-payload")
	secondPayload := []byte("backup-must-not-be-appended")
	first := &regressionProvider{
		host: "committed-primary.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart(firstPayload, "first.bin")
		},
	}
	backup := &regressionProvider{
		host: "committed-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart(secondPayload, "backup.bin")
		},
	}
	client := fncoreClient(t, first.provider(false), backup.provider(true))
	writer := &removeProviderWriter{client: client, provider: first.host}

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
	if err == nil {
		t.Fatal("BodyStream() error = nil, want terminal committed-attempt error")
	}
	if writer.removeErr != nil {
		t.Fatalf("RemoveProvider() error = %v", writer.removeErr)
	}
	if !bytes.Equal(writer.bytes.Bytes(), firstPayload) {
		t.Fatalf("caller sink = %q, want only first provider payload %q", writer.bytes.Bytes(), firstPayload)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY attempts = %d, want zero after caller-writer commitment", got)
	}
}

func TestFNCOREStreamingClassifierIsCaseInsensitiveForArticle(t *testing.T) {
	firstPayload := []byte("article-primary")
	secondPayload := []byte("article-backup")
	articleResponse := func(payload []byte, name string) []byte {
		response := yencSinglePart(payload, name)
		return append([]byte("220"), response[3:]...)
	}
	first := &regressionProvider{
		host: "article-primary.invalid:119",
		respond: func(int, string) []byte {
			return articleResponse(firstPayload, "article-first.bin")
		},
	}
	backup := &regressionProvider{
		host: "article-backup.invalid:119",
		respond: func(int, string) []byte {
			return articleResponse(secondPayload, "article-backup.bin")
		},
	}
	client := fncoreClient(t, first.provider(false), backup.provider(true))
	writer := &removeProviderWriter{client: client, provider: first.host}

	response := <-client.Send(
		context.Background(),
		[]byte("aRtIcLe <fixture@example.invalid>\r\n"),
		writer,
	)
	if response.Err == nil {
		t.Fatal("mixed-case ARTICLE error = nil, want terminal committed-attempt error")
	}
	if writer.removeErr != nil {
		t.Fatalf("RemoveProvider() error = %v", writer.removeErr)
	}
	if !bytes.Equal(writer.bytes.Bytes(), firstPayload) {
		t.Fatalf("ARTICLE caller sink = %q, want only %q", writer.bytes.Bytes(), firstPayload)
	}
	if got := backup.commandCount("aRtIcLe"); got != 0 {
		t.Fatalf("backup ARTICLE attempts = %d, want zero after commitment", got)
	}
	if len(response.Attempts) == 0 || response.Attempts[len(response.Attempts)-1].Operation != Operation("ARTICLE") {
		t.Fatalf("ARTICLE attempts = %+v, want factual ARTICLE operation", response.Attempts)
	}
}

func TestFNCORECallerWriterFailuresAreLocalAndBreakerNeutral(t *testing.T) {
	writerErr := errors.New("local sink failure")
	server, provider := breakerProvider(
		"local-writer",
		"local-writer.invalid:119",
		func(int, string) []byte {
			return yencSinglePart([]byte("healthy provider payload"), "healthy.bin")
		},
	)
	client := newBreakerClient(t, newBreakerFakeClock(), provider)

	for request := 1; request <= 3; request++ {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", failingWriter{err: writerErr})
		if !errors.Is(err, writerErr) {
			t.Fatalf("request %d error = %v, want underlying local sink error", request, err)
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || len(transportErr.Attempts) == 0 {
			t.Fatalf("request %d error = %v, want structured attempt evidence", request, err)
		}
		if got := transportErr.Attempts[len(transportErr.Attempts)-1].Outcome; got != OutcomeKind("local_failure") {
			t.Fatalf("request %d outcome = %q, want local_failure", request, got)
		}
		if transportErr.Attempts[len(transportErr.Attempts)-1].ProviderResponseTimeout {
			t.Fatalf("request %d local sink failure marked provider response timeout", request)
		}
	}
	if got := server.commandCount("BODY"); got != 3 {
		t.Fatalf("healthy provider BODY commands = %d, want 3", got)
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
		t.Fatalf("local writer failures changed breaker state: %+v", stats)
	}
	if got := client.Stats().Providers[0].Errors; got != 0 {
		t.Fatalf("provider errors = %d, want local sink failures excluded", got)
	}
}

func TestFNCOREZeroProgressWriterIsTerminalShortWrite(t *testing.T) {
	server := &regressionProvider{
		host: "short-writer.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("nonempty"), "short.bin")
		},
	}
	provider := server.provider(false)
	provider.ID = "short-writer"
	client := fncoreClient(t, provider)

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", zeroProgressWriter{})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("BodyStream() error = %v, want io.ErrShortWrite", err)
	}
	if got := client.Stats().Providers[0].Errors; got != 0 {
		t.Fatalf("provider errors = %d, want local short write excluded", got)
	}
}

func TestFNCOREPrebufferedResponseProgressUsesStallDeadline(t *testing.T) {
	firstResponse := yencSinglePart([]byte("first"), "first.bin")
	secondResponse := yencSinglePart(bytes.Repeat([]byte("s"), 32*1024), "second.bin")
	split := len(secondResponse) / 2
	commandsRead := make(chan struct{})
	conn := mockServer(t, func(server net.Conn) {
		_, _ = server.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(server)
		_, _ = reader.ReadString('\n')
		_, _ = reader.ReadString('\n')
		close(commandsRead)
		_, _ = server.Write(append(bytes.Clone(firstResponse), secondResponse[:split]...))
		time.Sleep(150 * time.Millisecond)
		_, _ = server.Write(secondResponse[split:])
	})

	reqCh := make(chan *Request, 2)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), conn, 2, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.stallTimeout = 500 * time.Millisecond
	t.Cleanup(func() { _ = connection.Close() })

	first := &Request{
		Ctx:             context.Background(),
		Payload:         []byte("BODY <first@example.invalid>\r\n"),
		RespCh:          make(chan Response, 1),
		BodyWriter:      io.Discard,
		responseTimeout: 50 * time.Millisecond,
	}
	second := &Request{
		Ctx:             context.Background(),
		Payload:         []byte("BODY <second@example.invalid>\r\n"),
		RespCh:          make(chan Response, 1),
		BodyWriter:      io.Discard,
		responseTimeout: 50 * time.Millisecond,
	}
	reqCh <- first
	reqCh <- second
	go connection.Run()
	select {
	case <-commandsRead:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive both pipelined requests")
	}
	if response := <-first.RespCh; response.Err != nil {
		t.Fatalf("first response error = %v", response.Err)
	}
	if response := <-second.RespCh; response.Err != nil {
		t.Fatalf("prebuffer-progress response error = %v, want rolling stall deadline", response.Err)
	}
}

func TestFNCORESubQuantumStallTimeoutRollsWithProgress(t *testing.T) {
	response := yencSinglePart(bytes.Repeat([]byte("q"), 128*1024), "rolling.bin")
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			const chunks = 20
			for start := 0; start < len(response); {
				end := min(start+(len(response)+chunks-1)/chunks, len(response))
				if _, err := server.Write(response[start:end]); err != nil {
					return
				}
				start = end
				time.Sleep(10 * time.Millisecond)
			}
		}()
		return client, nil
	}
	client := fncoreClient(t, Provider{
		ID:             "rolling-progress",
		Host:           "rolling-progress.invalid:119",
		Factory:        factory,
		Connections:    1,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 500 * time.Millisecond,
		StallTimeout:   40 * time.Millisecond,
	})

	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v, want progress to roll sub-quantum stall timeout", err)
	}
	if len(body.Bytes) != 128*1024 {
		t.Fatalf("Body() bytes = %d, want %d", len(body.Bytes), 128*1024)
	}
}

func TestFNCOREProviderPrivateDeadlineTripsBreaker(t *testing.T) {
	provider := Provider{
		ID:   "provider-private-deadline",
		Host: "provider-private-deadline.invalid:119",
		Factory: func(context.Context) (net.Conn, error) {
			return nil, context.DeadlineExceeded
		},
		Connections: 3,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	for request, err := range targetedBreakerErrors(client, provider.ID, 3) {
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeTransportFailure {
			t.Fatalf("request %d error = %v, want provider transport failure", request+1, err)
		}
		if len(transportErr.Attempts) == 0 || transportErr.Attempts[len(transportErr.Attempts)-1].ProviderResponseTimeout {
			t.Fatalf("request %d attempts = %+v, want non-response provider deadline", request+1, transportErr.Attempts)
		}
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second {
		t.Fatalf("provider-private deadlines did not open breaker: %+v", stats)
	}
}

func TestFNCORECancelledHalfOpenRemainsExclusiveUntilWriterSettles(t *testing.T) {
	clock := newBreakerFakeClock()
	var recovering atomic.Bool
	writerStarted := make(chan struct{})
	writerRelease := make(chan struct{})
	var writerOnce sync.Once
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			for {
				command, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if !recovering.Load() {
					_, _ = server.Write([]byte("451 temporary failure\r\n"))
					continue
				}
				if strings.HasPrefix(command, "STAT") {
					_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
					continue
				}
				_, _ = server.Write(yencSinglePart(bytes.Repeat([]byte("h"), 256*1024), "probe.bin"))
			}
		}()
		return client, nil
	}
	provider := Provider{
		ID:             "half-open-settlement",
		Host:           "half-open-settlement.invalid:119",
		Factory:        factory,
		Connections:    2,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 500 * time.Millisecond,
	}
	client := newBreakerClient(t, clock, provider)
	for range 3 {
		_ = targetedBreakerStat(client, provider.ID)
	}
	recovering.Store(true)
	clock.Advance(10 * time.Second)

	writer := &blockingWriter{started: writerStarted, release: writerRelease, once: writerOnce}
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	probeResult := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(probeCtx, "fixture@example.invalid", writer)
		probeResult <- err
	}()
	select {
	case <-writerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("half-open BODY did not reach caller writer")
	}
	cancelProbe()
	var earlyProbeErr error
	probeReturned := false
	select {
	case earlyProbeErr = <-probeResult:
		probeReturned = true
	case <-time.After(50 * time.Millisecond):
		// A committed caller writer keeps transport ownership until Write settles.
	}

	secondErr := targetedBreakerStat(client, provider.ID)
	if !errors.Is(secondErr, ErrCircuitBreakerOpen) {
		t.Fatalf("concurrent half-open request error = %v, want breaker-open until transport settles", secondErr)
	}
	close(writerRelease)
	if probeReturned {
		if !errors.Is(earlyProbeErr, context.Canceled) {
			t.Fatalf("early canceled half-open BODY error = %v, want context cancellation", earlyProbeErr)
		}
		return
	}
	select {
	case err := <-probeResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled half-open BODY error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled half-open BODY did not return after writer settled")
	}
}

func TestFNCOREFreshTransportDoesNotAbortCommittedCollateralBody(t *testing.T) {
	var connections atomic.Int32
	streamPayload := bytes.Repeat([]byte("c"), 256*1024)
	freshPayload := []byte("fresh payload")
	factory := func(context.Context) (net.Conn, error) {
		connection := connections.Add(1)
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			for {
				_, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				payload := freshPayload
				name := "fresh.bin"
				if connection == 1 {
					payload = streamPayload
					name = "stream.bin"
				}
				if _, err := server.Write(yencSinglePart(payload, name)); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
	provider := Provider{
		ID:          "fresh-isolation",
		Host:        "fresh-isolation.invalid:119",
		Factory:     factory,
		Connections: 1,
		Inflight:    2,
		SkipPing:    true,
	}
	client := fncoreClient(t, provider)
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	streamResult := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "stream@example.invalid", writer)
		streamResult <- err
	}()
	select {
	case <-writer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("collateral BODY did not commit to caller writer")
	}

	freshResult := make(chan error, 1)
	go func() {
		_, err := client.BodyTargeted(context.Background(), "fresh@example.invalid", TargetedBodyOptions{
			Provider:       provider.ID,
			FreshTransport: true,
		})
		freshResult <- err
	}()
	select {
	case err := <-freshResult:
		t.Fatalf("fresh request completed before collateral stream settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(writer.release)
	select {
	case err := <-streamResult:
		if err != nil {
			t.Fatalf("collateral committed BODY error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("collateral BODY did not settle")
	}
	select {
	case err := <-freshResult:
		if err != nil {
			t.Fatalf("fresh targeted BODY error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fresh targeted BODY did not complete after collateral settlement")
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("connections = %d, want a new transport after old collateral settled", got)
	}
}
