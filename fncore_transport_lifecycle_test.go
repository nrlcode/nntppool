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

type partialErrorWriter struct {
	err   error
	bytes bytes.Buffer
}

type timeoutSinkError struct{}

func (timeoutSinkError) Error() string   { return "local sink timeout" }
func (timeoutSinkError) Timeout() bool   { return true }
func (timeoutSinkError) Temporary() bool { return true }

type readWithEOFConn struct {
	data []byte
}

func (c *readWithEOFConn) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.data)
	c.data = c.data[n:]
	return n, io.EOF
}

func (*readWithEOFConn) Write(p []byte) (int, error)      { return len(p), nil }
func (*readWithEOFConn) Close() error                     { return nil }
func (*readWithEOFConn) LocalAddr() net.Addr              { return nil }
func (*readWithEOFConn) RemoteAddr() net.Addr             { return nil }
func (*readWithEOFConn) SetDeadline(time.Time) error      { return nil }
func (*readWithEOFConn) SetReadDeadline(time.Time) error  { return nil }
func (*readWithEOFConn) SetWriteDeadline(time.Time) error { return nil }

type blockingCloseConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCloseConn) Close() error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return c.Conn.Close()
}

func (w *partialErrorWriter) Write(p []byte) (int, error) {
	n := max(1, len(p)/2)
	_, _ = w.bytes.Write(p[:n])
	return n, w.err
}

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

func fncoreProviderGroup(t *testing.T, client *Client, providerID string) *providerGroup {
	t.Helper()
	for _, groups := range [...]*[]*providerGroup{client.mainGroups.Load(), client.backupGroups.Load()} {
		for _, group := range *groups {
			if group.id == providerID {
				return group
			}
		}
	}
	t.Fatalf("provider group %q not found", providerID)
	return nil
}

func waitForFNCOREGateAvailability(t *testing.T, group *providerGroup, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for group.gate.available.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("provider gate available = %d, want %d", group.gate.available.Load(), want)
		}
		runtime.Gosched()
	}
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

func TestFNCOREStreamingClassifierIsCaseInsensitive(t *testing.T) {
	for _, test := range []struct {
		name       string
		command    string
		operation  Operation
		statusCode string
	}{
		{name: "mixed BODY", command: "bOdY", operation: Operation("BODY"), statusCode: "222"},
		{name: "mixed ARTICLE", command: "aRtIcLe", operation: Operation("ARTICLE"), statusCode: "220"},
	} {
		t.Run(test.name, func(t *testing.T) {
			firstPayload := []byte("first-" + test.name)
			secondPayload := []byte("backup-" + test.name)
			responseFor := func(payload []byte, name string) []byte {
				response := yencSinglePart(payload, name)
				return append([]byte(test.statusCode), response[3:]...)
			}
			first := &regressionProvider{
				host: "stream-primary-" + test.statusCode + ".invalid:119",
				respond: func(int, string) []byte {
					return responseFor(firstPayload, "stream-first.bin")
				},
			}
			backup := &regressionProvider{
				host: "stream-backup-" + test.statusCode + ".invalid:119",
				respond: func(int, string) []byte {
					return responseFor(secondPayload, "stream-backup.bin")
				},
			}
			client := fncoreClient(t, first.provider(false), backup.provider(true))
			writer := &removeProviderWriter{client: client, provider: first.host}

			response := <-client.Send(
				context.Background(),
				[]byte(test.command+" <fixture@example.invalid>\r\n"),
				writer,
			)
			if response.Err == nil {
				t.Fatalf("mixed-case %s error = nil, want terminal committed-attempt error", test.operation)
			}
			if writer.removeErr != nil {
				t.Fatalf("RemoveProvider() error = %v", writer.removeErr)
			}
			if !bytes.Equal(writer.bytes.Bytes(), firstPayload) {
				t.Fatalf("%s caller sink = %q, want only %q", test.operation, writer.bytes.Bytes(), firstPayload)
			}
			if got := backup.commandCount(test.command); got != 0 {
				t.Fatalf("backup %s attempts = %d, want zero after commitment", test.operation, got)
			}
			if len(response.Attempts) == 0 || response.Attempts[len(response.Attempts)-1].Operation != test.operation {
				t.Fatalf("%s attempts = %+v, want factual operation", test.operation, response.Attempts)
			}
		})
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
	backup, backupProvider := breakerProvider(
		"local-writer-backup",
		"local-writer-backup.invalid:119",
		func(int, string) []byte {
			return yencSinglePart([]byte("backup must not be used"), "backup.bin")
		},
	)
	backupProvider.Backup = true
	client := newBreakerClient(t, newBreakerFakeClock(), provider, backupProvider)

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
		if cause := transportErr.Attempts[len(transportErr.Attempts)-1].Cause; cause == nil || cause.Error() != writerErr.Error() {
			t.Fatalf("request %d attempt cause = %v, want unprefixed local cause %q", request, cause, writerErr)
		} else if !errors.Is(cause, writerErr) {
			t.Fatalf("request %d attempt cause = %v, want errors.Is local cause %q", request, cause, writerErr)
		}
	}
	if got := server.commandCount("BODY"); got != 3 {
		t.Fatalf("healthy provider BODY commands = %d, want 3", got)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after terminal local writer faults", got)
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
	backup := &regressionProvider{
		host: "short-writer-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("backup must not be used"), "backup.bin")
		},
	}
	client := fncoreClient(t, provider, backup.provider(true))

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", zeroProgressWriter{})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("BodyStream() error = %v, want io.ErrShortWrite", err)
	}
	if got := client.Stats().Providers[0].Errors; got != 0 {
		t.Fatalf("provider errors = %d, want local short write excluded", got)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after terminal short write", got)
	}
}

func TestFNCOREPartialCallerWriterErrorPreservesCauseAndNeverReplays(t *testing.T) {
	writerErr := errors.New("partial local sink failure")
	primaryPayload := bytes.Repeat([]byte("p"), 32*1024)
	primary := &regressionProvider{
		host: "partial-writer.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart(primaryPayload, "partial.bin")
		},
	}
	backup := &regressionProvider{
		host: "partial-writer-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("backup must not be appended"), "backup.bin")
		},
	}
	client := fncoreClient(t, primary.provider(false), backup.provider(true))
	writer := &partialErrorWriter{err: writerErr}

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
	if !errors.Is(err, writerErr) {
		t.Fatalf("BodyStream() error = %v, want underlying partial writer error", err)
	}
	if writer.bytes.Len() == 0 || writer.bytes.Len() >= len(primaryPayload) {
		t.Fatalf("partial caller bytes = %d, want nonzero prefix below %d", writer.bytes.Len(), len(primaryPayload))
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after partial local write", got)
	}
	if got := client.Stats().Providers[0].Errors; got != 0 {
		t.Fatalf("provider errors = %d, want partial local sink failure excluded", got)
	}
}

func TestFNCORETimeoutLikeCallerWriterErrorRemainsLocal(t *testing.T) {
	primary := &regressionProvider{
		host: "timeout-writer.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("healthy provider payload"), "timeout.bin")
		},
	}
	backup := &regressionProvider{
		host: "timeout-writer-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("backup must not be used"), "backup.bin")
		},
	}
	client := fncoreClient(t, primary.provider(false), backup.provider(true))
	writerErr := timeoutSinkError{}

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", failingWriter{err: writerErr})
	if !errors.Is(err, writerErr) {
		t.Fatalf("BodyStream() error = %v, want timeout-like local sink error", err)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || len(transportErr.Attempts) == 0 {
		t.Fatalf("BodyStream() error = %v, want structured attempt", err)
	}
	attempt := transportErr.Attempts[len(transportErr.Attempts)-1]
	if attempt.Outcome != OutcomeKind("local_failure") || attempt.ProviderResponseTimeout {
		t.Fatalf("timeout-like local attempt = %+v, want local failure without provider timeout", attempt)
	}
	if got := client.Stats().Providers[0].Errors; got != 0 {
		t.Fatalf("provider errors = %d, want timeout-like local error excluded", got)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after timeout-like local error", got)
	}
}

func TestFNCOREBackupCommittedWriterRemovalIsTerminal(t *testing.T) {
	missing := &regressionProvider{
		host:    "missing-main.invalid:119",
		respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
	}
	firstBackup := &regressionProvider{
		host: "committing-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("first backup payload"), "first-backup.bin")
		},
	}
	secondBackup := &regressionProvider{
		host: "unused-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not be appended"), "unused.bin")
		},
	}
	client := fncoreClient(t, missing.provider(false), firstBackup.provider(true), secondBackup.provider(true))
	writer := &removeProviderWriter{client: client, provider: firstBackup.host}

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
	if err == nil {
		t.Fatal("BodyStream() error = nil, want terminal committed backup error")
	}
	if got := secondBackup.commandCount("BODY"); got != 0 {
		t.Fatalf("later backup BODY attempts = %d, want zero after committed backup", got)
	}
}

func TestFNCOREStatWinnerCommittedRemovalDoesNotReachBackup(t *testing.T) {
	missing := &regressionProvider{
		host:    "probe-missing.invalid:119",
		respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
	}
	winner := &regressionProvider{
		host: "probe-winner.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("winner payload"), "winner.bin")
		},
	}
	probeMiss := &regressionProvider{
		host:    "probe-other.invalid:119",
		respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
	}
	backup := &regressionProvider{
		host: "probe-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not be appended"), "backup.bin")
		},
	}
	client := newRegressionClient(
		t,
		missing.provider(false),
		winner.provider(false),
		probeMiss.provider(false),
		backup.provider(true),
	)
	writer := &removeProviderWriter{client: client, provider: winner.host}

	_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
	if err == nil {
		t.Fatal("BodyStream() error = nil, want terminal committed STAT-winner error")
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY attempts = %d, want zero after committed STAT winner", got)
	}
}

func TestFNCOREReadWithDataAndEOFFeedsFinalBytes(t *testing.T) {
	connection := &readWithEOFConn{data: []byte("complete")}
	var received []byte
	feeder := &mockFeeder{feedFunc: func(in []byte, _ io.Writer) (int, bool, error) {
		received = append(received, in[0])
		return 1, len(received) == len("complete"), nil
	}}
	var buffer readBuffer

	err := buffer.feedUntilDone(connection, feeder, io.Discard, func(int) (time.Time, bool) {
		return time.Time{}, false
	})
	if err != nil {
		t.Fatalf("feedUntilDone() error = %v, want final bytes consumed before EOF", err)
	}
	if string(received) != "complete" {
		t.Fatalf("received = %q, want complete", received)
	}
}

func TestFNCORELargeCommandAutoFlushCannotStrandReader(t *testing.T) {
	commandSeen := make(chan []byte, 1)
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			command, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			commandSeen <- bytes.Clone(command)
			_, _ = server.Write([]byte("200 large command accepted\r\n"))
		}()
		return client, nil
	}
	client := fncoreClient(t, Provider{
		ID:          "large-command",
		Host:        "large-command.invalid:119",
		Factory:     factory,
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	})
	payload := []byte("XTEST " + strings.Repeat("x", 8*1024) + "\r\n")
	responseCh := client.Send(context.Background(), payload, nil)
	select {
	case response := <-responseCh:
		if response.Err != nil || response.StatusCode != 200 {
			t.Fatalf("large-command response = %+v", response)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("large auto-flushed command stranded response reader")
	}
	select {
	case command := <-commandSeen:
		if !bytes.Equal(command, payload) {
			t.Fatalf("server command length/content = %d/%q, want exact %d-byte payload", len(command), command, len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive large command")
	}
}

func TestFNCOREBootstrapFreshRejectionSettlesDeferredNormal(t *testing.T) {
	wireRead := make(chan []byte, 1)
	connection := mockServer(t, func(server net.Conn) {
		_, _ = server.Write([]byte("200 regression server ready\r\n"))
		_ = server.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 256)
		n, _ := server.Read(buffer)
		wireRead <- bytes.Clone(buffer[:n])
	})
	reqCh := make(chan *Request)
	nntpConnection, err := newNNTPConnectionFromConn(
		context.Background(), connection, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() { _ = nntpConnection.Close() })

	freshResponse := make(chan Response, 1)
	deferredResponse := make(chan Response, 1)
	nntpConnection.firstReq = &Request{
		Ctx:            context.Background(),
		Payload:        []byte("BODY <fresh@example.invalid>\r\n"),
		RespCh:         freshResponse,
		FreshTransport: true,
		submittedAt:    nntpConnection.createdAt.Add(time.Second),
	}
	nntpConnection.secondReq = &Request{
		Ctx:     context.Background(),
		Payload: []byte("STAT <deferred@example.invalid>\r\n"),
		RespCh:  deferredResponse,
	}
	go nntpConnection.Run()

	assertSingleSettlement := func(name string, responses <-chan Response, want error) {
		t.Helper()
		select {
		case response, ok := <-responses:
			if !ok {
				t.Fatalf("%s response channel closed without a response", name)
			}
			if !errors.Is(response.Err, want) {
				t.Fatalf("%s error = %v, want %v", name, response.Err, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s request was stranded", name)
		}
		select {
		case response, ok := <-responses:
			if ok {
				t.Fatalf("%s settled more than once: %+v", name, response)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s response channel was not closed after settlement", name)
		}
	}
	assertSingleSettlement("fresh bootstrap", freshResponse, errFreshTransportRequired)
	assertSingleSettlement("deferred normal", deferredResponse, ErrConnectionDied)
	select {
	case wire := <-wireRead:
		if len(wire) != 0 {
			t.Fatalf("bootstrap rejection wrote commands to wire: %q", wire)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe deterministic bootstrap retirement")
	}
}

func TestFNCOREPublicFreshBootstrapAdmissionSucceeds(t *testing.T) {
	server := &regressionProvider{
		host: "fresh-bootstrap-public.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <normal@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("fresh bootstrap"), "fresh.bin")
		},
	}
	provider := server.provider(false)
	provider.ID = "fresh-bootstrap-public"
	client := fncoreClient(t, provider)

	body, err := client.BodyTargeted(context.Background(), "fresh@example.invalid", TargetedBodyOptions{
		Provider:       provider.ID,
		FreshTransport: true,
	})
	if err != nil {
		t.Fatalf("first public fresh bootstrap error = %v", err)
	}
	if body == nil || body.ProviderID != provider.ID || body.BytesDecoded != len("fresh bootstrap") {
		t.Fatalf("first public fresh bootstrap result = %+v", body)
	}
	if _, err := client.Stat(context.Background(), "normal@example.invalid"); err != nil {
		t.Fatalf("normal request after public fresh bootstrap error = %v", err)
	}
	if got := server.commandCount("BODY"); got != 1 {
		t.Fatalf("public fresh bootstrap BODY commands = %d, want one", got)
	}
	if got := server.commandCount("STAT"); got != 1 {
		t.Fatalf("normal STAT commands = %d, want one", got)
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("public bootstrap connections = %d, want one", got)
	}
}

func TestFNCOREHalfOpenCompletionWaitsForSocketRetirement(t *testing.T) {
	clock := newBreakerFakeClock()
	var recovering atomic.Bool
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	var closeReleaseOnce sync.Once
	releaseClose := func() { closeReleaseOnce.Do(func() { close(closeRelease) }) }
	defer releaseClose()
	writerErr := errors.New("retiring local writer")
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
				_, _ = server.Write(yencSinglePart([]byte("healthy provider body"), "healthy.bin"))
			}
		}()
		if recovering.Load() {
			return &blockingCloseConn{Conn: client, started: closeStarted, release: closeRelease}, nil
		}
		return client, nil
	}
	provider := Provider{
		ID:          "half-open-retirement",
		Host:        "half-open-retirement.invalid:119",
		Factory:     factory,
		Connections: 2,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, clock, provider)
	group := fncoreProviderGroup(t, client, provider.ID)
	for range providerBreakerFailureThreshold {
		_ = targetedBreakerStat(client, provider.ID)
	}
	recovering.Store(true)
	clock.Advance(providerBreakerCooldowns[0])

	result := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", failingWriter{err: writerErr})
		result <- err
	}()
	select {
	case <-closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("writer-error socket did not begin retirement")
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerHalfOpen || !stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("breaker completed before socket retirement: %+v", stats)
	}
	select {
	case err := <-result:
		t.Fatalf("half-open request returned before socket retirement: %v", err)
	default:
	}
	if err := targetedBreakerStat(client, provider.ID); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("second half-open request error = %v, want breaker-open during socket retirement", err)
	}
	if got := group.gate.available.Load(); got != int32(provider.Connections-1) {
		t.Fatalf("provider gate available during retirement = %d, want %d", got, provider.Connections-1)
	}
	releaseClose()
	select {
	case err := <-result:
		if !errors.Is(err, writerErr) {
			t.Fatalf("retired half-open request error = %v, want local writer cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("half-open request did not return after socket retirement")
	}
	stats = providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("breaker state after retired local writer = %+v, want closed neutral state", stats)
	}
	waitForFNCOREGateAvailability(t, group, int32(provider.Connections))
}

func TestFNCORESuccessfulHalfOpenReturnsAfterPipelineSettlement(t *testing.T) {
	clock := newBreakerFakeClock()
	var recovering atomic.Bool
	server, provider := breakerProvider(
		"half-open-success-settlement",
		"half-open-success-settlement.invalid:119",
		func(_ int, _ string) []byte {
			if !recovering.Load() {
				return []byte("451 temporary failure\r\n")
			}
			return []byte("223 1 <fixture@example.invalid> exists\r\n")
		},
	)
	provider.Connections = 2
	provider.Inflight = 1
	provider.StatInflight = 1
	client := newBreakerClient(t, clock, provider)
	group := fncoreProviderGroup(t, client, provider.ID)
	for range providerBreakerFailureThreshold {
		_ = targetedBreakerStat(client, provider.ID)
	}
	commandsBeforeRecovery := server.commandCount("STAT")
	recovering.Store(true)
	clock.Advance(providerBreakerCooldowns[0])

	if err := targetedBreakerStat(client, provider.ID); err != nil {
		t.Fatalf("successful half-open STAT error = %v", err)
	}
	stats := providerBreakerStats(t, client, provider.ID)
	if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("successful half-open breaker state = %+v, want closed", stats)
	}
	providerStats := client.Stats().Providers[0]
	if providerStats.PipelineInUse != 0 {
		t.Fatalf("successful half-open returned with %d pipeline slots still in use", providerStats.PipelineInUse)
	}
	if got := group.gate.available.Load(); got < 1 {
		t.Fatalf("successful half-open returned without provider gate capacity: %d", got)
	}
	if got := server.commandCount("STAT"); got != commandsBeforeRecovery+1 {
		t.Fatalf("successful half-open STAT commands = %d, want %d", got, commandsBeforeRecovery+1)
	}
}

func TestFNCORENearMissCommandDoesNotTriggerStatProbe(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
	}{
		{name: "BODYX", payload: "BODYX <fixture@example.invalid>\r\n"},
		{name: "arbitrary identifier syntax", payload: "XTEST <arbitrary-identifier@example.invalid>\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			first := &regressionProvider{
				host:    "near-miss-first.invalid:119",
				respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
			}
			second := &regressionProvider{
				host: "near-miss-second.invalid:119",
				respond: func(_ int, payload string) []byte {
					if strings.HasPrefix(payload, "STAT") {
						return []byte("223 1 <fixture@example.invalid> exists\r\n")
					}
					return []byte("200 near-miss command accepted\r\n")
				},
			}
			third := &regressionProvider{
				host:    "near-miss-third.invalid:119",
				respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
			}
			client := newRegressionClient(t, first.provider(false), second.provider(false), third.provider(false))
			response := <-client.Send(
				context.Background(),
				[]byte(test.payload),
				nil,
			)
			if response.Err != nil || response.StatusCode != 200 {
				t.Fatalf("%s response = %+v, want direct fallback success", test.name, response)
			}
			for providerIndex, provider := range []*regressionProvider{first, second, third} {
				if got := provider.commandCount("STAT"); got != 0 {
					t.Fatalf("provider %d near-miss STAT probes = %d, want zero", providerIndex, got)
				}
			}
			command := strings.Fields(test.payload)[0]
			if got := second.commandCount(command); got != 1 {
				t.Fatalf("near-miss direct commands = %d, want one", got)
			}
			if len(response.Attempts) == 0 {
				t.Fatal("near-miss response omitted attempt evidence")
			}
			for attemptIndex, attempt := range response.Attempts {
				if attempt.Operation != OperationUnknown {
					t.Fatalf("attempt %d operation = %q, want UNKNOWN", attemptIndex, attempt.Operation)
				}
			}
		})
	}
}

func TestFNCOREProviderResponseTimeoutStartsAfterCommandFlush(t *testing.T) {
	commandRead := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			time.Sleep(150 * time.Millisecond)
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			close(commandRead)
			_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
		}()
		return client, nil
	}
	client := fncoreClient(t, Provider{
		ID:             "flush-before-timeout",
		Host:           "flush-before-timeout.invalid:119",
		Factory:        factory,
		Connections:    1,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.Stat(ctx, "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Stat() error = %v, command flush delay is not provider response time", err)
	}
	if result == nil || result.ProviderID != "flush-before-timeout" {
		t.Fatalf("Stat() result = %+v", result)
	}
	select {
	case <-commandRead:
	default:
		t.Fatal("server did not observe flushed command")
	}
}

func TestFNCOREStaleSuccessCannotResetActiveHalfOpenGeneration(t *testing.T) {
	clock := newBreakerFakeClock()
	breaker := newProviderCircuitBreaker(true, clock)
	stale, err := breaker.acquire("provider")
	if err != nil {
		t.Fatalf("stale acquire error = %v", err)
	}
	for range providerBreakerFailureThreshold {
		lease, acquireErr := breaker.acquire("provider")
		if acquireErr != nil {
			t.Fatalf("closed acquire error = %v", acquireErr)
		}
		breaker.complete(lease, circuitBreakerFailure)
	}
	clock.Advance(providerBreakerCooldowns[0])
	probe, err := breaker.acquire("provider")
	if err != nil || !probe.probe {
		t.Fatalf("half-open acquire = %+v, %v", probe, err)
	}

	breaker.complete(stale, circuitBreakerSuccess)
	stats := breaker.snapshot()
	if stats.State != CircuitBreakerHalfOpen || !stats.ProbeInFlight {
		t.Fatalf("stale pre-open success reset active probe: %+v", stats)
	}
	breaker.complete(probe, circuitBreakerFailure)
	stats = breaker.snapshot()
	if stats.State != CircuitBreakerOpen || stats.Cooldown != 20*time.Second {
		t.Fatalf("failed half-open probe progression = %+v, want reopened 20s cooldown", stats)
	}
}

func TestFNCORECommandTokenNearMissAndRawBodyCompatibility(t *testing.T) {
	if operation := operationFromPayload([]byte("BODYX <fixture@example.invalid>\r\n")); operation != OperationUnknown {
		t.Fatalf("BODYX operation = %q, want UNKNOWN", operation)
	}
	provider := &regressionProvider{
		host:    "raw-body-compat.invalid:119",
		respond: func(int, string) []byte { return []byte("222 body follows\r\n.\r\n") },
	}
	client := fncoreClient(t, provider.provider(false))
	response := <-client.Send(context.Background(), []byte("bOdY <fixture@example.invalid>\r\n"), nil)
	if response.Err != nil || response.StatusCode != 222 {
		t.Fatalf("raw mixed-case BODY response = %+v, want unvalidated success", response)
	}
	if len(response.Attempts) != 1 || response.Attempts[0].BodyValidation != BodyValidationNotRequested {
		t.Fatalf("raw BODY attempts = %+v, want validation not requested", response.Attempts)
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
		StallTimeout:   120 * time.Millisecond,
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
	select {
	case earlyProbeErr := <-probeResult:
		close(writerRelease)
		t.Fatalf("canceled half-open BODY returned before caller writer settled: %v", earlyProbeErr)
	case <-time.After(50 * time.Millisecond):
		// A committed caller writer keeps transport ownership until Write settles.
	}

	secondErr := targetedBreakerStat(client, provider.ID)
	if !errors.Is(secondErr, ErrCircuitBreakerOpen) {
		t.Fatalf("concurrent half-open request error = %v, want breaker-open until transport settles", secondErr)
	}
	close(writerRelease)
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
	bodyCommandSeen := make(chan struct{})
	statCommandSeen := make(chan struct{})
	releaseResponses := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		connection := connections.Add(1)
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if connection == 1 {
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				close(bodyCommandSeen)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				close(statCommandSeen)
				<-releaseResponses
				if _, err := server.Write(yencSinglePart(streamPayload, "stream.bin")); err != nil {
					return
				}
				_, _ = server.Write([]byte("223 1 <pending@example.invalid> exists\r\n"))
				return
			}
			for {
				_, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if _, err := server.Write(yencSinglePart(freshPayload, "fresh.bin")); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
	provider := Provider{
		ID:           "fresh-isolation",
		Host:         "fresh-isolation.invalid:119",
		Factory:      factory,
		Connections:  1,
		Inflight:     2,
		StatInflight: 3,
		SkipPing:     true,
	}
	client := fncoreClient(t, provider)
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	streamResult := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "stream@example.invalid", writer)
		streamResult <- err
	}()
	select {
	case <-bodyCommandSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive collateral BODY")
	}
	statResult := make(chan error, 1)
	go func() {
		_, err := client.Stat(context.Background(), "pending@example.invalid")
		statResult <- err
	}()
	select {
	case <-statCommandSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive pending collateral STAT")
	}
	close(releaseResponses)
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
	case err := <-statResult:
		if err != nil {
			t.Fatalf("pending collateral STAT error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pending collateral STAT did not settle")
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
