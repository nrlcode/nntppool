package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnightingale/rapidyenc"
)

const fncoreAdmissionProviderID = "fncore-admission"

func fncoreAdmissionResponse(_ int, command string) []byte {
	switch {
	case strings.HasPrefix(command, "BODY "):
		return yencSinglePart([]byte("bounded admission fixture"), "admission.bin")
	case strings.HasPrefix(command, "STAT "):
		return []byte("223 1 <admission@example.invalid> exists\r\n")
	default:
		return []byte("500 unexpected admission command\r\n")
	}
}

func fncoreAdmissionClient(t *testing.T, providers ...Provider) *Client {
	t.Helper()
	return newBreakerClient(t, newBreakerFakeClock(), providers...)
}

func fncoreQuotaExhaustedProvider(id, host string) (*regressionProvider, Provider) {
	server, provider := breakerProvider(id, host, fncoreAdmissionResponse)
	provider.QuotaBytes = 1
	provider.QuotaUsed = 1
	return server, provider
}

func fncoreAdmissionGroup(t *testing.T, client *Client, providerID string) *providerGroup {
	t.Helper()
	group := client.findGroup(providerID)
	if group == nil {
		t.Fatalf("provider %q was not registered", providerID)
	}
	return group
}

func fncoreRecordBreakerCompletions(t *testing.T, client *Client, providerID string, count int, completion circuitBreakerCompletion) {
	t.Helper()
	group := fncoreAdmissionGroup(t, client, providerID)
	for range count {
		lease, err := group.breaker.acquire(providerID)
		if err != nil {
			t.Fatalf("breaker preload acquire: %v", err)
		}
		group.breaker.complete(lease, completion)
	}
}

func fncoreRequireClosedResetBreaker(t *testing.T, client *Client, providerID string) {
	t.Helper()
	if stats := providerBreakerStats(t, client, providerID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
		t.Fatalf("breaker state = %+v, want closed with no qualifying failures", stats)
	}
}

func fncoreRequireQuotaEvidence(t *testing.T, err error, providerID string, operation Operation) {
	t.Helper()
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("error = %v, want ErrQuotaExceeded", err)
		return
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Errorf("quota error type = %T, want *TransportError", err)
		return
	}
	if transportErr.Kind != OutcomeProviderUnavailable || transportErr.ProviderID != providerID {
		t.Errorf("quota summary = kind %s provider %q, want %s from %q",
			transportErr.Kind, transportErr.ProviderID, OutcomeProviderUnavailable, providerID)
	}
	if len(transportErr.Attempts) != 1 {
		t.Errorf("quota attempts = %+v, want one eligibility record", transportErr.Attempts)
		return
	}
	attempt := transportErr.Attempts[0]
	if attempt.ProviderID != providerID || attempt.Operation != operation || attempt.Outcome != OutcomeProviderUnavailable {
		t.Errorf("quota attempt = %+v, want provider %q operation %s outcome %s",
			attempt, providerID, operation, OutcomeProviderUnavailable)
	}
}

func fncoreAdmissionProviderStats(t *testing.T, client *Client, providerID string) ProviderStats {
	t.Helper()
	for _, provider := range client.Stats().Providers {
		if provider.ProviderID == providerID {
			return provider
		}
	}
	t.Fatalf("provider %q missing from Stats()", providerID)
	return ProviderStats{}
}

func fncoreAdmissionContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestFNCOREQuotaSuppressesTargetedPublicPaths(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		invoke func(*testing.T, context.Context, *Client, string)
	}{
		{
			name:   "BODY",
			prefix: "BODY",
			invoke: func(t *testing.T, ctx context.Context, client *Client, providerID string) {
				t.Helper()
				_, err := client.BodyTargeted(ctx, "admission@example.invalid", TargetedBodyOptions{Provider: providerID})
				fncoreRequireQuotaEvidence(t, err, providerID, OperationBody)
			},
		},
		{
			name:   "STAT",
			prefix: "STAT",
			invoke: func(t *testing.T, ctx context.Context, client *Client, providerID string) {
				t.Helper()
				result, ok := <-client.StatMany(ctx, []string{"admission@example.invalid"}, StatManyOptions{
					Concurrency: 1,
					Provider:    providerID,
				})
				if !ok {
					t.Fatal("targeted StatMany returned no result")
				}
				fncoreRequireQuotaEvidence(t, result.Err, providerID, OperationStat)
			},
		},
		{
			name:   "SpeedTest",
			prefix: "BODY",
			invoke: func(t *testing.T, ctx context.Context, client *Client, providerID string) {
				t.Helper()
				result, err := client.SpeedTest(ctx, SpeedTestOptions{
					NZBReader:    testNZBReader("admission@example.invalid"),
					ProviderName: providerID,
				})
				if err != nil {
					t.Fatalf("quota-suppressed targeted SpeedTest: %v", err)
				}
				if result.SegmentsDone != 1 {
					t.Errorf("SpeedTest segments done = %d, want 1 settled eligibility result", result.SegmentsDone)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			targetServer, target := fncoreQuotaExhaustedProvider(fncoreAdmissionProviderID, "quota-target.invalid:119")
			otherServer, other := breakerProvider("fncore-other", "quota-other.invalid:119", fncoreAdmissionResponse)
			client := fncoreAdmissionClient(t, target, other)
			fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, 1, circuitBreakerFailure)
			before := providerBreakerStats(t, client, fncoreAdmissionProviderID)

			test.invoke(t, fncoreAdmissionContext(t), client, fncoreAdmissionProviderID)

			if got := targetServer.commandCount(test.prefix); got != 0 {
				t.Errorf("quota-exhausted target received %d %s commands, want 0", got, test.prefix)
			}
			if got := otherServer.commandCount(test.prefix); got != 0 {
				t.Errorf("targeted %s escaped to another provider %d times", test.name, got)
			}
			if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
				t.Errorf("quota rejection changed breaker: before=%+v after=%+v", before, after)
			}
		})
	}
}

type fncorePostProvider struct {
	connections       atomic.Int32
	posts             atomic.Int32
	finalCode         string
	closeBeforeStatus bool
}

func (p *fncorePostProvider) factory(context.Context) (net.Conn, error) {
	p.connections.Add(1)
	client, server := net.Pipe()
	go func() {
		defer func() { _ = server.Close() }()
		if _, err := io.WriteString(server, "200 admission server ready\r\n"); err != nil {
			return
		}
		reader := bufio.NewReader(server)
		command, err := reader.ReadString('\n')
		if err != nil || command != "POST\r\n" {
			return
		}
		p.posts.Add(1)
		if p.closeBeforeStatus {
			return
		}
		if _, err := io.WriteString(server, "340 send article\r\n"); err != nil {
			return
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == ".\r\n" {
				break
			}
		}
		code := p.finalCode
		if code == "" {
			code = "240 article accepted"
		}
		_, _ = io.WriteString(server, code+"\r\n")
	}()
	return client, nil
}

func fncorePostProviderConfig(id, host string, server *fncorePostProvider) Provider {
	return Provider{ID: id, Host: host, Factory: server.factory, Connections: 1, Inflight: 1, SkipPing: true}
}

func fncorePost(t *testing.T, ctx context.Context, client *Client) error {
	t.Helper()
	body := []byte("bounded post fixture")
	_, err := client.PostYenc(ctx, PostHeaders{
		From:       "fixture@example.invalid",
		Subject:    "FNCORE admission",
		Newsgroups: []string{"alt.test"},
		MessageID:  "<fncore-admission@example.invalid>",
		Date:       time.Unix(1_700_000_000, 0),
	}, bytes.NewReader(body), rapidyenc.Meta{
		FileName:   "admission.bin",
		FileSize:   int64(len(body)),
		PartNumber: 1,
		TotalParts: 1,
		PartSize:   int64(len(body)),
	})
	return err
}

func TestFNCOREOpenBreakerSuppressesPublicPaths(t *testing.T) {
	t.Run("POST", func(t *testing.T) {
		server := &fncorePostProvider{}
		client := fncoreAdmissionClient(t, fncorePostProviderConfig(fncoreAdmissionProviderID, "open-post.invalid:119", server))
		fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, providerBreakerFailureThreshold, circuitBreakerFailure)
		before := providerBreakerStats(t, client, fncoreAdmissionProviderID)

		err := fncorePost(t, fncoreAdmissionContext(t), client)
		if !errors.Is(err, ErrCircuitBreakerOpen) {
			t.Errorf("PostYenc error = %v, want ErrCircuitBreakerOpen", err)
		}
		if got := server.posts.Load(); got != 0 {
			t.Errorf("open-breaker POST reached provider %d times, want 0", got)
		}
		if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
			t.Errorf("open-breaker POST changed breaker: before=%+v after=%+v", before, after)
		}
	})

	t.Run("targeted SpeedTest", func(t *testing.T) {
		server, provider := breakerProvider(fncoreAdmissionProviderID, "open-speed.invalid:119", fncoreAdmissionResponse)
		client := fncoreAdmissionClient(t, provider)
		fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, providerBreakerFailureThreshold, circuitBreakerFailure)
		before := providerBreakerStats(t, client, fncoreAdmissionProviderID)

		result, err := client.SpeedTest(fncoreAdmissionContext(t), SpeedTestOptions{
			NZBReader:    testNZBReader("admission@example.invalid"),
			ProviderName: fncoreAdmissionProviderID,
		})
		if err != nil {
			t.Fatalf("open-breaker targeted SpeedTest: %v", err)
		}
		if result.SegmentsDone != 1 {
			t.Errorf("SpeedTest segments done = %d, want 1 settled eligibility result", result.SegmentsDone)
		}
		if got := server.commandCount("BODY"); got != 0 {
			t.Errorf("open-breaker targeted SpeedTest sent %d BODY commands, want 0", got)
		}
		if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
			t.Errorf("open-breaker SpeedTest changed breaker: before=%+v after=%+v", before, after)
		}
	})
}

func TestFNCORESuccessfulPublicPathsCompleteBreakerAccounting(t *testing.T) {
	t.Run("POST is one-shot and ignores download quota", func(t *testing.T) {
		server := &fncorePostProvider{}
		provider := fncorePostProviderConfig(fncoreAdmissionProviderID, "success-post.invalid:119", server)
		provider.QuotaBytes = 1
		provider.QuotaUsed = 1
		client := fncoreAdmissionClient(t, provider)
		fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, 1, circuitBreakerFailure)
		beforeQuota := fncoreAdmissionProviderStats(t, client, fncoreAdmissionProviderID)

		if err := fncorePost(t, fncoreAdmissionContext(t), client); err != nil {
			t.Fatalf("eligible PostYenc: %v", err)
		}
		if got := server.posts.Load(); got != 1 {
			t.Errorf("POST attempts = %d, want exactly 1", got)
		}
		afterQuota := fncoreAdmissionProviderStats(t, client, fncoreAdmissionProviderID)
		if afterQuota.QuotaUsed != beforeQuota.QuotaUsed || afterQuota.QuotaExceeded != beforeQuota.QuotaExceeded {
			t.Errorf("POST changed download quota: before=%d/%v after=%d/%v",
				beforeQuota.QuotaUsed, beforeQuota.QuotaExceeded, afterQuota.QuotaUsed, afterQuota.QuotaExceeded)
		}
		fncoreRequireClosedResetBreaker(t, client, fncoreAdmissionProviderID)
	})

	t.Run("targeted SpeedTest remains pinned and single-attempt", func(t *testing.T) {
		targetServer, target := breakerProvider(fncoreAdmissionProviderID, "success-speed.invalid:119", fncoreAdmissionResponse)
		otherServer, other := breakerProvider("fncore-other", "success-speed-other.invalid:119", fncoreAdmissionResponse)
		client := fncoreAdmissionClient(t, target, other)
		fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, 1, circuitBreakerFailure)

		result, err := client.SpeedTest(fncoreAdmissionContext(t), SpeedTestOptions{
			NZBReader:    testNZBReader("admission@example.invalid"),
			ProviderName: fncoreAdmissionProviderID,
		})
		if err != nil {
			t.Fatalf("eligible targeted SpeedTest: %v", err)
		}
		if result.SegmentsDone != 1 {
			t.Errorf("SpeedTest segments done = %d, want 1", result.SegmentsDone)
		}
		if got := targetServer.commandCount("BODY"); got != 1 {
			t.Errorf("targeted SpeedTest BODY attempts = %d, want exactly 1", got)
		}
		if got := otherServer.commandCount("BODY"); got != 0 {
			t.Errorf("targeted SpeedTest escaped to another provider %d times", got)
		}
		fncoreRequireClosedResetBreaker(t, client, fncoreAdmissionProviderID)
	})
}

func TestFNCOREPublicPathAttemptBounds(t *testing.T) {
	t.Run("POST rejection is one-shot", func(t *testing.T) {
		firstServer := &fncorePostProvider{finalCode: "441 posting failed"}
		secondServer := &fncorePostProvider{}
		client := fncoreAdmissionClient(t,
			fncorePostProviderConfig(fncoreAdmissionProviderID, "post-first.invalid:119", firstServer),
			fncorePostProviderConfig("fncore-other", "post-second.invalid:119", secondServer),
		)

		if err := fncorePost(t, fncoreAdmissionContext(t), client); err == nil {
			t.Fatal("rejected PostYenc error = nil")
		}
		if got := firstServer.posts.Load(); got != 1 {
			t.Errorf("first-provider POST attempts = %d, want exactly 1", got)
		}
		if got := secondServer.posts.Load(); got != 0 {
			t.Errorf("one-shot POST escaped to another provider %d times", got)
		}
	})

	t.Run("targeted SpeedTest temporary failure is one attempt", func(t *testing.T) {
		server, provider := breakerProvider(fncoreAdmissionProviderID, "speed-temporary.invalid:119", func(_ int, command string) []byte {
			if strings.HasPrefix(command, "BODY ") {
				return []byte("451 temporary failure\r\n")
			}
			return []byte("500 unexpected admission command\r\n")
		})
		client := fncoreAdmissionClient(t, provider)

		result, err := client.SpeedTest(fncoreAdmissionContext(t), SpeedTestOptions{
			NZBReader:    testNZBReader("admission@example.invalid"),
			ProviderName: fncoreAdmissionProviderID,
		})
		if err != nil {
			t.Fatalf("temporary targeted SpeedTest: %v", err)
		}
		if result.SegmentsDone != 1 {
			t.Errorf("SpeedTest segments done = %d, want 1", result.SegmentsDone)
		}
		if got := server.commandCount("BODY"); got != 1 {
			t.Errorf("temporary targeted SpeedTest BODY attempts = %d, want exactly 1", got)
		}
		stats := providerBreakerStats(t, client, fncoreAdmissionProviderID)
		if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 1 {
			t.Errorf("temporary targeted SpeedTest breaker completion = %+v, want one qualifying failure", stats)
		}
	})

	t.Run("targeted SpeedTest connection death is one attempt", func(t *testing.T) {
		server := &fncoreDisconnectProvider{}
		provider := Provider{
			ID: fncoreAdmissionProviderID, Host: "speed-disconnect.invalid:119",
			Factory: server.factory, Connections: 1, Inflight: 1, SkipPing: true,
		}
		client := fncoreAdmissionClient(t, provider)

		result, err := client.SpeedTest(fncoreAdmissionContext(t), SpeedTestOptions{
			NZBReader:    testNZBReader("admission@example.invalid"),
			ProviderName: fncoreAdmissionProviderID,
		})
		if err != nil {
			t.Fatalf("connection-death targeted SpeedTest: %v", err)
		}
		if result.SegmentsDone != 1 {
			t.Errorf("SpeedTest segments done = %d, want 1", result.SegmentsDone)
		}
		if got := server.bodies.Load(); got != 1 {
			t.Errorf("connection-death targeted SpeedTest BODY attempts = %d, want exactly 1", got)
		}
		if got := server.connections.Load(); got != 1 {
			t.Errorf("connection-death targeted SpeedTest connections = %d, want exactly 1", got)
		}
		stats := providerBreakerStats(t, client, fncoreAdmissionProviderID)
		if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 1 {
			t.Errorf("connection-death targeted SpeedTest breaker completion = %+v, want one qualifying failure", stats)
		}
	})
}

type fncoreDisconnectProvider struct {
	connections atomic.Int32
	bodies      atomic.Int32
}

func (p *fncoreDisconnectProvider) factory(context.Context) (net.Conn, error) {
	p.connections.Add(1)
	client, server := net.Pipe()
	go func() {
		defer func() { _ = server.Close() }()
		if _, err := io.WriteString(server, "200 admission server ready\r\n"); err != nil {
			return
		}
		command, err := bufio.NewReader(server).ReadString('\n')
		if err == nil && strings.HasPrefix(command, "BODY ") {
			p.bodies.Add(1)
		}
	}()
	return client, nil
}

func TestFNCOREPostTransportFailuresOpenBreaker(t *testing.T) {
	server := &fncorePostProvider{closeBeforeStatus: true}
	client := fncoreAdmissionClient(t,
		fncorePostProviderConfig(fncoreAdmissionProviderID, "post-disconnect.invalid:119", server),
	)

	for attempt := 1; attempt <= providerBreakerFailureThreshold; attempt++ {
		if err := fncorePost(t, fncoreAdmissionContext(t), client); err == nil {
			t.Errorf("transport-failure POST %d error = nil", attempt)
		}
		waitForProviderCapacity(t, client, fncoreAdmissionProviderID)
	}
	if got := server.posts.Load(); got != providerBreakerFailureThreshold {
		t.Errorf("qualifying POST wire attempts = %d, want %d", got, providerBreakerFailureThreshold)
	}
	if got := server.connections.Load(); got != providerBreakerFailureThreshold {
		t.Errorf("qualifying POST connections = %d, want %d", got, providerBreakerFailureThreshold)
	}
	if stats := providerBreakerStats(t, client, fncoreAdmissionProviderID); stats.State != CircuitBreakerOpen {
		t.Errorf("breaker after three POST transport failures = %+v, want open", stats)
	}

	err := fncorePost(t, fncoreAdmissionContext(t), client)
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("fourth POST error = %v, want ErrCircuitBreakerOpen", err)
	}
	if got := server.posts.Load(); got != providerBreakerFailureThreshold {
		t.Errorf("open breaker allowed a fourth POST: wire attempts = %d", got)
	}
	if got := server.connections.Load(); got != providerBreakerFailureThreshold {
		t.Errorf("open breaker allowed a fourth connection: connections = %d", got)
	}
}

func TestFNCOREHalfOpenTargetedSpeedTestUsesFreshTransport(t *testing.T) {
	clock := newBreakerFakeClock()
	server, provider := breakerProvider(fncoreAdmissionProviderID, "half-open-speed.invalid:119", fncoreAdmissionResponse)
	client := newBreakerClient(t, clock, provider)

	if _, err := client.Stat(fncoreAdmissionContext(t), "warm-socket@example.invalid"); err != nil {
		t.Fatalf("warm provider socket: %v", err)
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("warm provider connections = %d, want 1", got)
	}
	fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, providerBreakerFailureThreshold, circuitBreakerFailure)
	clock.Advance(providerBreakerCooldowns[0] + time.Nanosecond)

	result, err := client.SpeedTest(fncoreAdmissionContext(t), SpeedTestOptions{
		NZBReader:    testNZBReader("half-open-speed@example.invalid"),
		ProviderName: fncoreAdmissionProviderID,
	})
	if err != nil {
		t.Fatalf("half-open targeted SpeedTest: %v", err)
	}
	if result.SegmentsDone != 1 {
		t.Errorf("SpeedTest segments done = %d, want 1", result.SegmentsDone)
	}
	if got := server.commandCount("BODY"); got != 1 {
		t.Errorf("half-open targeted SpeedTest BODY attempts = %d, want exactly 1", got)
	}
	if got := server.connections.Load(); got != 2 {
		t.Errorf("half-open targeted SpeedTest connections = %d, want fresh second transport", got)
	}
	fncoreRequireClosedResetBreaker(t, client, fncoreAdmissionProviderID)
}

func TestFNCOREPostSkipsBreakerRejectedProviderBeforeOwnership(t *testing.T) {
	first := &fncorePostProvider{}
	second := &fncorePostProvider{}
	client := fncoreAdmissionClient(t,
		fncorePostProviderConfig(fncoreAdmissionProviderID, "post-open-first.invalid:119", first),
		fncorePostProviderConfig("fncore-other", "post-healthy-second.invalid:119", second),
	)
	fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, providerBreakerFailureThreshold, circuitBreakerFailure)
	before := providerBreakerStats(t, client, fncoreAdmissionProviderID)

	if err := fncorePost(t, fncoreAdmissionContext(t), client); err != nil {
		t.Fatalf("POST after first-provider breaker rejection: %v", err)
	}
	if got := first.connections.Load(); got != 0 {
		t.Errorf("breaker-rejected first provider connections = %d, want 0", got)
	}
	if got := first.posts.Load(); got != 0 {
		t.Errorf("breaker-rejected first provider POST attempts = %d, want 0", got)
	}
	if got := second.connections.Load(); got != 1 {
		t.Errorf("healthy second provider connections = %d, want 1", got)
	}
	if got := second.posts.Load(); got != 1 {
		t.Errorf("healthy second provider POST attempts = %d, want exactly 1", got)
	}
	if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
		t.Errorf("pretransport POST rejection changed first breaker: before=%+v after=%+v", before, after)
	}
}

func TestFNCOREAdmissionDeadlineEvidenceRemainsCancellation(t *testing.T) {
	server, provider := breakerProvider(fncoreAdmissionProviderID, "deadline-evidence.invalid:119", fncoreAdmissionResponse)
	client := fncoreAdmissionClient(t, provider)
	fncoreRecordBreakerCompletions(t, client, fncoreAdmissionProviderID, providerBreakerFailureThreshold, circuitBreakerFailure)
	before := providerBreakerStats(t, client, fncoreAdmissionProviderID)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := client.BodyTargeted(ctx, "deadline-evidence@example.invalid", TargetedBodyOptions{Provider: fncoreAdmissionProviderID})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired admission error = %v, want context deadline", err)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("expired admission error type = %T, want *TransportError", err)
	}
	if transportErr.Kind != OutcomeCancellation || len(transportErr.Attempts) != 1 {
		t.Fatalf("expired admission evidence = %+v, want one cancellation attempt", transportErr)
	}
	attempt := transportErr.Attempts[0]
	if attempt.ProviderID != fncoreAdmissionProviderID || attempt.Operation != OperationBody ||
		attempt.Outcome != OutcomeCancellation || !errors.Is(attempt.Cause, context.DeadlineExceeded) {
		t.Errorf("expired admission attempt = %+v, want caller-deadline BODY cancellation", attempt)
	}
	if got := server.connections.Load(); got != 0 {
		t.Errorf("expired admission provider connections = %d, want 0", got)
	}
	if got := server.commandCount("BODY"); got != 0 {
		t.Errorf("expired admission BODY commands = %d, want 0", got)
	}
	if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
		t.Errorf("expired admission changed breaker: before=%+v after=%+v", before, after)
	}
}

func TestFNCOREClientDeadlineEvidenceRemainsCancellation(t *testing.T) {
	server, provider := breakerProvider(fncoreAdmissionProviderID, "client-deadline-evidence.invalid:119", fncoreAdmissionResponse)
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelClient()
	client, err := NewClient(
		clientCtx,
		[]Provider{provider},
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
		WithSpeedAwareDispatch(false),
		WithProviderCircuitBreaker(true),
		withCircuitBreakerClock(newBreakerFakeClock()),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	before := providerBreakerStats(t, client, fncoreAdmissionProviderID)
	select {
	case <-clientCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client parent deadline did not expire")
	}
	if !errors.Is(clientCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("client context error = %v, want context deadline", clientCtx.Err())
	}

	_, err = client.BodyTargeted(context.Background(), "client-deadline-evidence@example.invalid", TargetedBodyOptions{Provider: fncoreAdmissionProviderID})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("client-deadline admission error = %v, want context deadline", err)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("client-deadline admission error type = %T, want *TransportError", err)
	}
	if transportErr.Kind != OutcomeCancellation || len(transportErr.Attempts) != 1 {
		t.Fatalf("client-deadline admission evidence = %+v, want one cancellation attempt", transportErr)
	}
	attempt := transportErr.Attempts[0]
	if attempt.ProviderID != fncoreAdmissionProviderID || attempt.Operation != OperationBody ||
		attempt.Outcome != OutcomeCancellation || !errors.Is(attempt.Cause, context.DeadlineExceeded) {
		t.Errorf("client-deadline admission attempt = %+v, want client-deadline BODY cancellation", attempt)
	}
	if got := server.connections.Load(); got != 0 {
		t.Errorf("client-deadline provider connections = %d, want 0", got)
	}
	if got := server.commandCount("BODY"); got != 0 {
		t.Errorf("client-deadline BODY commands = %d, want 0", got)
	}
	if after := providerBreakerStats(t, client, fncoreAdmissionProviderID); after != before {
		t.Errorf("client-deadline admission changed breaker: before=%+v after=%+v", before, after)
	}
}
