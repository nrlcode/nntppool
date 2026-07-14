package nntppool

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type breakerFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newBreakerFakeClock() *breakerFakeClock {
	return &breakerFakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *breakerFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *breakerFakeClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

func breakerProvider(id, host string, respond func(int, string) []byte) (*regressionProvider, Provider) {
	server := &regressionProvider{host: host, respond: respond}
	provider := server.provider(false)
	provider.ID = id
	return server, provider
}

func newBreakerClient(t *testing.T, clock circuitBreakerClock, providers ...Provider) *Client {
	t.Helper()
	client, err := NewClient(
		context.Background(),
		providers,
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
		WithSpeedAwareDispatch(false),
		WithProviderCircuitBreaker(true),
		withCircuitBreakerClock(clock),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func targetedBreakerStat(client *Client, providerID string) error {
	resultCh := client.StatMany(context.Background(), []string{"fixture@example.invalid"}, StatManyOptions{
		Concurrency: 1,
		Provider:    providerID,
	})
	result, ok := <-resultCh
	if !ok {
		return errors.New("targeted STAT returned no result")
	}
	return result.Err
}

func providerBreakerStats(t *testing.T, client *Client, providerID string) CircuitBreakerStats {
	t.Helper()
	for _, provider := range client.Stats().Providers {
		if provider.ProviderID == providerID {
			return provider.CircuitBreaker
		}
	}
	t.Fatalf("provider %q missing from Stats()", providerID)
	return CircuitBreakerStats{}
}

func waitForProviderCapacity(t *testing.T, client *Client, providerID string) {
	t.Helper()
	var group *providerGroup
	for _, groups := range [...]*[]*providerGroup{client.mainGroups.Load(), client.backupGroups.Load()} {
		for _, candidate := range *groups {
			if candidate.id == providerID {
				group = candidate
				break
			}
		}
	}
	if group == nil {
		t.Fatalf("provider %q missing", providerID)
	}
	deadline := time.Now().Add(2 * time.Second)
	for group.gate.available.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("provider %q did not return to idle capacity", providerID)
		}
		runtime.Gosched()
	}
}

func requireBreakerTransportKind(t *testing.T, err error, want OutcomeKind) *TransportError {
	t.Helper()
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Errorf("error = %v, want TransportError", err)
		return nil
	}
	if transportErr.Kind != want {
		t.Errorf("transport kind = %s, want %s", transportErr.Kind, want)
	}
	if errors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("error = %v, bootstrap classification was hidden by breaker-open", err)
	}
	return transportErr
}

func requireBreakerConfigurationError(t *testing.T, err error) {
	t.Helper()
	_ = requireBreakerTransportKind(t, err, OutcomeProviderUnavailable)
	if !errors.Is(err, ErrInvalidProviderConfiguration) {
		t.Errorf("error = %v, want ErrInvalidProviderConfiguration", err)
	}
}

func targetedBreakerErrors(client *Client, providerID string, count int) []error {
	start := make(chan struct{})
	results := make(chan error, count)
	for range count {
		go func() {
			<-start
			results <- targetedBreakerStat(client, providerID)
		}()
	}
	close(start)
	errs := make([]error, 0, count)
	for range count {
		errs = append(errs, <-results)
	}
	return errs
}

func authRejectingBreakerFactory(context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer func() { _ = server.Close() }()
		if _, err := io.WriteString(server, "200 regression server ready\r\n"); err != nil {
			return
		}
		reader := bufio.NewReader(server)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		if _, err := io.WriteString(server, "381 password required\r\n"); err != nil {
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		_, _ = io.WriteString(server, "481 authentication rejected\r\n")
	}()
	return client, nil
}

type breakerDialTimeoutError struct{}

func (breakerDialTimeoutError) Error() string   { return "deterministic provider dial timeout" }
func (breakerDialTimeoutError) Timeout() bool   { return true }
func (breakerDialTimeoutError) Temporary() bool { return true }

func TestPR2CircuitBreakerIsOptInAndUsesOneEligibilityPath(t *testing.T) {
	var disabledCommands atomic.Int32
	disabledServer, disabledProvider := breakerProvider(
		"disabled",
		"breaker-disabled.invalid:119",
		func(int, string) []byte {
			disabledCommands.Add(1)
			return []byte("451 temporary failure\r\n")
		},
	)
	disabled, err := NewClient(
		context.Background(),
		[]Provider{disabledProvider},
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
		WithSpeedAwareDispatch(false),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = disabled.Close() })

	for range 4 {
		if err := targetedBreakerStat(disabled, "disabled"); err == nil {
			t.Fatal("targeted STAT error = nil, want temporary failure")
		}
	}
	if got := disabledServer.commandCount("STAT"); got != 8 {
		t.Fatalf("disabled breaker STAT attempts = %d, want 8 (two internal 451 attempts per request)", got)
	}
	disabledStats := providerBreakerStats(t, disabled, "disabled")
	if disabledStats.State != CircuitBreakerDisabled || disabledStats.QualifyingFailures != 0 {
		t.Fatalf("disabled breaker stats = %+v", disabledStats)
	}
	group := (*disabled.mainGroups.Load())[0]
	if group.breaker == nil {
		t.Fatal("disabled client bypassed the shared provider eligibility object")
	}

	clock := newBreakerFakeClock()
	_, enabledProvider := breakerProvider(
		"enabled",
		"breaker-enabled.invalid:119",
		func(int, string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
	)
	enabled := newBreakerClient(t, clock, enabledProvider)
	enabledStats := providerBreakerStats(t, enabled, "enabled")
	if enabledStats.State != CircuitBreakerClosed || enabledStats.QualifyingFailures != 0 {
		t.Fatalf("enabled breaker initial stats = %+v", enabledStats)
	}
	if (*enabled.mainGroups.Load())[0].breaker == nil {
		t.Fatal("enabled client did not use the shared provider eligibility object")
	}
}

func TestPR2CircuitBreakerCountsOneFinalFailurePerRequest(t *testing.T) {
	clock := newBreakerFakeClock()
	server, provider := breakerProvider(
		"temporary",
		"breaker-temporary.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	client := newBreakerClient(t, clock, provider)

	for request := 1; request <= 3; request++ {
		if err := targetedBreakerStat(client, "temporary"); err == nil {
			t.Fatalf("request %d error = nil, want temporary failure", request)
		}
		stats := providerBreakerStats(t, client, "temporary")
		if request < 3 {
			if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != request {
				t.Fatalf("request %d breaker stats = %+v", request, stats)
			}
		} else if stats.State != CircuitBreakerOpen || stats.QualifyingFailures != 0 || stats.Cooldown != 10*time.Second {
			t.Fatalf("third request breaker stats = %+v, want open with 10s cooldown", stats)
		}
	}
	if got := server.commandCount("STAT"); got != 6 {
		t.Fatalf("provider STAT attempts = %d, want 6 internal attempts from three distinct requests", got)
	}
}

func TestPR2CircuitBreakerUsesRollingFailureWindow(t *testing.T) {
	clock := newBreakerFakeClock()
	_, provider := breakerProvider(
		"window",
		"breaker-window.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	client := newBreakerClient(t, clock, provider)

	for range 2 {
		_ = targetedBreakerStat(client, "window")
	}
	clock.Advance(31 * time.Second)
	_ = targetedBreakerStat(client, "window")
	stats := providerBreakerStats(t, client, "window")
	if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 1 {
		t.Fatalf("post-window stats = %+v, want one retained failure", stats)
	}
	for range 2 {
		_ = targetedBreakerStat(client, "window")
	}
	stats = providerBreakerStats(t, client, "window")
	if stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second {
		t.Fatalf("within-window threshold stats = %+v", stats)
	}
}

func TestPR2CircuitBreakerQualifyingFailures(t *testing.T) {
	serviceReq := &Request{Ctx: context.Background()}
	serviceReq.writtenAt.Store(10)
	serviceReq.responseHeadAt.Store(20)
	collateralReq := &Request{Ctx: context.Background()}
	collateralReq.writtenAt.Store(10)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledReq := &Request{Ctx: cancelledCtx}

	tests := []struct {
		name      string
		resp      Response
		ok        bool
		cancelled bool
		want      circuitBreakerCompletion
	}{
		{
			name: "final 451 after internal retry counts once",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{
				{Outcome: OutcomeTemporaryFailure, ResponseCode: 451},
				{Outcome: OutcomeTemporaryFailure, ResponseCode: 451},
			}},
			ok:   true,
			want: circuitBreakerFailure,
		},
		{
			name: "provider response timeout",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{
				Outcome: OutcomeTransportFailure, ProviderResponseTimeout: true,
			}}},
			ok:   true,
			want: circuitBreakerFailure,
		},
		{
			name: "direct response-head transport failure",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeTransportFailure}}},
			ok:   true,
			want: circuitBreakerFailure,
		},
		{
			name: "dial transport failure after internal retry",
			resp: Response{Request: &Request{Ctx: context.Background()}, Attempts: []AttemptEvidence{{Outcome: OutcomeTransportFailure}}},
			ok:   true,
			want: circuitBreakerFailure,
		},
		{
			name: "local queue cancellation",
			resp: Response{Request: cancelledReq, Attempts: []AttemptEvidence{{Outcome: OutcomeCancellation}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "collateral pipeline cancellation",
			resp: Response{Request: collateralReq, Attempts: []AttemptEvidence{{Outcome: OutcomeTransportFailure}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "hard absence",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeHardArticleAbsence, ResponseCode: 430}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "caller cancellation",
			resp: Response{Request: cancelledReq, Attempts: []AttemptEvidence{{Outcome: OutcomeCancellation}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "health preemption",
			resp: Response{Request: cancelledReq, Attempts: []AttemptEvidence{{Outcome: OutcomeCancellation}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "isolated corruption",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeCorruptBody}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "provider unavailable",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeProviderUnavailable}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name: "unknown response",
			resp: Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeInconclusive, ResponseCode: 499}}},
			ok:   true,
			want: circuitBreakerNeutral,
		},
		{
			name:      "client cancellation flag",
			resp:      Response{Request: serviceReq, Attempts: []AttemptEvidence{{Outcome: OutcomeTransportFailure}}},
			ok:        true,
			cancelled: true,
			want:      circuitBreakerNeutral,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyCircuitBreakerCompletion(test.resp, test.ok, test.cancelled); got != test.want {
				t.Fatalf("completion = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPR2OpenProviderIsSkippedForEligibleAlternative(t *testing.T) {
	clock := newBreakerFakeClock()
	primary, primaryProvider := breakerProvider(
		"primary",
		"breaker-primary.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	alternative, alternativeProvider := breakerProvider(
		"alternative",
		"breaker-alternative.invalid:119",
		func(int, string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
	)
	client := newBreakerClient(t, clock, primaryProvider, alternativeProvider)

	for range 3 {
		_ = targetedBreakerStat(client, "primary")
	}
	waitForProviderCapacity(t, client, "primary")
	primaryBefore := primary.commandCount("STAT")
	result, err := client.Stat(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if result.ProviderID != "alternative" {
		t.Fatalf("serving provider = %q, want alternative", result.ProviderID)
	}
	if got := primary.commandCount("STAT"); got != primaryBefore {
		t.Fatalf("open primary received %d new STAT commands", got-primaryBefore)
	}
	if got := alternative.commandCount("STAT"); got != 1 {
		t.Fatalf("alternative STAT commands = %d, want 1", got)
	}
	if len(result.Attempts) != 2 || !errors.Is(result.Attempts[0].Cause, ErrCircuitBreakerOpen) || result.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("ordered attempts = %+v, want breaker skip then alternative success", result.Attempts)
	}
}

func TestPR2HalfOpenTransportProbeIsExclusive(t *testing.T) {
	clock := newBreakerFakeClock()
	var recovering atomic.Bool
	probeStarted := make(chan struct{}, 1)
	probeRelease := make(chan struct{})
	server, provider := breakerProvider(
		"exclusive",
		"breaker-exclusive.invalid:119",
		func(int, string) []byte {
			if !recovering.Load() {
				return []byte("451 temporary failure\r\n")
			}
			select {
			case probeStarted <- struct{}{}:
			default:
			}
			<-probeRelease
			return []byte("223 1 <fixture@example.invalid> exists\r\n")
		},
	)
	client := newBreakerClient(t, clock, provider)
	for range 3 {
		_ = targetedBreakerStat(client, "exclusive")
	}
	recovering.Store(true)
	clock.Advance(10 * time.Second)
	before := server.commandCount("STAT")

	const callers = 32
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			results <- targetedBreakerStat(client, "exclusive")
		}()
	}
	close(start)
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("half-open transport probe did not reach provider")
	}

	for range callers - 1 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrCircuitBreakerOpen) {
				t.Fatalf("concurrent half-open caller error = %v, want breaker-open", err)
			}
			var breakerErr *CircuitBreakerError
			if !errors.As(err, &breakerErr) || breakerErr.State != CircuitBreakerHalfOpen || breakerErr.ProviderID != "exclusive" {
				t.Fatalf("structured half-open error = %#v", breakerErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent half-open caller did not receive immediate structured outcome")
		}
	}
	if got := server.commandCount("STAT") - before; got != 1 {
		t.Fatalf("half-open provider commands = %d, want exactly one exclusive probe", got)
	}
	stats := providerBreakerStats(t, client, "exclusive")
	if stats.State != CircuitBreakerHalfOpen || !stats.ProbeInFlight {
		t.Fatalf("inflight half-open stats = %+v", stats)
	}

	close(probeRelease)
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("half-open probe error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("successful half-open probe did not complete")
	}
	stats = providerBreakerStats(t, client, "exclusive")
	if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 || stats.Cooldown != 0 || stats.ProbeInFlight {
		t.Fatalf("successful half-open reset stats = %+v", stats)
	}
}

func TestPR2HalfOpenCooldownProgressionIsCapped(t *testing.T) {
	clock := newBreakerFakeClock()
	server, provider := breakerProvider(
		"cooldown",
		"breaker-cooldown.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	client := newBreakerClient(t, clock, provider)
	for range 3 {
		_ = targetedBreakerStat(client, "cooldown")
	}

	for _, expected := range []time.Duration{20, 40, 80, 120, 120, 120} {
		current := providerBreakerStats(t, client, "cooldown")
		clock.Advance(current.Cooldown)
		before := server.commandCount("STAT")
		_ = targetedBreakerStat(client, "cooldown")
		if got := server.commandCount("STAT") - before; got != 2 {
			t.Fatalf("half-open internal attempts = %d, want 2", got)
		}
		stats := providerBreakerStats(t, client, "cooldown")
		if stats.State != CircuitBreakerOpen || stats.Cooldown != expected*time.Second {
			t.Fatalf("post-probe stats = %+v, want cooldown %v", stats, expected*time.Second)
		}
	}
}

func TestPR2OnlyOpenProviderReturnsStructuredTemporaryOutcome(t *testing.T) {
	clock := newBreakerFakeClock()
	server, provider := breakerProvider(
		"only",
		"breaker-only.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	client := newBreakerClient(t, clock, provider)
	for range 3 {
		_ = targetedBreakerStat(client, "only")
	}
	before := server.commandCount("STAT")

	_, err := client.Stat(context.Background(), "fixture@example.invalid")
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeTemporaryFailure || transportErr.ProviderID != "only" {
		t.Fatalf("only-provider error = %v, want provider-tagged temporary TransportError", err)
	}
	var breakerErr *CircuitBreakerError
	if !errors.As(err, &breakerErr) {
		t.Fatalf("only-provider error = %v, want CircuitBreakerError", err)
	}
	if breakerErr.ProviderID != "only" || breakerErr.State != CircuitBreakerOpen || !breakerErr.RetryAt.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("structured breaker error = %+v", breakerErr)
	}
	if got := server.commandCount("STAT") - before; got != 0 {
		t.Fatalf("open only provider received %d commands before cooldown", got)
	}
	stats := providerBreakerStats(t, client, "only")
	if stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second || !stats.OpenUntil.Equal(breakerErr.RetryAt) {
		t.Fatalf("open provider stats = %+v", stats)
	}
}

func TestPR2CallerCancellationPrecedesOpenProviderEligibility(t *testing.T) {
	clock := newBreakerFakeClock()
	server, provider := breakerProvider(
		"cancelled",
		"breaker-cancelled.invalid:119",
		func(int, string) []byte { return []byte("451 temporary failure\r\n") },
	)
	client := newBreakerClient(t, clock, provider)
	for range 3 {
		_ = targetedBreakerStat(client, "cancelled")
	}
	before := server.commandCount("STAT")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Stat(ctx, "fixture@example.invalid")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("canceled request error = %v, breaker eligibility hid caller cancellation", err)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeCancellation {
		t.Fatalf("canceled request error = %v, want cancellation TransportError", err)
	}
	if got := server.commandCount("STAT") - before; got != 0 {
		t.Fatalf("canceled request sent %d provider commands", got)
	}
	stats := providerBreakerStats(t, client, "cancelled")
	if stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second {
		t.Fatalf("caller cancellation changed breaker state: %+v", stats)
	}
}

func TestPR2SuccessfulRequestResetsClosedBreakerWindow(t *testing.T) {
	clock := newBreakerFakeClock()
	var healthy atomic.Bool
	_, provider := breakerProvider(
		"reset",
		"breaker-reset.invalid:119",
		func(int, string) []byte {
			if healthy.Load() {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return []byte("451 temporary failure\r\n")
		},
	)
	client := newBreakerClient(t, clock, provider)
	for range 2 {
		_ = targetedBreakerStat(client, "reset")
	}
	healthy.Store(true)
	if err := targetedBreakerStat(client, "reset"); err != nil {
		t.Fatalf("successful reset STAT error = %v", err)
	}
	stats := providerBreakerStats(t, client, "reset")
	if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
		t.Fatalf("successful request did not reset breaker window: %+v", stats)
	}
}

func TestPR2BootstrapAuthenticationAndConfigurationDoNotTripBreaker(t *testing.T) {
	t.Run("authentication rejection", func(t *testing.T) {
		provider := Provider{
			ID:          "auth-rejected",
			Host:        "auth-rejected.invalid:119",
			Factory:     authRejectingBreakerFactory,
			Auth:        Auth{Username: "fixture-user", Password: "fixture-password"},
			Connections: 3,
			Inflight:    1,
			SkipPing:    true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for request, err := range targetedBreakerErrors(client, provider.ID, 3) {
			_ = requireBreakerTransportKind(t, err, OutcomeProviderUnavailable)
			if !errors.Is(err, ErrAuthRejected) {
				t.Errorf("request %d error = %v, want ErrAuthRejected", request+1, err)
			}
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("authentication failures changed breaker state: %+v", stats)
		}
	})

	t.Run("missing password configuration", func(t *testing.T) {
		greetingFactory := func(context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = io.WriteString(server, "200 regression server ready\r\n")
			}()
			return client, nil
		}
		provider := Provider{
			ID:          "missing-password",
			Host:        "missing-password.invalid:119",
			Factory:     greetingFactory,
			Auth:        Auth{Username: "fixture-user"},
			Connections: 3,
			Inflight:    1,
			SkipPing:    true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for _, err := range targetedBreakerErrors(client, provider.ID, 3) {
			requireBreakerConfigurationError(t, err)
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("missing-password configuration changed breaker state: %+v", stats)
		}
	})

	t.Run("malformed address configuration", func(t *testing.T) {
		provider := Provider{
			ID:          "malformed-address",
			Host:        "missing-port",
			Connections: 3,
			Inflight:    1,
			SkipPing:    true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for _, err := range targetedBreakerErrors(client, provider.ID, 3) {
			requireBreakerConfigurationError(t, err)
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("malformed-address configuration changed breaker state: %+v", stats)
		}
	})

	t.Run("invalid TLS policy configuration", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}
		stop := make(chan struct{})
		t.Cleanup(func() {
			close(stop)
			_ = listener.Close()
		})
		go func() {
			for {
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				go func() {
					<-stop
					_ = conn.Close()
				}()
			}
		}()
		provider := Provider{
			ID:   "invalid-tls-policy",
			Host: listener.Addr().String(),
			TLSConfig: &tls.Config{
				ServerName: "fixture.invalid",
				MinVersion: tls.VersionTLS13,
				MaxVersion: tls.VersionTLS12,
			},
			Connections: 3,
			Inflight:    1,
			SkipPing:    true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for _, requestErr := range targetedBreakerErrors(client, provider.ID, 3) {
			requireBreakerConfigurationError(t, requestErr)
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("invalid TLS policy changed breaker state: %+v", stats)
		}
	})
}

func TestPR2RealTransportLifecycleFeedsBreakerAccounting(t *testing.T) {
	t.Run("provider response timeout counts", func(t *testing.T) {
		hungFactory := func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				if _, err := io.WriteString(server, "200 regression server ready\r\n"); err != nil {
					return
				}
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				<-ctx.Done()
			}()
			return client, nil
		}
		provider := Provider{
			ID:             "response-timeout",
			Host:           "response-timeout.invalid:119",
			Factory:        hungFactory,
			Connections:    1,
			Inflight:       1,
			SkipPing:       true,
			AttemptTimeout: 20 * time.Millisecond,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for request := 1; request <= 3; request++ {
			transportErr := requireBreakerTransportKind(t, targetedBreakerStat(client, provider.ID), OutcomeTransportFailure)
			if transportErr == nil || len(transportErr.Attempts) == 0 || !transportErr.Attempts[len(transportErr.Attempts)-1].ProviderResponseTimeout {
				t.Errorf("request %d error = %v, want real provider response-timeout evidence", request, transportErr)
			}
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second {
			t.Fatalf("provider response timeouts did not open breaker: %+v", stats)
		}
	})

	t.Run("genuine dial timeout counts", func(t *testing.T) {
		provider := Provider{
			ID:   "dial-timeout",
			Host: "dial-timeout.invalid:119",
			Factory: func(context.Context) (net.Conn, error) {
				return nil, breakerDialTimeoutError{}
			},
			Connections: 3,
			Inflight:    1,
			SkipPing:    true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		for request, err := range targetedBreakerErrors(client, provider.ID, 3) {
			transportErr := requireBreakerTransportKind(t, err, OutcomeTransportFailure)
			if transportErr == nil || len(transportErr.Attempts) == 0 || transportErr.Attempts[len(transportErr.Attempts)-1].ProviderResponseTimeout {
				t.Errorf("request %d error = %v, want non-response provider dial timeout", request+1, transportErr)
			}
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerOpen || stats.Cooldown != 10*time.Second {
			t.Fatalf("genuine provider dial failures did not open breaker: %+v", stats)
		}
	})

	t.Run("local pipeline wait cancellation does not count", func(t *testing.T) {
		firstCommand := make(chan struct{})
		release := make(chan struct{})
		var firstOnce, releaseOnce sync.Once
		releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(releaseServer)
		factory := func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				if _, err := io.WriteString(server, "200 regression server ready\r\n"); err != nil {
					return
				}
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				firstOnce.Do(func() { close(firstCommand) })
				select {
				case <-release:
				case <-ctx.Done():
				}
			}()
			return client, nil
		}
		provider := Provider{
			ID:             "local-cancellation",
			Host:           "local-cancellation.invalid:119",
			Factory:        factory,
			Connections:    1,
			Inflight:       1,
			StatInflight:   1,
			SkipPing:       true,
			AttemptTimeout: time.Second,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		firstResp := client.Send(firstCtx, []byte("STAT <first@example.invalid>\r\n"), nil)
		select {
		case <-firstCommand:
		case <-time.After(2 * time.Second):
			t.Fatal("first STAT did not occupy the provider pipeline")
		}

		queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 25*time.Millisecond)
		_, queuedErr := client.Stat(queuedCtx, "queued@example.invalid")
		cancelQueued()
		if !errors.Is(queuedErr, context.DeadlineExceeded) {
			t.Errorf("queued request error = %v, want context deadline", queuedErr)
		}
		cancelFirst()
		first := <-firstResp
		if !errors.Is(first.Err, context.Canceled) {
			t.Errorf("first request error = %v, want caller cancellation", first.Err)
		}
		releaseServer()
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("local cancellations changed breaker state: %+v", stats)
		}
	})

	t.Run("collateral pipeline loss does not count", func(t *testing.T) {
		var connections atomic.Int32
		firstCommand := make(chan string, 1)
		bothWritten := make(chan struct{})
		releaseMalformed := make(chan struct{})
		var releaseOnce sync.Once
		releaseFirstResponse := func() { releaseOnce.Do(func() { close(releaseMalformed) }) }
		t.Cleanup(releaseFirstResponse)
		factory := func(ctx context.Context) (net.Conn, error) {
			connection := connections.Add(1)
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				if _, err := io.WriteString(server, "200 regression server ready\r\n"); err != nil {
					return
				}
				reader := bufio.NewReader(server)
				if connection == 1 {
					command, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					firstCommand <- command
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					close(bothWritten)
					select {
					case <-releaseMalformed:
					case <-ctx.Done():
						return
					}
					_, _ = io.WriteString(server, "222 0 <fixture@example.invalid> body follows\r\nmalformed body\r\n.\r\n")
					return
				}
				for {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					if _, err := io.WriteString(server, "430 no such article\r\n"); err != nil {
						return
					}
				}
			}()
			return client, nil
		}
		provider := Provider{
			ID:           "collateral-pipeline",
			Host:         "collateral-pipeline.invalid:119",
			Factory:      factory,
			Connections:  1,
			Inflight:     2,
			StatInflight: 2,
			SkipPing:     true,
		}
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		type bodyResult struct {
			body *ArticleBody
			err  error
		}
		bodyDone := make(chan bodyResult, 1)
		go func() {
			body, err := client.Body(ctx, "pipeline-body@example.invalid")
			bodyDone <- bodyResult{body: body, err: err}
		}()
		select {
		case command := <-firstCommand:
			if operationFromPayload([]byte(command)) != OperationBody {
				t.Fatalf("first pipelined command = %q, want BODY", command)
			}
		case <-ctx.Done():
			t.Fatal("first transport did not receive BODY")
		}
		statResponse := client.Send(ctx, []byte("STAT <pipeline-stat@example.invalid>\r\n"), nil)
		select {
		case <-bothWritten:
		case <-ctx.Done():
			t.Fatal("STAT was not written behind BODY")
		}
		for client.Stats().Providers[0].PipelineInUse < 2 {
			select {
			case <-ctx.Done():
				t.Fatal("both commands did not become pending before transport loss")
			default:
				runtime.Gosched()
			}
		}
		releaseFirstResponse()

		bodyOutcome := <-bodyDone
		var bodyErr *TransportError
		if bodyOutcome.body != nil || !errors.As(bodyOutcome.err, &bodyErr) || bodyErr.Kind != OutcomeCorruptBody {
			t.Fatalf("BODY result = body %v error %v, want isolated corruption", bodyOutcome.body, bodyOutcome.err)
		}
		response := <-statResponse
		if response.Err != nil || (response.StatusCode != 423 && response.StatusCode != 430) {
			t.Fatalf("retried collateral response = %+v, want hard absence", response)
		}
		if len(response.Attempts) < 2 || response.Attempts[0].Outcome != OutcomeTransportFailure || response.Attempts[len(response.Attempts)-1].Outcome != OutcomeHardArticleAbsence {
			t.Fatalf("collateral attempts = %+v, want transport loss then hard absence", response.Attempts)
		}
		initial := response.Attempts[0]
		if initial.PipelineHeadWaitDuration <= 0 || initial.ResponseServiceDuration != 0 {
			t.Fatalf("collateral evidence = %+v, want written request that never reached response head", initial)
		}
		if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
			t.Fatalf("retried/collateral pipeline loss changed breaker state: %+v", stats)
		}
	})
}

func ExampleWithProviderCircuitBreaker() {
	option := WithProviderCircuitBreaker(true)
	fmt.Printf("%T\n", option)
	// Output: nntppool.ClientOption
}
