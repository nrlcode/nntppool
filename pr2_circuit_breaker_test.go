package nntppool

import (
	"context"
	"errors"
	"fmt"
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

func ExampleWithProviderCircuitBreaker() {
	option := WithProviderCircuitBreaker(true)
	fmt.Printf("%T\n", option)
	// Output: nntppool.ClientOption
}
