package nntppool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	providerBreakerFailureThreshold = 3
	providerBreakerFailureWindow    = 30 * time.Second
)

var providerBreakerCooldowns = [...]time.Duration{
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	80 * time.Second,
	120 * time.Second,
}

// ErrCircuitBreakerOpen identifies a provider request suppressed by the
// opt-in circuit breaker. Use errors.As with [CircuitBreakerError] for the
// provider, state, and retry time.
var ErrCircuitBreakerOpen = errors.New("nntp: provider circuit breaker open")

// CircuitBreakerState is the in-memory eligibility state of one provider.
type CircuitBreakerState string

const (
	CircuitBreakerDisabled CircuitBreakerState = "disabled"
	CircuitBreakerClosed   CircuitBreakerState = "closed"
	CircuitBreakerOpen     CircuitBreakerState = "open"
	CircuitBreakerHalfOpen CircuitBreakerState = "half_open"
)

// CircuitBreakerStats is a point-in-time provider breaker snapshot.
// Breaker state is transient transport state and is never durable article
// availability evidence.
type CircuitBreakerStats struct {
	State              CircuitBreakerState
	QualifyingFailures int
	OpenUntil          time.Time
	Cooldown           time.Duration
	ProbeInFlight      bool
}

// CircuitBreakerError reports why a provider was not eligible for a request.
// RetryAt is the end of the current cooldown. During an in-flight half-open
// probe it may already be in the past; callers should wait for a later request
// rather than polling inside nntppool.
type CircuitBreakerError struct {
	ProviderID string
	State      CircuitBreakerState
	RetryAt    time.Time
}

func (e *CircuitBreakerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v: provider %s is %s until %s", ErrCircuitBreakerOpen, e.ProviderID, e.State, e.RetryAt.Format(time.RFC3339Nano))
}

func (e *CircuitBreakerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return ErrCircuitBreakerOpen
}

type circuitBreakerClock interface {
	Now() time.Time
}

type wallCircuitBreakerClock struct{}

func (wallCircuitBreakerClock) Now() time.Time { return time.Now() }

type circuitBreakerCompletion uint8

const (
	circuitBreakerNeutral circuitBreakerCompletion = iota
	circuitBreakerSuccess
	circuitBreakerFailure
)

type circuitBreakerLease struct {
	breaker    *providerCircuitBreaker
	generation uint64
	enabled    bool
	probe      bool
}

type providerCircuitBreaker struct {
	mu sync.Mutex

	enabled bool
	clock   circuitBreakerClock

	state         CircuitBreakerState
	failures      []time.Time
	cooldownIndex int
	openUntil     time.Time
	probeInFlight bool
	generation    uint64
}

func newProviderCircuitBreaker(enabled bool, clock circuitBreakerClock) *providerCircuitBreaker {
	if clock == nil {
		clock = wallCircuitBreakerClock{}
	}
	state := CircuitBreakerDisabled
	if enabled {
		state = CircuitBreakerClosed
	}
	return &providerCircuitBreaker{
		enabled: enabled,
		clock:   clock,
		state:   state,
	}
}

// acquire is the single provider eligibility boundary used whether the
// breaker is disabled, closed, open, or half-open. A disabled breaker is a
// no-op lease rather than a separate legacy dispatch path.
func (b *providerCircuitBreaker) acquire(providerID string) (circuitBreakerLease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	lease := circuitBreakerLease{breaker: b, generation: b.generation, enabled: b.enabled}
	if !b.enabled {
		return lease, nil
	}
	if b.state == CircuitBreakerClosed {
		return lease, nil
	}

	now := b.clock.Now()
	state := CircuitBreakerOpen
	if !now.Before(b.openUntil) {
		state = CircuitBreakerHalfOpen
		if !b.probeInFlight {
			b.probeInFlight = true
			lease.probe = true
			return lease, nil
		}
	}
	return circuitBreakerLease{}, &CircuitBreakerError{
		ProviderID: providerID,
		State:      state,
		RetryAt:    b.openUntil,
	}
}

func (b *providerCircuitBreaker) complete(lease circuitBreakerLease, completion circuitBreakerCompletion) {
	if !lease.enabled || lease.breaker != b {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// A completion can mutate only the breaker generation that admitted it.
	// In particular, an old success must not reset a newer open generation.
	if lease.generation != b.generation {
		return
	}

	// A successful request proves the provider is usable now, including when
	// it was admitted in the current generation.
	if completion == circuitBreakerSuccess {
		b.resetLocked()
		return
	}

	if lease.probe {
		if !b.probeInFlight {
			return
		}
		b.probeInFlight = false
		if completion == circuitBreakerFailure {
			if b.cooldownIndex < len(providerBreakerCooldowns)-1 {
				b.cooldownIndex++
			}
			b.openLocked()
		}
		return
	}
	if completion != circuitBreakerFailure || b.state != CircuitBreakerClosed {
		return
	}

	now := b.clock.Now()
	b.pruneFailuresLocked(now)
	b.failures = append(b.failures, now)
	if len(b.failures) >= providerBreakerFailureThreshold {
		b.cooldownIndex = 0
		b.openLocked()
	}
}

func (b *providerCircuitBreaker) pruneFailuresLocked(now time.Time) {
	cutoff := now.Add(-providerBreakerFailureWindow)
	first := 0
	for first < len(b.failures) && b.failures[first].Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(b.failures, b.failures[first:])
		b.failures = b.failures[:len(b.failures)-first]
	}
}

func (b *providerCircuitBreaker) openLocked() {
	b.state = CircuitBreakerOpen
	b.failures = b.failures[:0]
	b.probeInFlight = false
	b.openUntil = b.clock.Now().Add(providerBreakerCooldowns[b.cooldownIndex])
	b.generation++
}

func (b *providerCircuitBreaker) resetLocked() {
	b.state = CircuitBreakerClosed
	b.failures = b.failures[:0]
	b.cooldownIndex = 0
	b.openUntil = time.Time{}
	b.probeInFlight = false
	b.generation++
}

func (b *providerCircuitBreaker) snapshot() CircuitBreakerStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.enabled {
		return CircuitBreakerStats{State: CircuitBreakerDisabled}
	}
	state := b.state
	if state == CircuitBreakerClosed {
		b.pruneFailuresLocked(b.clock.Now())
	} else if !b.clock.Now().Before(b.openUntil) {
		state = CircuitBreakerHalfOpen
	}
	stats := CircuitBreakerStats{
		State:              state,
		QualifyingFailures: len(b.failures),
		OpenUntil:          b.openUntil,
		ProbeInFlight:      b.probeInFlight,
	}
	if b.state == CircuitBreakerOpen {
		stats.Cooldown = providerBreakerCooldowns[b.cooldownIndex]
	}
	return stats
}

// classifyCircuitBreakerCompletion maps one final provider request—not each
// internal transport retry—to breaker accounting. PR1 attempt evidence is the
// authority for response-timeout attribution and collateral cancellation.
func classifyCircuitBreakerCompletion(resp Response, ok, cancelled bool) circuitBreakerCompletion {
	if cancelled || !ok || len(resp.Attempts) == 0 {
		return circuitBreakerNeutral
	}
	final := resp.Attempts[len(resp.Attempts)-1]
	switch final.Outcome {
	case OutcomeSuccess:
		return circuitBreakerSuccess
	case outcomeLocalFailure:
		return circuitBreakerNeutral
	case OutcomeTemporaryFailure:
		if errors.Is(final.Cause, ErrCircuitBreakerOpen) {
			return circuitBreakerNeutral
		}
		return circuitBreakerFailure
	case OutcomeTransportFailure:
		if final.ProviderResponseTimeout {
			return circuitBreakerFailure
		}
		if resp.Request == nil {
			return circuitBreakerNeutral
		}
		writtenAt := resp.Request.writtenAt.Load()
		headAt := resp.Request.responseHeadAt.Load()
		// A failure before command write is a provider connection/bootstrap
		// failure. A failure at response head is provider service failure. A
		// written request that never became head is collateral pipeline loss.
		if writtenAt == 0 || headAt > 0 {
			return circuitBreakerFailure
		}
	}
	return circuitBreakerNeutral
}
