package nntppool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func fncoreCHG005Provider(id, host string, quotaBytes, quotaUsed int64, quotaPeriod time.Duration, resetAt time.Time) (*regressionProvider, Provider) {
	server := &regressionProvider{host: host, respond: fncoreAdmissionResponse}
	provider := server.provider(false)
	provider.ID = id
	provider.QuotaBytes = quotaBytes
	provider.QuotaUsed = quotaUsed
	provider.QuotaPeriod = quotaPeriod
	provider.QuotaResetAt = resetAt
	return server, provider
}

func fncoreCHG005QuotaStates(stats ClientStats) map[string]ProviderQuotaState {
	states := make(map[string]ProviderQuotaState, len(stats.Providers))
	for _, provider := range stats.Providers {
		states[provider.ProviderID] = ProviderQuotaState{
			Used:    provider.QuotaUsed,
			ResetAt: provider.QuotaResetAt,
		}
	}
	return states
}

func fncoreCHG005RequireState(t *testing.T, client *Client, providerID string, want ProviderQuotaState) ProviderStats {
	t.Helper()
	got := fncoreAdmissionProviderStats(t, client, providerID)
	if got.QuotaUsed != want.Used || !got.QuotaResetAt.Equal(want.ResetAt) {
		t.Fatalf("provider %q quota = (%d, %v), want (%d, %v)",
			providerID, got.QuotaUsed, got.QuotaResetAt, want.Used, want.ResetAt)
	}
	return got
}

func fncoreCHG005RequireStatesEqual(t *testing.T, got, want map[string]ProviderQuotaState) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("quota state count = %d, want %d", len(got), len(want))
	}
	for providerID, wantState := range want {
		gotState, ok := got[providerID]
		if !ok {
			t.Fatalf("provider %q missing from quota states", providerID)
		}
		if gotState.Used != wantState.Used || !gotState.ResetAt.Equal(wantState.ResetAt) {
			t.Fatalf("provider %q quota = (%d, %v), want (%d, %v)",
				providerID, gotState.Used, gotState.ResetAt, wantState.Used, wantState.ResetAt)
		}
	}
}

func TestFNCORECHG005QuotaRestoreUsesCanonicalIDsAndExactState(t *testing.T) {
	future := time.Unix(1_900_000_000, 123)
	expired := time.Unix(1_600_000_000, 456)
	_, lifetime := fncoreCHG005Provider("quota-lifetime", "new-lifetime.invalid:119", 100, 3, 0, time.Time{})
	_, periodicB := fncoreCHG005Provider("quota-b", "new-b.invalid:119", 100, 4, time.Hour, future)
	_, periodicA := fncoreCHG005Provider("quota-a", "new-a.invalid:119", 200, 5, time.Hour, future)
	candidate := newRegressionClient(t, lifetime, periodicB, periodicA)

	states := map[string]ProviderQuotaState{
		"quota-a":        {Used: 250, ResetAt: future},
		"quota-b":        {Used: 50, ResetAt: expired},
		"quota-lifetime": {Used: 75, ResetAt: future},
	}
	if err := candidate.RestoreProviderQuotas(states); err != nil {
		t.Fatalf("RestoreProviderQuotas() error = %v", err)
	}

	a := fncoreCHG005RequireState(t, candidate, "quota-a", states["quota-a"])
	if !a.QuotaExceeded || !candidate.findGroup("quota-a").stats.quotaExceeded.Load() {
		t.Fatal("restored usage above the candidate limit did not restore exceeded eligibility")
	}
	b := fncoreCHG005RequireState(t, candidate, "quota-b", states["quota-b"])
	if b.QuotaExceeded {
		t.Fatal("restored usage below the candidate limit is marked exceeded")
	}
	fncoreCHG005RequireState(t, candidate, "quota-lifetime", ProviderQuotaState{Used: 75})

	if alias := candidate.findGroup("quota-a").name; alias == "quota-a" {
		t.Fatal("fixture operational name unexpectedly equals its canonical ID")
	}
}

func TestFNCORECHG005QuotaRestoreValidationIsAllOrNothing(t *testing.T) {
	future := time.Unix(1_900_000_000, 789)
	newCandidate := func(t *testing.T) *Client {
		t.Helper()
		_, first := fncoreCHG005Provider("quota-first", "first.invalid:119", 100, 7, time.Hour, future)
		_, second := fncoreCHG005Provider("quota-second", "second.invalid:119", 100, 8, time.Hour, future)
		return newRegressionClient(t, first, second)
	}

	tests := []struct {
		name   string
		states func(*Client) map[string]ProviderQuotaState
	}{
		{
			name: "unknown canonical ID",
			states: func(*Client) map[string]ProviderQuotaState {
				return map[string]ProviderQuotaState{
					"quota-first": {Used: 90, ResetAt: future},
					"unknown":     {Used: 1, ResetAt: future},
				}
			},
		},
		{
			name: "operational alias",
			states: func(client *Client) map[string]ProviderQuotaState {
				return map[string]ProviderQuotaState{
					"quota-first":                         {Used: 90, ResetAt: future},
					client.findGroup("quota-second").name: {Used: 1, ResetAt: future},
				}
			},
		},
		{
			name: "negative usage",
			states: func(*Client) map[string]ProviderQuotaState {
				return map[string]ProviderQuotaState{
					"quota-first":  {Used: 90, ResetAt: future},
					"quota-second": {Used: -1, ResetAt: future},
				}
			},
		},
		{
			name: "periodic quota without deadline",
			states: func(*Client) map[string]ProviderQuotaState {
				return map[string]ProviderQuotaState{
					"quota-first":  {Used: 90, ResetAt: future},
					"quota-second": {Used: 1},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := newCandidate(t)
			before := fncoreCHG005QuotaStates(candidate.Stats())
			if err := candidate.RestoreProviderQuotas(test.states(candidate)); err == nil {
				t.Fatal("RestoreProviderQuotas() error = nil, want validation failure")
			}
			fncoreCHG005RequireStatesEqual(t, fncoreCHG005QuotaStates(candidate.Stats()), before)
		})
	}

	t.Run("inactive provider", func(t *testing.T) {
		candidate := newCandidate(t)
		before := fncoreCHG005QuotaStates(candidate.Stats())
		inactive := candidate.findGroup("quota-second")
		if _, changed := candidate.deactivateProvider(inactive, true); !changed {
			t.Fatal("fixture provider did not become inactive")
		}
		if err := candidate.RestoreProviderQuotas(map[string]ProviderQuotaState{
			"quota-first":  {Used: 90, ResetAt: future},
			"quota-second": {Used: 1, ResetAt: future},
		}); err == nil {
			t.Fatal("RestoreProviderQuotas() error = nil, want inactive-provider failure")
		}
		got := fncoreCHG005QuotaStates(candidate.Stats())
		if got["quota-first"] != before["quota-first"] {
			t.Fatalf("active provider changed after inactive-provider failure: got %+v, want %+v",
				got["quota-first"], before["quota-first"])
		}
	})
}

func TestFNCORECHG005RestoredQuotaBlocksBeforeFirstWireAttempt(t *testing.T) {
	server, provider := fncoreCHG005Provider("quota-blocked", "blocked.invalid:119", 100, 0, 0, time.Time{})
	candidate := newRegressionClient(t, provider)
	if err := candidate.RestoreProviderQuotas(map[string]ProviderQuotaState{
		"quota-blocked": {Used: 100},
	}); err != nil {
		t.Fatalf("RestoreProviderQuotas() error = %v", err)
	}

	_, err := candidate.BodyTargeted(
		fncoreAdmissionContext(t),
		"blocked@example.invalid",
		TargetedBodyOptions{Provider: "quota-blocked"},
	)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("BodyTargeted() error = %v, want ErrQuotaExceeded", err)
	}
	if got := server.commandCount("BODY"); got != 0 {
		t.Fatalf("restored quota allowed %d BODY commands, want 0", got)
	}
}

func TestFNCORECHG005QuotaHandoverIncludesSettledCompletionCharge(t *testing.T) {
	const initialUsed = int64(100)
	_, oldProvider := fncoreCHG005Provider("quota-shared", "old.invalid:119", 1_000_000, initialUsed, 0, time.Time{})
	old := newRegressionClient(t, oldProvider)
	_, candidateProvider := fncoreCHG005Provider("quota-shared", "candidate.invalid:119", 1_000_000, initialUsed, 0, time.Time{})
	candidate := newRegressionClient(t, candidateProvider)

	if _, err := old.BodyTargeted(
		context.Background(),
		"settled@example.invalid",
		TargetedBodyOptions{Provider: "quota-shared"},
	); err != nil {
		t.Fatalf("old BodyTargeted() error = %v", err)
	}
	settled := fncoreCHG005QuotaStates(old.Stats())
	if settled["quota-shared"].Used <= initialUsed {
		t.Fatalf("settled old quota = %d, want response-completion charge above %d",
			settled["quota-shared"].Used, initialUsed)
	}
	if got := fncoreAdmissionProviderStats(t, candidate, "quota-shared").QuotaUsed; got != initialUsed {
		t.Fatalf("prebuilt candidate quota = %d, want stale construction value %d", got, initialUsed)
	}
	if err := candidate.RestoreProviderQuotas(settled); err != nil {
		t.Fatalf("RestoreProviderQuotas() error = %v", err)
	}
	fncoreCHG005RequireState(t, candidate, "quota-shared", settled["quota-shared"])
}

func TestFNCORECHG005QuotaHandoverIncludesSerializedReset(t *testing.T) {
	initialReset := time.Unix(1_900_000_000, 321)
	_, oldProvider := fncoreCHG005Provider("quota-reset", "old-reset.invalid:119", 100, 75, time.Hour, initialReset)
	old := newRegressionClient(t, oldProvider)
	_, candidateProvider := fncoreCHG005Provider("quota-reset", "candidate-reset.invalid:119", 100, 75, time.Hour, initialReset)
	candidate := newRegressionClient(t, candidateProvider)

	if err := old.ResetProviderQuota("quota-reset"); err != nil {
		t.Fatalf("ResetProviderQuota() error = %v", err)
	}
	settled := fncoreCHG005QuotaStates(old.Stats())
	if settled["quota-reset"].Used != 0 || settled["quota-reset"].ResetAt.Equal(initialReset) {
		t.Fatalf("settled reset state = %+v, want zero usage and a fresh deadline", settled["quota-reset"])
	}
	if err := candidate.RestoreProviderQuotas(settled); err != nil {
		t.Fatalf("RestoreProviderQuotas() error = %v", err)
	}
	fncoreCHG005RequireState(t, candidate, "quota-reset", settled["quota-reset"])
}
