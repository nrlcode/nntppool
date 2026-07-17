package nntppool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fncoreCHG002FailingFactory(calls *atomic.Int64) ConnFactory {
	return func(context.Context) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("test dial rejected")
	}
}

func fncoreCHG002StatusFactory(status string) ConnFactory {
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			if _, err := io.WriteString(server, "200 test server ready\r\n"); err != nil {
				return
			}
			if _, err := bufio.NewReader(server).ReadString('\n'); err != nil {
				return
			}
			_, _ = io.WriteString(server, status+"\r\n")
		}()
		return client, nil
	}
}

func TestFNCORECHG002StartupUsesOneOperationalIdentityNamespace(t *testing.T) {
	tests := []struct {
		name      string
		providers []Provider
	}{
		{
			name: "later resolved name collides with earlier ID",
			providers: []Provider{
				{ID: "canonical-a", Host: "transport-a.example:119", Connections: 1},
				{ID: "canonical-b", Host: "canonical-a", Connections: 1},
			},
		},
		{
			name: "later ID collides with earlier resolved name",
			providers: []Provider{
				{ID: "canonical-a", Host: "transport-a.example:119", Connections: 1},
				{ID: "transport-a.example:119", Host: "transport-b.example:119", Connections: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dials atomic.Int64
			for i := range tt.providers {
				tt.providers[i].Factory = fncoreCHG002FailingFactory(&dials)
			}

			client, err := NewClient(context.Background(), tt.providers)
			if client != nil {
				_ = client.Close()
			}
			if err == nil {
				t.Fatal("NewClient() accepted identities owned by different providers")
			}
			if got := dials.Load(); got != 0 {
				t.Fatalf("identity conflict dial count = %d, want 0", got)
			}
		})
	}
}

func TestFNCORECHG002SameOwnerIdentityTokensMayCoalesce(t *testing.T) {
	var dials atomic.Int64
	const token = "same-owner.example:119"
	client, err := NewClient(context.Background(), []Provider{{
		ID:          token,
		Host:        token,
		Factory:     fncoreCHG002FailingFactory(&dials),
		Connections: 1,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatalf("NewClient() rejected one provider whose ID equals its resolved name: %v", err)
	}
	defer func() { _ = client.Close() }()

	if got := client.NumProviders(); got != 1 {
		t.Fatalf("NumProviders() = %d, want 1", got)
	}
}

func TestFNCORECHG002RuntimeRejectsCrossTokenCollisionBeforeDial(t *testing.T) {
	tests := []struct {
		name     string
		provider Provider
	}{
		{
			name: "new resolved name collides with existing ID",
			provider: Provider{
				ID:          "canonical-b",
				Host:        "canonical-a",
				Connections: 1,
			},
		},
		{
			name: "new ID collides with existing resolved name",
			provider: Provider{
				ID:          "transport-a.example:119",
				Host:        "transport-b.example:119",
				Connections: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var baseDials atomic.Int64
			client, err := NewClient(context.Background(), []Provider{{
				ID:          "canonical-a",
				Host:        "transport-a.example:119",
				Factory:     fncoreCHG002FailingFactory(&baseDials),
				Connections: 1,
				SkipPing:    true,
			}})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}
			defer func() { _ = client.Close() }()

			var addedDials atomic.Int64
			tt.provider.Factory = fncoreCHG002FailingFactory(&addedDials)
			if err := client.AddProvider(tt.provider); err == nil {
				t.Fatal("AddProvider() accepted identities owned by different providers")
			}
			if got := addedDials.Load(); got != 0 {
				t.Fatalf("conflicting provider dial count = %d, want 0", got)
			}
			if got := client.NumProviders(); got != 1 {
				t.Fatalf("NumProviders() = %d after rejected add, want 1", got)
			}
		})
	}
}

func TestFNCORECHG002GeneratedIdentitySkipsOccupiedTokensDeterministically(t *testing.T) {
	const occupied = "provider-1"
	generated := make([]string, 0, 2)

	for range 2 {
		var dials atomic.Int64
		client, err := NewClient(context.Background(), []Provider{{
			ID:          occupied,
			Host:        "explicit.example:119",
			Factory:     fncoreCHG002FailingFactory(&dials),
			Connections: 1,
			SkipPing:    true,
		}})
		if err != nil {
			t.Fatalf("NewClient() error = %v", err)
		}

		err = client.AddProvider(Provider{
			Factory:     fncoreCHG002FailingFactory(&dials),
			Connections: 1,
			SkipPing:    true,
		})
		if err != nil {
			_ = client.Close()
			t.Fatalf("AddProvider() did not skip occupied generated token %q: %v", occupied, err)
		}

		stats := client.Stats()
		if len(stats.Providers) != 2 {
			_ = client.Close()
			t.Fatalf("Stats().Providers = %d, want 2", len(stats.Providers))
		}
		generated = append(generated, stats.Providers[1].ProviderID)
		_ = client.Close()
	}

	if generated[0] == occupied {
		t.Fatalf("generated identity reused occupied token %q", occupied)
	}
	if generated[0] == "" || generated[0] != generated[1] {
		t.Fatalf("generated identities = %q and %q, want one stable non-empty token", generated[0], generated[1])
	}
}

func TestFNCORECHG002CanonicalIDTargetsQuotaRemovalAndSpeedTest(t *testing.T) {
	const (
		providerID      = "canonical-speed-provider"
		operationalName = "speed.example:119"
	)
	client, err := NewClient(context.Background(), []Provider{{
		ID:          providerID,
		Host:        operationalName,
		Factory:     makeBodyFactory(t, 222),
		Connections: 1,
		QuotaBytes:  1024,
		QuotaUsed:   512,
	}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	result, err := client.SpeedTest(context.Background(), SpeedTestOptions{
		NZBReader:    testNZBReader("registry@test"),
		ProviderName: providerID,
	})
	if err != nil {
		t.Fatalf("SpeedTest() by Provider.ID error = %v", err)
	}
	if len(result.Providers) != 1 {
		t.Fatalf("SpeedTest().Providers = %d, want 1", len(result.Providers))
	}
	if got := result.Providers[0].ProviderID; got != providerID {
		t.Fatalf("SpeedTest provider ID = %q, want %q", got, providerID)
	}
	if got := result.Providers[0].Name; got != operationalName {
		t.Fatalf("SpeedTest operational name = %q, want %q", got, operationalName)
	}

	if err := client.ResetProviderQuota(providerID); err != nil {
		t.Fatalf("ResetProviderQuota(Provider.ID) error = %v", err)
	}
	if got := client.Stats().Providers[0].QuotaUsed; got != 0 {
		t.Fatalf("quota used after canonical reset = %d, want 0", got)
	}

	if err := client.RemoveProvider(providerID); err != nil {
		t.Fatalf("RemoveProvider(Provider.ID) error = %v", err)
	}
	if got := client.NumProviders(); got != 0 {
		t.Fatalf("NumProviders() after canonical removal = %d, want 0", got)
	}
}

func TestFNCORECHG002ReconnectPreservesGeneratedIdentityAndOrder(t *testing.T) {
	client, err := NewClient(context.Background(), []Provider{
		{
			Factory:        fncoreCHG002StatusFactory("502 service unavailable"),
			Connections:    1,
			SkipPing:       true,
			ReconnectDelay: time.Millisecond,
		},
		{
			ID:          "canonical-second",
			Host:        "second.example:119",
			Factory:     fncoreCHG002StatusFactory("223 1 <registry@test>"),
			Connections: 1,
			SkipPing:    true,
		},
	}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response := <-client.Send(ctx, []byte("STAT <registry@test>\r\n"), io.Discard)
	if response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("fallback response = %+v, want second provider success", response)
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := client.Stats()
		if len(stats.Providers) == 2 {
			got := []string{stats.Providers[0].ProviderID, stats.Providers[1].ProviderID}
			want := []string{"provider-0", "canonical-second"}
			if got[0] != want[0] || got[1] != want[1] {
				t.Fatalf("provider order after reconnect = %q, want %q", got, want)
			}
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("reconnected provider did not return: %+v", stats.Providers)
		case <-ticker.C:
		}
	}
}

func TestFNCORECHG002ConcurrentAddsReserveIdentityBeforeDial(t *testing.T) {
	var baseDials, addedDials atomic.Int64
	client, err := NewClient(context.Background(), []Provider{{
		ID:          "base-id",
		Host:        "base.example:119",
		Factory:     fncoreCHG002FailingFactory(&baseDials),
		Connections: 1,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	start := make(chan struct{})
	defer func() {
		select {
		case <-start:
		default:
			close(start)
		}
	}()
	enteredPing := make(chan struct{}, 2)
	releasePing := make(chan struct{})
	defer func() {
		select {
		case <-releasePing:
		default:
			close(releasePing)
		}
	}()
	pingFactory := func(ctx context.Context) (net.Conn, error) {
		addedDials.Add(1)
		enteredPing <- struct{}{}
		select {
		case <-releasePing:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return fncoreCHG002StatusFactory("111 20260716120000")(ctx)
	}

	results := make(chan error, 2)
	started := make(chan struct{}, 2)
	add := func(host string) {
		started <- struct{}{}
		<-start
		results <- client.AddProvider(Provider{
			ID:          "shared-canonical-id",
			Host:        host,
			Factory:     pingFactory,
			Connections: 1,
		})
	}
	go add("concurrent-a.example:119")
	go add("concurrent-b.example:119")
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("concurrent AddProvider goroutine did not start")
		}
	}
	close(start)

	select {
	case <-enteredPing:
	case <-time.After(time.Second):
		t.Fatal("neither concurrent AddProvider reached its ping")
	}

	// Give the competing call a bounded chance to either observe the reserved
	// identity or expose the exact-parent pre-publication race. A correct
	// implementation may instead serialize the complete mutation.
	observation := time.NewTimer(50 * time.Millisecond)
	var earlyResults []error
	select {
	case <-enteredPing:
	case err := <-results:
		earlyResults = append(earlyResults, err)
	case <-observation.C:
	}
	if !observation.Stop() {
		select {
		case <-observation.C:
		default:
		}
	}
	close(releasePing)

	var succeeded, failed int
	for _, err := range earlyResults {
		if err == nil {
			succeeded++
		} else {
			failed++
		}
	}
	for range 2 - len(earlyResults) {
		select {
		case err := <-results:
			if err == nil {
				succeeded++
			} else {
				failed++
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent AddProvider did not return")
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent AddProvider results = %d success, %d failure; want 1 and 1", succeeded, failed)
	}
	if got := addedDials.Load(); got != 1 {
		t.Fatalf("concurrent same-ID ping dials = %d, want 1", got)
	}
	if got := client.NumProviders(); got != 2 {
		t.Fatalf("NumProviders() = %d, want base plus one added owner", got)
	}
}

func TestFNCORECHG002ExplicitRemovalCancelsPendingReconnect(t *testing.T) {
	const reconnectDelay = 20 * time.Millisecond
	client, err := NewClient(context.Background(), []Provider{
		{
			Factory:        fncoreCHG002StatusFactory("502 service unavailable"),
			Connections:    1,
			SkipPing:       true,
			ReconnectDelay: reconnectDelay,
		},
		{
			ID:          "fallback-id",
			Host:        "fallback.example:119",
			Factory:     fncoreCHG002StatusFactory("223 1 <cancel-reconnect@test>"),
			Connections: 1,
			SkipPing:    true,
		},
	}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response := <-client.Send(ctx, []byte("STAT <cancel-reconnect@test>\r\n"), io.Discard)
	if response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("fallback response = %+v, want second provider success", response)
	}

	if err := client.RemoveProvider("provider-0"); err != nil {
		t.Fatalf("RemoveProvider(pending canonical ID) error = %v", err)
	}

	timer := time.NewTimer(5 * reconnectDelay)
	defer timer.Stop()
	<-timer.C
	stats := client.Stats()
	if len(stats.Providers) != 1 || stats.Providers[0].ProviderID != "fallback-id" {
		t.Fatalf("explicitly removed provider returned after reconnect delay: %+v", stats.Providers)
	}
}

func TestFNCORECHG002IdentityErrorsEscapeControlCharacters(t *testing.T) {
	const providerID = "account\r\n\t\x1bidentity"
	retryAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	transportErr := &TransportError{
		Kind:       OutcomeTransportFailure,
		ProviderID: providerID,
		Cause:      errors.New("test failure"),
	}
	breakerErr := &CircuitBreakerError{
		ProviderID: providerID,
		State:      CircuitBreakerOpen,
		RetryAt:    retryAt,
	}
	tests := []struct {
		name string
		err  error
		id   func() string
	}{
		{
			name: "transport error",
			err:  transportErr,
			id:   func() string { return transportErr.ProviderID },
		},
		{
			name: "circuit breaker error",
			err:  breakerErr,
			id:   func() string { return breakerErr.ProviderID },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text := tt.err.Error()
			if strings.ContainsAny(text, "\r\n\t\x1b") {
				t.Fatalf("Error() exposed raw control characters: %q", text)
			}
			for _, escaped := range []string{`\r`, `\n`, `\t`, `\x1b`} {
				if !strings.Contains(text, escaped) {
					t.Fatalf("Error() = %q, want visible escape %q", text, escaped)
				}
			}
			if got := tt.id(); got != providerID {
				t.Fatalf("structured ProviderID = %q, want unchanged %q", got, providerID)
			}
		})
	}
}

func TestFNCORECHG002RemovedLifecycleCannotReconnectReusedID(t *testing.T) {
	const (
		providerID              = "reused-id"
		oldReconnectDelay       = 500 * time.Millisecond
		replacementDelay        = 5 * time.Second
		earlyObservationWindow  = 2 * time.Second
		earlyObservationPolling = 5 * time.Millisecond
	)

	client, err := NewClient(context.Background(), []Provider{
		{
			ID:             providerID,
			Host:           "old-lifecycle.example:119",
			Factory:        fncoreCHG002StatusFactory("502 service unavailable"),
			Connections:    1,
			SkipPing:       true,
			ReconnectDelay: oldReconnectDelay,
		},
		{
			ID:          "stable-fallback-id",
			Host:        "stable-fallback.example:119",
			Factory:     fncoreCHG002StatusFactory("223 1 <stale-reconnect@test>"),
			Connections: 1,
			SkipPing:    true,
			Backup:      true,
		},
	}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*earlyObservationWindow)
	defer cancel()
	requireFallback := func(stage string) {
		t.Helper()
		response := <-client.Send(ctx, []byte("STAT <stale-reconnect@test>\r\n"), io.Discard)
		if response.Err != nil || response.StatusCode != 223 {
			t.Fatalf("%s response = %+v, want fallback success", stage, response)
		}
		if got := client.NumProviders(); got != 1 {
			t.Fatalf("NumProviders() after %s 502 = %d, want stable fallback only", stage, got)
		}
	}

	requireFallback("old lifecycle")
	if err := client.RemoveProvider(providerID); err != nil {
		t.Fatalf("RemoveProvider(%q) error = %v", providerID, err)
	}

	var replacementDials atomic.Int64
	earlyReconnect := make(chan int64, 1)
	replacementFactory := func(context.Context) (net.Conn, error) {
		call := replacementDials.Add(1)
		if call >= 3 {
			select {
			case earlyReconnect <- call:
			default:
			}
		}

		clientConn, serverConn := net.Pipe()
		go func() {
			defer func() { _ = serverConn.Close() }()
			if _, err := io.WriteString(serverConn, "200 test server ready\r\n"); err != nil {
				return
			}
			command, err := bufio.NewReader(serverConn).ReadString('\n')
			if err != nil {
				return
			}
			status := "502 service unavailable"
			if strings.HasPrefix(command, "DATE") {
				status = "111 20260716120000"
			}
			_, _ = io.WriteString(serverConn, status+"\r\n")
		}()
		return clientConn, nil
	}

	err = client.AddProvider(Provider{
		ID:             providerID,
		Host:           "replacement-lifecycle.example:119",
		Factory:        replacementFactory,
		Connections:    1,
		ReconnectDelay: replacementDelay,
	})
	if err != nil {
		t.Fatalf("AddProvider(replacement %q) error = %v", providerID, err)
	}
	if got := replacementDials.Load(); got != 1 {
		t.Fatalf("replacement startup dial count = %d, want 1", got)
	}

	requireFallback("replacement lifecycle")
	if got := replacementDials.Load(); got != 2 {
		t.Fatalf("replacement dial count after its 502 = %d, want startup ping plus command", got)
	}

	window := time.NewTimer(earlyObservationWindow)
	defer window.Stop()
	poll := time.NewTicker(earlyObservationPolling)
	defer poll.Stop()
	var earlyDial int64
	for {
		select {
		case earlyDial = <-earlyReconnect:
		case <-poll.C:
			if got := client.NumProviders(); got != 1 {
				t.Fatalf(
					"NumProviders() before replacement reconnect delay = %d, want stable fallback only (early replacement dial %d)",
					got,
					earlyDial,
				)
			}
		case <-window.C:
			if got := replacementDials.Load(); got != 2 {
				t.Fatalf("replacement dial count before its reconnect delay = %d, want 2", got)
			}
			if got := client.NumProviders(); got != 1 {
				t.Fatalf("NumProviders() before replacement reconnect delay = %d, want stable fallback only", got)
			}
			return
		case <-ctx.Done():
			t.Fatalf("timed out while observing stale reconnect window: %v", ctx.Err())
		}
	}
}
