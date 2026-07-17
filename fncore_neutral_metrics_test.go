package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

type fncoreMetricSnapshot struct {
	bytes         int64
	quota         int64
	errors        int64
	missing       int64
	ttfb          int64
	speed         uint64
	quotaExceeded bool
	breaker       CircuitBreakerStats
}

func seedFNCORENeutralMetrics(group *providerGroup) fncoreMetricSnapshot {
	group.stats.BytesConsumed.Store(4_096)
	group.stats.quotaBytes = 1 << 30
	group.stats.quotaUsed.Store(2_048)
	group.stats.quotaExceeded.Store(false)
	group.stats.Errors.Store(7)
	group.stats.Missing.Store(5)
	group.stats.ttfbEWMA.Store(int64(137 * time.Millisecond))
	group.stats.speedEWMA.Store(math.Float64bits(987_654.25))
	return fncoreMetricSnapshot{
		bytes:         group.stats.BytesConsumed.Load(),
		quota:         group.stats.quotaUsed.Load(),
		errors:        group.stats.Errors.Load(),
		missing:       group.stats.Missing.Load(),
		ttfb:          group.stats.ttfbEWMA.Load(),
		speed:         group.stats.speedEWMA.Load(),
		quotaExceeded: group.stats.quotaExceeded.Load(),
		breaker:       group.breaker.snapshot(),
	}
}

func requireFNCORENeutralMetrics(t *testing.T, group *providerGroup, before fncoreMetricSnapshot, wantPositiveBytes bool) {
	t.Helper()
	deltaBytes := group.stats.BytesConsumed.Load() - before.bytes
	deltaQuota := group.stats.quotaUsed.Load() - before.quota
	if wantPositiveBytes {
		if deltaBytes <= 0 {
			t.Fatalf("factual bytes delta = %d, want positive consumed-byte evidence", deltaBytes)
		}
	} else if deltaBytes != 0 {
		t.Fatalf("factual bytes delta = %d, want zero without response bytes", deltaBytes)
	}
	if deltaQuota != deltaBytes {
		t.Fatalf("quota delta = %d, consumed-byte delta = %d; factual accounting diverged", deltaQuota, deltaBytes)
	}
	if got := group.stats.Errors.Load(); got != before.errors {
		t.Fatalf("provider Errors = %d, want unchanged %d for neutral outcome", got, before.errors)
	}
	if got := group.stats.Missing.Load(); got != before.missing {
		t.Fatalf("provider Missing = %d, want unchanged %d for neutral outcome", got, before.missing)
	}
	if got := group.stats.ttfbEWMA.Load(); got != before.ttfb {
		t.Fatalf("provider TTFB EWMA = %v, want unchanged %v for neutral outcome", time.Duration(got), time.Duration(before.ttfb))
	}
	if got := group.stats.speedEWMA.Load(); got != before.speed {
		t.Fatalf("provider speed EWMA bits = %x, want unchanged %x for neutral outcome", got, before.speed)
	}
	if got := group.stats.quotaExceeded.Load(); got != before.quotaExceeded {
		t.Fatalf("quota-exceeded state = %v, want unchanged %v", got, before.quotaExceeded)
	}
	if got := group.breaker.snapshot(); got != before.breaker {
		t.Fatalf("breaker after neutral outcome = %+v, want unchanged %+v", got, before.breaker)
	}
	if got := group.stats.PipelineInUse.Load(); got != 0 {
		t.Fatalf("pipeline occupancy after neutral settlement = %d, want zero", got)
	}
}

func TestFNCORESilentResponseCancellationDoesNotMutateProviderHealthMetrics(t *testing.T) {
	requestSeen := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			close(requestSeen)
			_, _ = io.Copy(io.Discard, reader)
		}()
		return client, nil
	}
	provider := Provider{
		ID:          "neutral-silent-cancellation",
		Host:        "neutral-silent-cancellation.invalid:119",
		Factory:     factory,
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	group := fncoreProviderGroup(t, client, provider.ID)
	before := seedFNCORENeutralMetrics(group)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Body(ctx, "silent@example.invalid")
		result <- err
	}()
	select {
	case <-requestSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("silent cancellation request did not reach response service")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("silent response result = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("silent response cancellation did not settle")
	}
	requireFNCORENeutralMetrics(t, group, before, false)
}

func TestFNCORELargeCallerWriterFailureDoesNotMutateProviderHealthMetrics(t *testing.T) {
	payload := bytes.Repeat([]byte("m"), 64*1024)
	primary, provider := breakerProvider(
		"neutral-large-writer",
		"neutral-large-writer.invalid:119",
		func(int, string) []byte {
			return yencSinglePart(payload, "large-writer.bin")
		},
	)
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	group := fncoreProviderGroup(t, client, provider.ID)
	before := seedFNCORENeutralMetrics(group)
	writerErr := errors.New("large caller writer sentinel")
	_, err := client.BodyStream(context.Background(), "large-writer@example.invalid", failingWriter{err: writerErr})
	transportErr := requireFNCORELocalWriterError(t, err, writerErr)
	if len(transportErr.Attempts) != 1 || transportErr.Attempts[0].Outcome != OutcomeKind("local_failure") {
		t.Fatalf("large writer attempts = %+v, want one local failure", transportErr.Attempts)
	}
	if got := primary.commandCount("BODY"); got != 1 {
		t.Fatalf("large writer primary BODY commands = %d, want one", got)
	}
	requireFNCORENeutralMetrics(t, group, before, true)
}
