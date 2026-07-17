package nntppool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestFNCORECHG003Raw451PreservesSocketAndBreaker(t *testing.T) {
	var calls atomic.Int32
	server, provider := breakerProvider(
		"fncore-raw-451-state",
		"fncore-raw-451-state.invalid:119",
		func(int, string) []byte {
			if calls.Add(1) == 1 {
				return []byte("451 temporary stateful failure\r\n")
			}
			return []byte("211 state preserved\r\n")
		},
	)
	client := newBreakerClient(t, newBreakerFakeClock(), provider)

	first := <-client.Send(context.Background(), []byte("GROUP alt.fncore\r\n"), nil)
	if first.Err != nil || first.StatusCode != 451 {
		t.Fatalf("first raw GROUP = code %d, error %v; want original 451", first.StatusCode, first.Err)
	}
	if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
		t.Fatalf("raw 451 changed breaker state: %+v", stats)
	}

	second := <-client.Send(context.Background(), []byte("GROUP alt.fncore.next\r\n"), nil)
	if second.Err != nil || second.StatusCode != 211 {
		t.Fatalf("second raw GROUP = code %d, error %v; want 211", second.StatusCode, second.Err)
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("raw commands used %d connections, want preserved socket", got)
	}
	if got := server.commandCount("GROUP"); got != 2 {
		t.Fatalf("raw GROUP commands = %d, want two caller commands", got)
	}
	if stats := providerBreakerStats(t, client, provider.ID); stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
		t.Fatalf("raw command sequence changed breaker state: %+v", stats)
	}
}

func TestFNCORECHG003Article451PolicyKeepsArticleEvidence(t *testing.T) {
	server := &regressionProvider{
		host: "fncore-article-451.invalid:119",
		respond: func(connection int, command string) []byte {
			if connection == 1 {
				return []byte("451 retry article on a fresh connection\r\n")
			}
			return []byte("220 0 <article@example.invalid> article\r\nHeader: value\r\n\r\npayload\r\n.\r\n")
		},
	}
	client := fncoreCHG003Client(t, fncoreCHG003Provider(server, false))

	response := <-client.Send(context.Background(), []byte("aRtIcLe <article@example.invalid>\r\n"), nil)
	if response.Err != nil || response.StatusCode != 220 {
		t.Fatalf("raw ARTICLE = code %d, error %v; want fresh-retry success", response.StatusCode, response.Err)
	}
	if got := server.connections.Load(); got != 2 {
		t.Fatalf("ARTICLE connections = %d, want one original and one fresh retry", got)
	}
	if got := server.commandCount("aRtIcLe"); got != 2 {
		t.Fatalf("ARTICLE commands = %d, want original plus one retry", got)
	}
	if len(response.Attempts) != 2 {
		t.Fatalf("ARTICLE attempts = %+v, want two", response.Attempts)
	}
	for i, attempt := range response.Attempts {
		if attempt.Operation != operationArticle || string(attempt.Operation) != "ARTICLE" {
			t.Fatalf("ARTICLE attempt %d operation = %q, want ARTICLE", i, attempt.Operation)
		}
	}
}

func TestFNCORECHG003AttemptEvidenceUsesMonotonicBoundaries(t *testing.T) {
	submitted := time.Now()
	written := submitted.Add(250 * time.Millisecond)
	responseHead := submitted.Add(750 * time.Millisecond)
	completed := submitted.Add(time.Second)
	request := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <timing@example.invalid>\r\n"),
		submittedAt: submitted,
	}
	request.lifecycleMu.Lock()
	request.writtenTime = written
	request.responseHeadTime = responseHead
	request.lifecycleMu.Unlock()

	// Contradictory wall-clock projections prove evidence uses the monotonic
	// boundaries above instead of reconstructing time from Unix nanoseconds.
	request.writtenAt.Store(submitted.Add(900 * time.Millisecond).UnixNano())
	request.responseHeadAt.Store(submitted.Add(100 * time.Millisecond).UnixNano())

	attempt := buildAttemptEvidence(request, "timing-provider", Response{StatusCode: 223}, completed)
	if attempt.PoolQueueDuration != 250*time.Millisecond ||
		attempt.PipelineHeadWaitDuration != 500*time.Millisecond ||
		attempt.ResponseServiceDuration != 250*time.Millisecond {
		t.Fatalf("attempt phases = %v/%v/%v, want 250ms/500ms/250ms",
			attempt.PoolQueueDuration, attempt.PipelineHeadWaitDuration, attempt.ResponseServiceDuration)
	}
}
