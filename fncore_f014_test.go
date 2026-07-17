package nntppool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestFNCORECallerOwnedSocketDeadlineNormalizesOnlyTimeoutErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "timeout-shaped read", err: fncoreDeadlineTimeout{}, want: context.DeadlineExceeded},
		{name: "EOF before deadline", err: io.EOF, want: io.EOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := &Request{Ctx: context.Background()}
			got := request.finalize(normalizeRequestReadError(request, readDeadlineCaller, test.err))
			if !errors.Is(got, test.want) {
				t.Fatalf("caller-owned read error %v normalized to %v, want %v", test.err, got, test.want)
			}
			if errors.Is(test.err, io.EOF) && errors.Is(got, context.DeadlineExceeded) {
				t.Fatalf("caller-owned EOF normalized to caller deadline: %v", got)
			}
		})
	}
}

func TestFNCORELiveCallerDeadlineDoesNotHideFlushedSTATTransportFailure(t *testing.T) {
	const callerTimeout = time.Second
	if callerTimeout >= minAttemptTimeout {
		t.Fatalf("caller timeout %v must remain shorter than default provider timeout %v", callerTimeout, minAttemptTimeout)
	}

	commands := make(chan string, 16)
	var servers sync.WaitGroup
	factory := func(context.Context) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		servers.Add(1)
		go func() {
			defer servers.Done()
			defer func() { _ = serverConn.Close() }()
			if _, err := serverConn.Write([]byte("200 FNCORE-F-014 server ready\r\n")); err != nil {
				return
			}
			command, err := bufio.NewReader(serverConn).ReadString('\n')
			if err != nil {
				return
			}
			commands <- command
		}()
		return clientConn, nil
	}

	provider := Provider{
		ID:          "fncore-f014",
		Host:        "fncore-f014.invalid:119",
		Factory:     factory,
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
	}
	client := newBreakerClient(t, newBreakerFakeClock(), provider)
	t.Cleanup(func() {
		_ = client.Close()
		done := make(chan struct{})
		go func() {
			servers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("FNCORE-F-014 fixture servers did not stop")
		}
	})

	for request := 1; request <= providerBreakerFailureThreshold; request++ {
		ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
		t.Cleanup(cancel)
		result := fncoreTargetedStat(ctx, client, provider.ID, "close-after-flush@example.invalid")
		if err := ctx.Err(); err != nil {
			t.Fatalf("outer request %d context settled before transport attribution: %v", request, err)
		}

		var transportErr *TransportError
		if !errors.As(result.Err, &transportErr) ||
			transportErr.Kind != OutcomeTransportFailure ||
			!errors.Is(result.Err, io.EOF) ||
			errors.Is(result.Err, context.DeadlineExceeded) {
			t.Errorf("outer request %d error = %v (transport %+v), want EOF transport failure while caller remains live",
				request, result.Err, transportErr)
		}

		commandCount := 0
	commandsForRequest:
		for {
			select {
			case command := <-commands:
				commandCount++
				if command != "STAT <close-after-flush@example.invalid>\r\n" {
					t.Errorf("outer request %d flushed command = %q, want targeted STAT", request, command)
				}
			default:
				break commandsForRequest
			}
		}
		if commandCount == 0 {
			t.Fatalf("outer request %d completed before the provider observed a flushed STAT", request)
		}

		stats := providerBreakerStats(t, client, provider.ID)
		wantState := CircuitBreakerClosed
		wantFailures := request
		if request == providerBreakerFailureThreshold {
			wantState = CircuitBreakerOpen
			wantFailures = 0
		}
		if stats.State != wantState || stats.QualifyingFailures != wantFailures {
			t.Errorf("outer request %d breaker = %+v, want state %v with %d qualifying failures",
				request, stats, wantState, wantFailures)
		}
		cancel()
	}
}
