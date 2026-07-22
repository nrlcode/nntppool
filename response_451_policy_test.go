package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These values are deliberately test-local so this first red commit compiles
// before the public policy type exists. Reflection is also used for the new
// Provider and ArticleHead fields so the failures describe behavior, not an
// incomplete production API.
const (
	f451cPolicyTemporary        uint8 = 0
	f451cPolicyAbsentAfterRetry uint8 = 1
	f451cPolicyInvalid          uint8 = 255
	f451cArticleOperation             = Operation("ARTICLE")
)

func f451cSetProviderPolicy(provider Provider, policy uint8) (Provider, bool) {
	field := reflect.ValueOf(&provider).Elem().FieldByName("Response451Policy")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Uint8 {
		return provider, false
	}
	field.SetUint(uint64(policy))
	return provider, true
}

func f451cProvider(server *regressionProvider, id string, policy uint8) (Provider, bool) {
	provider := server.provider(false)
	provider.ID = id
	return f451cSetProviderPolicy(provider, policy)
}

func f451cClient(t *testing.T, providers ...Provider) *Client {
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

func f451cRequireTransportError(t *testing.T, err error, want OutcomeKind) *TransportError {
	t.Helper()
	if err == nil {
		t.Errorf("error = nil, want %s TransportError", want)
		return nil
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Errorf("error = %v, want TransportError", err)
		return nil
	}
	if transportErr.Kind != want {
		t.Errorf("transport kind = %s, want %s; attempts = %+v", transportErr.Kind, want, transportErr.Attempts)
	}
	return transportErr
}

func TestF451CDefaultOrdinaryArticle451RemainsTemporary(t *testing.T) {
	server := &regressionProvider{
		host: "f451c-default-stat.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("451 temporary article response\r\n")
		},
	}
	provider, _ := f451cProvider(server, "f451c-default-stat", f451cPolicyTemporary)
	client := f451cClient(t, provider)

	_, err := client.Stat(context.Background(), "fixture@example.invalid")
	transportErr := f451cRequireTransportError(t, err, OutcomeTemporaryFailure)
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("default STAT error = %v, must remain temporary", err)
	}
	if got := server.commandCount("STAT"); got != 2 {
		t.Fatalf("default STAT attempts = %d, want existing ordinary retry bound 2", got)
	}
	if transportErr == nil || len(transportErr.Attempts) != 2 {
		t.Fatalf("default STAT evidence = %+v, want two raw attempts", transportErr)
	}
	for index, attempt := range transportErr.Attempts {
		if attempt.Operation != OperationStat || attempt.Outcome != OutcomeTemporaryFailure || attempt.ResponseCode != 451 {
			t.Errorf("default STAT attempt %d = %+v, want temporary raw STAT 451", index, attempt)
		}
	}
}

func TestF451CInvalidPolicyRejectedBeforeFactory(t *testing.T) {
	newInvalid := func(calls *atomic.Int32, id string) Provider {
		provider := Provider{
			ID:          id,
			Connections: 1,
			Factory: func(context.Context) (net.Conn, error) {
				calls.Add(1)
				return nil, errors.New("factory must not run for invalid policy")
			},
		}
		provider, _ = f451cSetProviderPolicy(provider, f451cPolicyInvalid)
		return provider
	}

	t.Run("NewClient", func(t *testing.T) {
		var calls atomic.Int32
		client, err := NewClient(context.Background(), []Provider{newInvalid(&calls, "f451c-invalid-new")})
		if client != nil {
			_ = client.Close()
			t.Error("NewClient() returned a client for policy 255")
		}
		if !errors.Is(err, ErrInvalidProviderConfiguration) {
			t.Errorf("NewClient() error = %v, want ErrInvalidProviderConfiguration", err)
		}
		if got := calls.Load(); got != 0 {
			t.Errorf("NewClient() invoked invalid-policy factory %d times, want 0", got)
		}
	})

	t.Run("AddProvider", func(t *testing.T) {
		baseServer := &regressionProvider{
			host: "f451c-valid-base.invalid:119",
			respond: func(_ int, _ string) []byte {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
		}
		client := f451cClient(t, baseServer.provider(false))
		var calls atomic.Int32
		err := client.AddProvider(newInvalid(&calls, "f451c-invalid-add"))
		if !errors.Is(err, ErrInvalidProviderConfiguration) {
			t.Errorf("AddProvider() error = %v, want ErrInvalidProviderConfiguration", err)
		}
		if got := calls.Load(); got != 0 {
			t.Errorf("AddProvider() invoked invalid-policy factory %d times, want 0", got)
		}
	})
}

func TestF451CMappedPolicyIsArticleScoped(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		prefix    string
		operation Operation
		mapped    bool
	}{
		{name: "mixed_STAT", payload: "sTaT <fixture@example.invalid>\r\n", prefix: "sTaT", operation: OperationStat, mapped: true},
		{name: "mixed_BODY", payload: "BoDy <fixture@example.invalid>\r\n", prefix: "BoDy", operation: OperationBody, mapped: true},
		{name: "mixed_HEAD", payload: "hEaD <fixture@example.invalid>\r\n", prefix: "hEaD", operation: OperationHead, mapped: true},
		{name: "mixed_ARTICLE", payload: "aRtIcLe <fixture@example.invalid>\r\n", prefix: "aRtIcLe", operation: f451cArticleOperation, mapped: true},
		{name: "nonarticle_raw", payload: "gRoUp alt.f451c\r\n", prefix: "gRoUp", operation: OperationUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &regressionProvider{
				host: "f451c-scope-" + test.name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					return []byte("451 provider-mapped article absence\r\n")
				},
			}
			provider, _ := f451cProvider(server, "f451c-scope-"+test.name, f451cPolicyAbsentAfterRetry)
			client := f451cClient(t, provider)
			response := <-client.Send(context.Background(), []byte(test.payload), nil)

			if !test.mapped {
				if response.Err != nil || response.StatusCode != 451 {
					t.Fatalf("raw nonarticle response = code %d, error %v; want original raw 451", response.StatusCode, response.Err)
				}
				if got := server.commandCount(test.prefix); got != 1 {
					t.Fatalf("raw nonarticle attempts = %d, want one-shot", got)
				}
				if len(response.Attempts) != 1 {
					t.Fatalf("raw nonarticle evidence = %+v, want one attempt", response.Attempts)
				}
				attempt := response.Attempts[0]
				if attempt.Operation != test.operation || attempt.Outcome != OutcomeTemporaryFailure || attempt.ResponseCode != 451 {
					t.Fatalf("raw nonarticle attempt = %+v, want temporary raw 451", attempt)
				}
				if errors.Is(responseError(response), ErrArticleNotFound) {
					t.Fatal("raw nonarticle 451 was reclassified as article absence")
				}
				return
			}

			err := responseError(response)
			transportErr := f451cRequireTransportError(t, err, OutcomeHardArticleAbsence)
			if !errors.Is(err, ErrArticleNotFound) {
				t.Errorf("mapped %s error = %v, want ErrArticleNotFound compatibility", test.operation, err)
			}
			if test.operation == f451cArticleOperation {
				var raw *Error
				if !errors.As(err, &raw) || raw.Code != 451 || !strings.Contains(raw.Message, "provider-mapped article absence") {
					t.Errorf("mapped ARTICLE raw error = %#v, want original vendor 451", raw)
				}
			}
			if got := server.commandCount(test.prefix); got != 2 {
				t.Errorf("mapped %s attempts = %d, want original plus fresh retry", test.operation, got)
			}
			if transportErr == nil || len(transportErr.Attempts) != 2 {
				t.Fatalf("mapped %s evidence = %+v, want two raw attempts", test.operation, transportErr)
			}
			for index, attempt := range transportErr.Attempts {
				if attempt.Operation != test.operation || attempt.Outcome != OutcomeHardArticleAbsence || attempt.ResponseCode != 451 {
					t.Errorf("mapped %s attempt %d = %+v, want hard-absence raw 451", test.operation, index, attempt)
				}
				var raw *Error
				if !errors.As(attempt.Cause, &raw) || raw.Code != 451 || !strings.Contains(raw.Message, "provider-mapped article absence") {
					t.Errorf("mapped %s attempt %d cause = %v, want original vendor 451", test.operation, index, attempt.Cause)
				}
			}
		})
	}
}

func TestF451CTargetedSpeedTestPolicyBoundary(t *testing.T) {
	t.Run("default remains one attempt", func(t *testing.T) {
		server := &regressionProvider{
			host: "f451c-speed-default.invalid:119",
			respond: func(_ int, _ string) []byte {
				return []byte("451 temporary speed-test response\r\n")
			},
		}
		provider, _ := f451cProvider(server, "f451c-speed-default", f451cPolicyTemporary)
		client := f451cClient(t, provider)
		result, err := client.SpeedTest(context.Background(), SpeedTestOptions{
			NZBReader:    testNZBReader("default-speed@example.invalid"),
			ProviderName: "f451c-speed-default",
		})
		if err != nil {
			t.Fatalf("default targeted SpeedTest() error = %v", err)
		}
		if result.SegmentsDone != 1 || result.Errors != 1 || result.Missing != 0 {
			t.Errorf("default SpeedTest result = done/errors/missing %d/%d/%d, want 1/1/0", result.SegmentsDone, result.Errors, result.Missing)
		}
		if got := server.commandCount("BODY"); got != 1 {
			t.Errorf("default targeted SpeedTest BODY attempts = %d, want exactly 1", got)
		}
	})

	t.Run("mapped retries fresh and classifies", func(t *testing.T) {
		server := &regressionProvider{
			host: "f451c-speed-mapped.invalid:119",
			respond: func(connection int, command string) []byte {
				if strings.HasPrefix(command, "BODY") && connection == 1 {
					return []byte("451 provider-mapped article absence\r\n")
				}
				return yencSinglePart([]byte("mapped speed-test retry"), "mapped-speed.bin")
			},
		}
		provider, _ := f451cProvider(server, "f451c-speed-mapped", f451cPolicyAbsentAfterRetry)
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		fncoreRecordBreakerCompletions(t, client, provider.ID, 2, circuitBreakerFailure)
		result, err := client.SpeedTest(context.Background(), SpeedTestOptions{
			NZBReader:    testNZBReader("mapped-speed@example.invalid"),
			ProviderName: "f451c-speed-mapped",
		})
		if err != nil {
			t.Fatalf("mapped targeted SpeedTest() error = %v", err)
		}
		if result.SegmentsDone != 1 || result.Errors != 0 || result.Missing != 1 || result.DecodedBytes == 0 {
			t.Errorf("mapped SpeedTest result = done/errors/missing/decoded %d/%d/%d/%d, want 1/0/1/>0",
				result.SegmentsDone, result.Errors, result.Missing, result.DecodedBytes)
		}
		if got := server.commandCount("BODY"); got != 2 {
			t.Errorf("mapped targeted SpeedTest BODY attempts = %d, want original plus fresh retry", got)
		}
		if got := server.connections.Load(); got < 2 {
			t.Errorf("mapped targeted SpeedTest connections = %d, want a fresh retry transport", got)
		}
		breaker := providerBreakerStats(t, client, provider.ID)
		if breaker.State != CircuitBreakerClosed || breaker.QualifyingFailures != 2 {
			t.Errorf("mapped targeted SpeedTest breaker = %+v, want neutral completion with two prior failures", breaker)
		}
	})
}

func TestF451CHeadPublishesMappedRetryEvidence(t *testing.T) {
	server := &regressionProvider{
		host: "f451c-head-evidence.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			return mockNNTPResponse(
				"221 0 <fixture@example.invalid> headers follow",
				"Subject: mapped retry success",
			)
		},
	}
	provider, _ := f451cProvider(server, "f451c-head-evidence", f451cPolicyAbsentAfterRetry)
	client := f451cClient(t, provider)

	head, err := client.Head(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Head() error = %v, want mapped retry success", err)
	}
	if got := head.Headers["Subject"]; len(got) != 1 || got[0] != "mapped retry success" {
		t.Fatalf("Head() Subject = %v, want mapped retry success", got)
	}
	if got := server.commandCount("HEAD"); got != 2 {
		t.Fatalf("Head() attempts = %d, want original plus fresh retry", got)
	}

	value := reflect.ValueOf(head).Elem()
	providerField := value.FieldByName("ProviderID")
	attemptsField := value.FieldByName("Attempts")
	if !providerField.IsValid() || providerField.Kind() != reflect.String ||
		!attemptsField.IsValid() || !attemptsField.CanInterface() {
		t.Fatal("ArticleHead must publish ProviderID and Attempts after mapped retry")
	}
	if got := providerField.String(); got != "f451c-head-evidence" {
		t.Errorf("Head() ProviderID = %q, want f451c-head-evidence", got)
	}
	attempts, ok := attemptsField.Interface().([]AttemptEvidence)
	if !ok {
		t.Fatalf("ArticleHead.Attempts has type %s, want []AttemptEvidence", attemptsField.Type())
	}
	if len(attempts) != 2 ||
		attempts[0].Operation != OperationHead || attempts[0].Outcome != OutcomeHardArticleAbsence || attempts[0].ResponseCode != 451 ||
		attempts[1].Operation != OperationHead || attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("Head() evidence = %+v, want mapped raw 451 then HEAD success", attempts)
	}
}

func TestF451CMappedRetryPreservesDifferentFinalOutcome(t *testing.T) {
	server := &regressionProvider{
		host: "f451c-mixed-retry.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			return []byte("499 different retry outcome\r\n")
		},
	}
	provider, _ := f451cProvider(server, "f451c-mixed-retry", f451cPolicyAbsentAfterRetry)
	client := f451cClient(t, provider)

	_, err := client.Stat(context.Background(), "mixed-retry@example.invalid")
	transportErr := f451cRequireTransportError(t, err, OutcomeInconclusive)
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("mixed retry error = %v, must not collapse to article absence", err)
	}
	if got := server.commandCount("STAT"); got != 2 {
		t.Fatalf("STAT attempts = %d, want mapped response plus fresh retry", got)
	}
	if transportErr == nil || len(transportErr.Attempts) != 2 {
		t.Fatalf("mixed retry evidence = %+v, want two ordered attempts", transportErr)
	}
	first, second := transportErr.Attempts[0], transportErr.Attempts[1]
	if first.Operation != OperationStat || first.Outcome != OutcomeHardArticleAbsence || first.ResponseCode != 451 {
		t.Errorf("first attempt = %+v, want mapped hard-absence STAT 451", first)
	}
	if second.Operation != OperationStat || second.Outcome != OutcomeInconclusive || second.ResponseCode != 499 {
		t.Errorf("second attempt = %+v, want distinct inconclusive STAT 499", second)
	}
}

func TestF451CMappedRetryTransportFailureStopsAtBound(t *testing.T) {
	var factoryCalls atomic.Int32
	dialErr := errors.New("f451c deterministic retry dial failure")
	factory := func(context.Context) (net.Conn, error) {
		if factoryCalls.Add(1) > 1 {
			return nil, dialErr
		}
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 f451c transport server ready\r\n"))
			if _, err := bufio.NewReader(server).ReadString('\n'); err == nil {
				_, _ = server.Write([]byte("451 provider-mapped article absence\r\n"))
			}
		}()
		return client, nil
	}
	provider := Provider{ID: "f451c-transport", Host: "f451c-transport.invalid:119", Factory: factory, Connections: 1, SkipPing: true}
	provider, _ = f451cSetProviderPolicy(provider, f451cPolicyAbsentAfterRetry)
	client := f451cClient(t, provider)

	_, err := client.Stat(context.Background(), "transport@example.invalid")
	transportErr := f451cRequireTransportError(t, err, OutcomeInconclusive)
	if errors.Is(err, ErrArticleNotFound) || !errors.Is(err, dialErr) {
		t.Fatalf("transport retry error = %v, want inconclusive wrapped dial failure", err)
	}
	if factoryCalls.Load() != 2 || transportErr == nil || len(transportErr.Attempts) != 2 ||
		transportErr.Attempts[0].Outcome != OutcomeHardArticleAbsence || transportErr.Attempts[0].ResponseCode != 451 ||
		transportErr.Attempts[1].Outcome != OutcomeTransportFailure {
		t.Fatalf("factory calls/attempts = %d/%+v, want bounded mapped 451 then transport failure", factoryCalls.Load(), transportErr)
	}
}

func TestF451CCancellationAfterMappedRetryDispatch(t *testing.T) {
	retryDispatched := make(chan struct{})
	releaseRetry := make(chan struct{})
	var dispatchOnce sync.Once
	primary := &regressionProvider{
		host: "f451c-cancel-primary.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			dispatchOnce.Do(func() { close(retryDispatched) })
			<-releaseRetry
			return []byte("451 late response after cancellation\r\n")
		},
	}
	backup := &regressionProvider{
		host: "f451c-cancel-backup.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("backup must remain untouched"), "backup.bin")
		},
	}
	primaryProvider, _ := f451cProvider(primary, "f451c-cancel-primary", f451cPolicyAbsentAfterRetry)
	backupProvider := backup.provider(true)
	backupProvider.ID = "f451c-cancel-backup"
	client := f451cClient(t, primaryProvider, backupProvider)
	t.Cleanup(func() { close(releaseRetry) })

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Body(ctx, "cancel-retry@example.invalid")
		result <- err
	}()
	select {
	case <-retryDispatched:
	case err := <-result:
		t.Fatalf("Body() settled before mapped retry dispatch: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Body() did not dispatch the mapped retry")
	}
	cancel()
	var err error
	select {
	case err = <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("Body() did not settle after cancellation")
	}

	transportErr := f451cRequireTransportError(t, err, OutcomeCancellation)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Body() error = %v, want caller cancellation", err)
	}
	if got := primary.commandCount("BODY"); got != 2 {
		t.Errorf("primary BODY attempts = %d, want cancellation after retry dispatch", got)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Errorf("backup BODY attempts = %d, want untouched", got)
	}
	if transportErr == nil || len(transportErr.Attempts) != 2 {
		t.Fatalf("cancellation evidence = %+v, want two ordered attempts", transportErr)
	}
	first, second := transportErr.Attempts[0], transportErr.Attempts[1]
	if first.ProviderID != "f451c-cancel-primary" || first.Operation != OperationBody ||
		first.Outcome != OutcomeHardArticleAbsence || first.ResponseCode != 451 {
		t.Errorf("first attempt = %+v, want mapped hard-absence BODY 451", first)
	}
	if second.ProviderID != "f451c-cancel-primary" || second.Operation != OperationBody ||
		second.Outcome != OutcomeCancellation || !errors.Is(second.Cause, context.Canceled) {
		t.Errorf("second attempt = %+v, want cancellation on dispatched fresh retry", second)
	}
}

func f451cIntegrationProvider(server *regressionProvider, id string, backup bool, policy uint8) Provider {
	provider, _ := f451cProvider(server, id, policy)
	provider.Backup = backup
	return provider
}

func TestF451CQuotaAdmissionPrecedesMappedTargetedSpeedTest(t *testing.T) {
	server, provider := fncoreQuotaExhaustedProvider("f451c-quota", "f451c-quota.invalid:119")
	var configured bool
	provider, configured = f451cSetProviderPolicy(provider, f451cPolicyAbsentAfterRetry)
	if !configured {
		t.Error("quota fixture could not opt in to the mapped 451 policy")
	}
	client := fncoreAdmissionClient(t, provider)
	fncoreRecordBreakerCompletions(t, client, provider.ID, 1, circuitBreakerFailure)
	before := providerBreakerStats(t, client, provider.ID)

	result, err := client.SpeedTest(context.Background(), SpeedTestOptions{
		NZBReader:    testNZBReader("quota@example.invalid"),
		ProviderName: provider.ID,
	})
	if err != nil {
		t.Fatalf("SpeedTest() error = %v", err)
	}
	if result.SegmentsDone != 1 {
		t.Fatalf("SpeedTest segments done = %d, want one settled admission result", result.SegmentsDone)
	}
	if got := server.connections.Load(); got != 0 {
		t.Errorf("quota-exhausted provider connections = %d, want 0", got)
	}
	if got := server.commandCount("BODY"); got != 0 {
		t.Errorf("quota-exhausted provider BODY commands = %d, want 0", got)
	}
	if after := providerBreakerStats(t, client, provider.ID); after != before {
		t.Errorf("quota rejection changed breaker: before=%+v after=%+v", before, after)
	}
}

func TestF451CMappedRetryIsBreakerNeutralAndCountsWireAbsence(t *testing.T) {
	t.Run("repeated mapped absence", func(t *testing.T) {
		server, provider := breakerProvider(
			"f451c-neutral-repeated",
			"f451c-neutral-repeated.invalid:119",
			func(_ int, _ string) []byte { return []byte("451 provider-mapped article absence\r\n") },
		)
		provider, _ = f451cSetProviderPolicy(provider, f451cPolicyAbsentAfterRetry)
		client := newBreakerClient(t, newBreakerFakeClock(), provider)
		fncoreRecordBreakerCompletions(t, client, provider.ID, 2, circuitBreakerFailure)

		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		transportErr := f451cRequireTransportError(t, err, OutcomeHardArticleAbsence)
		if !errors.Is(err, ErrArticleNotFound) || transportErr == nil || len(transportErr.Attempts) != 2 {
			t.Errorf("repeated mapped Stat() = %v, attempts %+v; want conclusive two-attempt absence", err, transportErr)
		}
		breaker := providerBreakerStats(t, client, provider.ID)
		if breaker.State != CircuitBreakerClosed || breaker.QualifyingFailures != 2 {
			t.Errorf("breaker after repeated mapped absence = %+v, want closed with two prior failures", breaker)
		}
		stats := fncoreAdmissionProviderStats(t, client, provider.ID)
		if stats.Missing != 2 || stats.Errors != 0 || server.commandCount("STAT") != 2 {
			t.Errorf("repeated mapped commands/missing/errors = %d/%d/%d, want 2/2/0",
				server.commandCount("STAT"), stats.Missing, stats.Errors)
		}
	})

	t.Run("half-open settlement", func(t *testing.T) {
		clock := newBreakerFakeClock()
		server, provider := breakerProvider(
			"f451c-neutral-half-open",
			"f451c-neutral-half-open.invalid:119",
			func(connection int, _ string) []byte {
				if connection == 1 {
					return []byte("451 provider-mapped article absence\r\n")
				}
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
		)
		provider, _ = f451cSetProviderPolicy(provider, f451cPolicyAbsentAfterRetry)
		client := newBreakerClient(t, clock, provider)
		fncoreRecordBreakerCompletions(t, client, provider.ID, providerBreakerFailureThreshold, circuitBreakerFailure)
		opened := providerBreakerStats(t, client, provider.ID)
		clock.Advance(providerBreakerCooldowns[0])

		if _, err := client.Stat(context.Background(), "fixture@example.invalid"); err != nil {
			t.Fatalf("half-open mapped retry Stat() error = %v", err)
		}
		settled := providerBreakerStats(t, client, provider.ID)
		if settled.State != CircuitBreakerHalfOpen || settled.ProbeInFlight ||
			settled.QualifyingFailures != 0 || settled.Cooldown != opened.Cooldown ||
			!settled.OpenUntil.Equal(opened.OpenUntil) {
			t.Errorf("settled breaker = %+v, want unchanged eligible half-open state from %+v", settled, opened)
		}
		if got := server.commandCount("STAT"); got != 2 {
			t.Errorf("half-open STAT commands = %d, want mapped attempt plus fresh retry", got)
		}
	})
}

func TestF451CExactF006FailureOnlyFallback(t *testing.T) {
	mapped := &regressionProvider{
		host:    "f451c-f006-mapped.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 provider-mapped absence\r\n") },
	}
	temporary := &regressionProvider{
		host:    "f451c-f006-temporary.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 temporary failure\r\n") },
	}
	backup := &regressionProvider{
		host:    "f451c-f006-backup.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
	}
	client := f451cClient(t,
		f451cIntegrationProvider(mapped, "f451c-f006-mapped", false, f451cPolicyAbsentAfterRetry),
		f451cIntegrationProvider(temporary, "f451c-f006-temporary", false, f451cPolicyTemporary),
		f451cIntegrationProvider(backup, "f451c-f006-backup", true, f451cPolicyTemporary),
	)

	result, err := client.Stat(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if result.ProviderID != "f451c-f006-backup" {
		t.Fatalf("serving provider = %q, want failure-only backup", result.ProviderID)
	}
	if gotA, gotB, gotC := mapped.commandCount("STAT"), temporary.commandCount("STAT"), backup.commandCount("STAT"); gotA != 2 || gotB != 2 || gotC != 1 {
		t.Fatalf("mapped/default/backup STAT counts = %d/%d/%d, want 2/2/1", gotA, gotB, gotC)
	}
	wantProviders := []string{
		"f451c-f006-mapped", "f451c-f006-mapped",
		"f451c-f006-temporary", "f451c-f006-temporary",
		"f451c-f006-backup",
	}
	wantOutcomes := []OutcomeKind{
		OutcomeHardArticleAbsence, OutcomeHardArticleAbsence,
		OutcomeTemporaryFailure, OutcomeTemporaryFailure,
		OutcomeSuccess,
	}
	if len(result.Attempts) != len(wantProviders) {
		t.Fatalf("attempts = %+v, want five ordered outcomes", result.Attempts)
	}
	for index, attempt := range result.Attempts {
		if attempt.ProviderID != wantProviders[index] || attempt.Outcome != wantOutcomes[index] {
			t.Errorf("attempt[%d] = %+v, want provider %q outcome %s", index, attempt, wantProviders[index], wantOutcomes[index])
		}
	}
}

func TestF451CNativeAndMappedAbsenceRetryBounds(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		code   int
	}{
		{name: "423", status: "423 no article with that number\r\n", code: 423},
		{name: "430", status: "430 no such article\r\n", code: 430},
	} {
		t.Run(test.name+"_initial", func(t *testing.T) {
			server := &regressionProvider{host: "f451c-native-" + test.name + ".invalid:119", respond: func(_ int, _ string) []byte { return []byte(test.status) }}
			provider, _ := f451cProvider(server, "f451c-native-"+test.name, f451cPolicyAbsentAfterRetry)
			client := f451cClient(t, provider)
			_, err := client.Stat(context.Background(), "native@example.invalid")
			transportErr := f451cRequireTransportError(t, err, OutcomeHardArticleAbsence)
			if !errors.Is(err, ErrArticleNotFound) || server.commandCount("STAT") != 1 ||
				transportErr == nil || len(transportErr.Attempts) != 1 || transportErr.Attempts[0].ResponseCode != test.code {
				t.Errorf("initial %s error/attempts = %v/%+v, commands %d; want one native absence",
					test.name, err, transportErr, server.commandCount("STAT"))
			}
		})

		t.Run("451_then_"+test.name, func(t *testing.T) {
			server := &regressionProvider{
				host: "f451c-mapped-native-" + test.name + ".invalid:119",
				respond: func(connection int, _ string) []byte {
					if connection == 1 {
						return []byte("451 provider-mapped article absence\r\n")
					}
					return []byte(test.status)
				},
			}
			provider, _ := f451cProvider(server, "f451c-mapped-native-"+test.name, f451cPolicyAbsentAfterRetry)
			client := f451cClient(t, provider)
			_, err := client.Stat(context.Background(), "mapped-native@example.invalid")
			transportErr := f451cRequireTransportError(t, err, OutcomeHardArticleAbsence)
			if !errors.Is(err, ErrArticleNotFound) || server.commandCount("STAT") != 2 ||
				transportErr == nil || len(transportErr.Attempts) != 2 ||
				transportErr.Attempts[0].ResponseCode != 451 || transportErr.Attempts[1].ResponseCode != test.code {
				t.Errorf("451 then %s error/attempts = %v/%+v, commands %d; want bounded conclusive absence",
					test.name, err, transportErr, server.commandCount("STAT"))
			}
		})
	}
}

func TestF451CMappedAbsenceEntersParallelStatProbe(t *testing.T) {
	mapped := &regressionProvider{
		host:    "f451c-probe-mapped.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 provider-mapped article absence\r\n") },
	}
	available := &regressionProvider{
		host: "f451c-probe-available.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <probe@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("parallel probe payload"), "probe.bin")
		},
	}
	missing := &regressionProvider{
		host:    "f451c-probe-missing.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	client := newRegressionClient(t,
		f451cIntegrationProvider(mapped, "f451c-probe-mapped", false, f451cPolicyAbsentAfterRetry),
		f451cIntegrationProvider(available, "f451c-probe-available", false, f451cPolicyTemporary),
		f451cIntegrationProvider(missing, "f451c-probe-missing", false, f451cPolicyTemporary),
	)
	body, err := client.Body(context.Background(), "probe@example.invalid")
	if err != nil || body == nil || !bytes.Equal(body.Bytes, []byte("parallel probe payload")) {
		t.Fatalf("parallel probe Body() = body %+v, error %v", body, err)
	}
	if mapped.commandCount("BODY") != 2 || available.commandCount("STAT") != 1 ||
		available.commandCount("BODY") != 1 || missing.commandCount("STAT") != 1 {
		t.Fatalf("mapped BODY/available STAT+BODY/missing STAT = %d/%d+%d/%d, want 2/1+1/1",
			mapped.commandCount("BODY"), available.commandCount("STAT"), available.commandCount("BODY"), missing.commandCount("STAT"))
	}
}

func TestF451CMappedRetryPreservesPartialWriterCommit(t *testing.T) {
	writerErr := errors.New("f451c partial writer sentinel")
	payload := bytes.Repeat([]byte("p"), 32*1024)
	primary := &regressionProvider{
		host: "f451c-writer-primary.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			return yencSinglePart(payload, "partial.bin")
		},
	}
	provider, _ := f451cProvider(primary, "f451c-writer-primary", f451cPolicyAbsentAfterRetry)
	backup := &regressionProvider{
		host: "f451c-writer-backup.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("backup tripwire"), "backup.bin")
		},
	}
	backupProvider := backup.provider(true)
	backupProvider.ID = "f451c-writer-backup"
	client := f451cClient(t, provider, backupProvider)
	writer := &partialErrorWriter{err: writerErr}

	response := <-client.Send(context.Background(), []byte("BODY <partial@example.invalid>\r\n"), writer)
	err := responseError(response)
	if !errors.Is(err, writerErr) {
		t.Fatalf("Send() error = %v, want partial writer cause", err)
	}
	if writer.bytes.Len() == 0 || writer.bytes.Len() >= len(payload) ||
		!bytes.Equal(writer.bytes.Bytes(), payload[:writer.bytes.Len()]) {
		t.Fatalf("writer bytes = %d, want exact nonempty payload prefix below %d", writer.bytes.Len(), len(payload))
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want zero after writer commit", got)
	}
	if got := primary.connections.Load(); got != 2 {
		t.Fatalf("primary connections = %d, want original plus fresh retry", got)
	}
	if got := primary.commandCount("BODY"); got != 2 {
		t.Fatalf("primary BODY commands = %d, want mapped reply plus retry", got)
	}
	if len(response.Attempts) != 2 ||
		response.Attempts[0].ProviderID != provider.ID ||
		response.Attempts[0].Operation != OperationBody ||
		response.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
		response.Attempts[0].ResponseCode != 451 ||
		response.Attempts[1].ProviderID != provider.ID ||
		response.Attempts[1].Operation != OperationBody ||
		response.Attempts[1].Outcome != outcomeLocalFailure ||
		!errors.Is(response.Attempts[1].Cause, writerErr) {
		t.Fatalf("attempts = %+v, want mapped 451 then local writer failure", response.Attempts)
	}
}

func TestF451CMappedRetryDoesNotAbortCommittedCollateralBody(t *testing.T) {
	var collateralConnection atomic.Int32
	var mappedConnection atomic.Int32
	var successConnection atomic.Int32
	var targetAttempts atomic.Int32
	collateralPayload := bytes.Repeat([]byte("c"), 256*1024)
	targetPayload := []byte("fresh mapped retry")
	server := &regressionProvider{
		host: "f451c-collateral.invalid:119",
		respond: func(connection int, command string) []byte {
			if strings.Contains(command, "collateral@example.invalid") {
				collateralConnection.Store(int32(connection))
				return yencSinglePart(collateralPayload, "collateral.bin")
			}
			if targetAttempts.Add(1) == 1 {
				mappedConnection.Store(int32(connection))
				return []byte("451 provider-mapped article absence\r\n")
			}
			successConnection.Store(int32(connection))
			return yencSinglePart(targetPayload, "target.bin")
		},
	}
	provider, _ := f451cProvider(server, "f451c-collateral", f451cPolicyAbsentAfterRetry)
	provider.Connections = 2
	provider.Inflight = 1
	client := f451cClient(t, provider)

	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(releaseWriter)
	type bodyResult struct {
		body *ArticleBody
		err  error
	}
	collateralResult := make(chan bodyResult, 1)
	go func() {
		body, err := client.BodyStream(context.Background(), "collateral@example.invalid", writer)
		collateralResult <- bodyResult{body: body, err: err}
	}()
	select {
	case <-writer.started:
	case <-time.After(3 * time.Second):
		t.Fatal("collateral BODY did not commit to caller writer")
	}

	targetResult := make(chan bodyResult, 1)
	go func() {
		body, err := client.Body(context.Background(), "target@example.invalid")
		targetResult <- bodyResult{body: body, err: err}
	}()
	var target bodyResult
	select {
	case target = <-targetResult:
	case <-time.After(3 * time.Second):
		t.Fatal("mapped retry did not complete while collateral writer was blocked")
	}
	if target.err != nil || target.body == nil || !bytes.Equal(target.body.Bytes, targetPayload) {
		t.Fatalf("mapped retry result = body %+v, error %v", target.body, target.err)
	}
	if len(target.body.Attempts) != 2 ||
		target.body.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
		target.body.Attempts[0].ResponseCode != 451 ||
		target.body.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("mapped retry attempts = %+v, want hard absence then success", target.body.Attempts)
	}
	select {
	case result := <-collateralResult:
		t.Fatalf("collateral BODY settled before writer release: body %+v, error %v", result.body, result.err)
	default:
	}

	collateralConn := collateralConnection.Load()
	mappedConn := mappedConnection.Load()
	successConn := successConnection.Load()
	if collateralConn == 0 || mappedConn == 0 || successConn == 0 ||
		collateralConn == mappedConn || collateralConn == successConn || mappedConn == successConn {
		t.Fatalf("connection ownership = collateral %d, mapped %d, success %d; want three distinct transports",
			collateralConn, mappedConn, successConn)
	}
	if got := server.connections.Load(); got != 3 {
		t.Fatalf("connections = %d, want capacity pair plus one fresh retry", got)
	}

	releaseWriter()
	select {
	case result := <-collateralResult:
		if result.err != nil || result.body == nil || result.body.ProviderID != provider.ID {
			t.Fatalf("collateral BODY result = body %+v, error %v", result.body, result.err)
		}
		if len(result.body.Attempts) != 1 || result.body.Attempts[0].Outcome != OutcomeSuccess {
			t.Fatalf("collateral attempts = %+v, want one successful committed attempt", result.body.Attempts)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("collateral BODY did not settle after writer release")
	}
}
