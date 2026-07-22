package nntppool

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
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
		client := f451cClient(t, provider)
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
