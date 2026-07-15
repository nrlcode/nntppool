package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func response451Provider(server *regressionProvider, id string, backup bool, policy Response451Policy) Provider {
	provider := server.provider(backup)
	provider.ID = id
	provider.Response451Policy = policy
	return provider
}

func newResponse451Client(t *testing.T, providers ...Provider) *Client {
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

func require451TransportError(t *testing.T, err error, want OutcomeKind) *TransportError {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s TransportError", want)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("error = %v, want TransportError", err)
	}
	if transportErr.Kind != want {
		t.Fatalf("transport kind = %s, want %s; attempts = %+v", transportErr.Kind, want, transportErr.Attempts)
	}
	return transportErr
}

func Test451AZeroAndExplicitTemporaryPolicyPreserveV4Behavior(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy Response451Policy
	}{
		{name: "omitted_zero_value"},
		{name: "explicit_temporary", policy: Response451Temporary},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &regressionProvider{
				host: "temporary-" + test.name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					return []byte("451 temporary article response\r\n")
				},
			}
			client := newResponse451Client(t, response451Provider(server, "temporary", false, test.policy))

			_, err := client.Stat(context.Background(), "fixture@example.invalid")
			transportErr := require451TransportError(t, err, OutcomeTemporaryFailure)
			if errors.Is(err, ErrArticleNotFound) {
				t.Fatalf("error = %v, default 451 policy must remain temporary", err)
			}
			if server.connections.Load() != 2 || server.commandCount("STAT") != 2 {
				t.Fatalf("connections/STAT attempts = %d/%d, want one existing fresh retry (2/2)", server.connections.Load(), server.commandCount("STAT"))
			}
			if len(transportErr.Attempts) != 2 {
				t.Fatalf("attempts = %+v, want two", transportErr.Attempts)
			}
			for _, attempt := range transportErr.Attempts {
				if attempt.Outcome != OutcomeTemporaryFailure || attempt.ResponseCode != 451 || attempt.Operation != OperationStat {
					t.Errorf("attempt = %+v, want temporary raw STAT 451", attempt)
				}
			}
		})
	}
}

func Test451AInvalidPolicyIsConfigurationError(t *testing.T) {
	var factoryCalls atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		factoryCalls.Add(1)
		return nil, errors.New("factory must not run")
	}
	invalid := Provider{
		ID:                "invalid-policy",
		Factory:           factory,
		Connections:       1,
		SkipPing:          true,
		Response451Policy: Response451Policy(255),
	}

	client, err := NewClient(context.Background(), []Provider{invalid})
	if client != nil {
		_ = client.Close()
		t.Fatal("NewClient() returned a client for an invalid 451 policy")
	}
	if !errors.Is(err, ErrInvalidProviderConfiguration) {
		t.Fatalf("NewClient() error = %v, want ErrInvalidProviderConfiguration", err)
	}
	if factoryCalls.Load() != 0 {
		t.Fatalf("invalid NewClient policy invoked factory %d times", factoryCalls.Load())
	}

	validServer := &regressionProvider{
		host: "valid-policy.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("223 1 <fixture@example.invalid> exists\r\n")
		},
	}
	validClient := newResponse451Client(t, validServer.provider(false))
	if err := validClient.AddProvider(invalid); !errors.Is(err, ErrInvalidProviderConfiguration) {
		t.Fatalf("AddProvider() error = %v, want ErrInvalidProviderConfiguration", err)
	}
	if factoryCalls.Load() != 0 {
		t.Fatalf("invalid AddProvider policy invoked factory %d times", factoryCalls.Load())
	}
}

func Test451APolicyIsArticleOperationScoped(t *testing.T) {
	for _, test := range []struct {
		name      string
		payload   string
		operation Operation
		mapped    bool
	}{
		{name: "stat", payload: "STAT <fixture@example.invalid>\r\n", operation: OperationStat, mapped: true},
		{name: "stat_current_article", payload: "STAT\r\n", operation: OperationStat, mapped: true},
		{name: "body", payload: "BODY <fixture@example.invalid>\r\n", operation: OperationBody, mapped: true},
		{name: "body_mixed_case", payload: "BoDy <fixture@example.invalid>\r\n", operation: OperationBody, mapped: true},
		{name: "head", payload: "HEAD <fixture@example.invalid>\r\n", operation: OperationHead, mapped: true},
		{name: "head_current_article", payload: "HEAD\r\n", operation: OperationHead, mapped: true},
		{name: "article", payload: "ARTICLE <fixture@example.invalid>\r\n", operation: OperationArticle, mapped: true},
		{name: "article_lowercase_current", payload: "article\r\n", operation: OperationArticle, mapped: true},
		{name: "post", payload: "POST\r\n", operation: OperationPost},
		{name: "capabilities", payload: "CAPABILITIES\r\n", operation: OperationUnknown},
		{name: "date", payload: "DATE\r\n", operation: OperationUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &regressionProvider{
				host: "scope-" + test.name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					return []byte("451 vendor-specific response\r\n")
				},
			}
			client := newResponse451Client(t, response451Provider(server, "scope-"+test.name, false, Response451AbsentAfterRetry))
			response := <-client.Send(context.Background(), []byte(test.payload), nil)
			err := responseError(response)

			wantKind := OutcomeTemporaryFailure
			if test.mapped {
				wantKind = OutcomeHardArticleAbsence
			}
			transportErr := require451TransportError(t, err, wantKind)
			if got := errors.Is(err, ErrArticleNotFound); got != test.mapped {
				t.Fatalf("errors.Is(ErrArticleNotFound) = %v, want %v; error = %v", got, test.mapped, err)
			}
			if len(transportErr.Attempts) != 2 {
				t.Fatalf("attempts = %+v, want two", transportErr.Attempts)
			}
			for _, attempt := range transportErr.Attempts {
				if attempt.Operation != test.operation || attempt.Outcome != wantKind || attempt.ResponseCode != 451 {
					t.Errorf("attempt = %+v, want operation=%s outcome=%s raw 451", attempt, test.operation, wantKind)
				}
			}
		})
	}
}

func Test451ATargetedSpeedTestPathUsesMappedRetry(t *testing.T) {
	server := &regressionProvider{
		host: "mapped-speedtest.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 mapped article absence\r\n")
			}
			return yencSinglePart([]byte("speedtest retry payload"), "speedtest.bin")
		},
	}
	client := newResponse451Client(t, response451Provider(server, "mapped-speedtest", false, Response451AbsentAfterRetry))
	group := client.findGroup("mapped-speedtest")
	if group == nil {
		t.Fatal("mapped speed-test provider not found")
	}

	response := <-client.sendToGroup(
		context.Background(),
		group,
		[]byte("BODY <fixture@example.invalid>\r\n"),
		io.Discard,
	)
	if err := responseError(response); err != nil {
		t.Fatalf("targeted speed-test BODY error = %v", err)
	}
	if response.StatusCode != 222 || response.ProviderID != "mapped-speedtest" {
		t.Fatalf("response = %+v, want successful mapped-speedtest BODY", response)
	}
	if len(response.Attempts) != 2 ||
		response.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
		response.Attempts[0].ResponseCode != 451 ||
		response.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("attempts = %+v, want mapped 451 then successful targeted BODY", response.Attempts)
	}
	if server.connections.Load() != 2 || server.commandCount("BODY") != 2 {
		t.Fatalf("connections/BODY attempts = %d/%d, want fresh retry 2/2", server.connections.Load(), server.commandCount("BODY"))
	}
}

func Test451AHeadSuccessRetainsMappedRetryEvidence(t *testing.T) {
	server := &regressionProvider{
		host: "mapped-head-success.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 mapped article absence\r\n")
			}
			return mockNNTPResponse(
				"221 0 <fixture@example.invalid> headers follow",
				"Subject: mapped retry success",
			)
		},
	}
	client := newResponse451Client(t, response451Provider(server, "mapped-head-success", false, Response451AbsentAfterRetry))

	head, err := client.Head(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if head.ProviderID != "mapped-head-success" {
		t.Fatalf("Head() provider = %q, want mapped-head-success", head.ProviderID)
	}
	if len(head.Attempts) != 2 ||
		head.Attempts[0].Operation != OperationHead ||
		head.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
		head.Attempts[0].ResponseCode != 451 ||
		head.Attempts[1].Operation != OperationHead ||
		head.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("Head() attempts = %+v, want mapped 451 then success", head.Attempts)
	}
	if got := head.Headers["Subject"]; len(got) != 1 || got[0] != "mapped retry success" {
		t.Fatalf("Head() Subject = %v", got)
	}
}

type response451FailingWriter struct {
	err   error
	bytes atomic.Int64
}

func (w *response451FailingWriter) Write(payload []byte) (int, error) {
	w.bytes.Add(int64(len(payload)))
	return 0, w.err
}

func Test451AArticleWriterCommitStopsFallbackForEveryCommandForm(t *testing.T) {
	for _, test := range []struct {
		name          string
		payload       string
		commandPrefix string
		responseCode  string
	}{
		{name: "mixed_case_body", payload: "BoDy <fixture@example.invalid>\r\n", commandPrefix: "BoDy", responseCode: "222"},
		{name: "article", payload: "ARTICLE <fixture@example.invalid>\r\n", commandPrefix: "ARTICLE", responseCode: "220"},
	} {
		t.Run(test.name, func(t *testing.T) {
			committedErr := errors.New("deterministic committed writer failure")
			writer := &response451FailingWriter{err: committedErr}
			primary := &regressionProvider{
				host: "writer-commit-primary-" + test.name + ".invalid:119",
				respond: func(connection int, _ string) []byte {
					if connection == 1 {
						return []byte("451 mapped article absence\r\n")
					}
					response := yencSinglePart([]byte("committed writer payload"), "committed.bin")
					copy(response[:3], test.responseCode)
					return response
				},
			}
			fallback := &regressionProvider{
				host: "writer-commit-fallback-" + test.name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					response := yencSinglePart([]byte("forbidden fallback payload"), "fallback.bin")
					copy(response[:3], test.responseCode)
					return response
				},
			}
			client := newResponse451Client(t,
				response451Provider(primary, "writer-commit-primary", false, Response451AbsentAfterRetry),
				response451Provider(fallback, "writer-commit-fallback", false, Response451Temporary),
			)

			response := <-client.Send(context.Background(), []byte(test.payload), writer)
			err := responseError(response)
			if !errors.Is(err, committedErr) {
				t.Fatalf("Send() error = %v, want committed writer failure", err)
			}
			if writer.bytes.Load() == 0 {
				t.Fatal("writer received no decoded bytes before its committed failure")
			}
			if fallback.commandCount(test.commandPrefix) != 0 {
				t.Fatalf("fallback commands = %d, committed writer must stop cross-provider restart", fallback.commandCount(test.commandPrefix))
			}
			if len(response.Attempts) != 2 ||
				response.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
				response.Attempts[0].ResponseCode != 451 ||
				response.Attempts[1].Operation == OperationUnknown {
				t.Fatalf("attempts = %+v, want mapped 451 then committed article-operation failure", response.Attempts)
			}
		})
	}
}

func Test451AMappedClassificationPreservesNonNilErrorPrecedence(t *testing.T) {
	req := &Request{
		Ctx:               context.Background(),
		Payload:           []byte("STAT <fixture@example.invalid>\r\n"),
		response451Policy: Response451AbsentAfterRetry,
		submittedAt:       time.Now(),
	}
	attempt := buildAttemptEvidence(req, "mapped-error-precedence", Response{
		StatusCode: 451,
		Status:     "451 mapped article absence",
		Err:        context.Canceled,
	}, time.Now())
	if attempt.Outcome != OutcomeCancellation || !errors.Is(attempt.Cause, context.Canceled) {
		t.Fatalf("attempt = %+v, non-nil cancellation must take precedence over mapped status", attempt)
	}
}

func Test451AMappedRetryRejectsEveryPreexistingHotTransport(t *testing.T) {
	var connections atomic.Int32
	var bodyAttempts atomic.Int32
	warmSeen := make(chan int, 3)
	warmRelease := make(chan struct{})
	bodyConnections := make(chan int, 2)
	valid := yencSinglePart([]byte("fresh mapped retry"), "fresh.bin")

	factory := func(context.Context) (net.Conn, error) {
		connection := int(connections.Add(1))
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 mapped retry server ready\r\n"))
			reader := bufio.NewReader(server)
			for {
				command, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				switch {
				case strings.HasPrefix(command, "STAT"):
					warmSeen <- connection
					<-warmRelease
					_, _ = server.Write([]byte("223 1 <warm@example.invalid> exists\r\n"))
				case strings.HasPrefix(command, "BODY"):
					bodyConnections <- connection
					if bodyAttempts.Add(1) == 1 {
						_, _ = server.Write([]byte("451 mapped article absence\r\n"))
					} else {
						_, _ = server.Write(valid)
					}
				}
			}
		}()
		return client, nil
	}
	client := newResponse451Client(t, Provider{
		ID:                "multi-hot-mapped",
		Host:              "multi-hot-mapped.invalid:119",
		Factory:           factory,
		Connections:       2,
		Inflight:          1,
		StatInflight:      1,
		SkipPing:          true,
		Response451Policy: Response451AbsentAfterRetry,
	})

	warmResults := make(chan error, 3)
	for index := range 3 {
		go func(index int) {
			_, err := client.Stat(context.Background(), fmt.Sprintf("warm-%d@example.invalid", index))
			warmResults <- err
		}(index)
	}
	warmConnections := make(map[int]struct{}, 2)
	for len(warmConnections) < 2 {
		select {
		case connection := <-warmSeen:
			warmConnections[connection] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("did not establish two preexisting hot transports")
		}
	}
	close(warmRelease)
	for range 3 {
		if err := <-warmResults; err != nil {
			t.Fatalf("warming STAT error = %v", err)
		}
	}

	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("fresh mapped retry")) {
		t.Fatalf("Body() bytes = %q", body.Bytes)
	}
	firstConnection := <-bodyConnections
	secondConnection := <-bodyConnections
	if _, existed := warmConnections[firstConnection]; !existed {
		t.Fatalf("first 451 connection = %d, want a preexisting hot transport from %v", firstConnection, warmConnections)
	}
	if _, existed := warmConnections[secondConnection]; existed {
		t.Fatalf("451 retry connection = %d, want newer than all preexisting transports %v", secondConnection, warmConnections)
	}
	if connections.Load() < 3 {
		t.Fatalf("created transports = %d, want at least one new transport after 451", connections.Load())
	}
	if len(body.Attempts) != 2 || body.Attempts[0].Outcome != OutcomeHardArticleAbsence || body.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("attempts = %+v, want mapped hard absence then success", body.Attempts)
	}
}

func Test451AMappedRetryConclusions(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := &regressionProvider{
			host: "mapped-success.invalid:119",
			respond: func(connection int, _ string) []byte {
				if connection == 1 {
					return []byte("451 mapped article absence\r\n")
				}
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
		}
		client := newResponse451Client(t, response451Provider(server, "mapped-success", false, Response451AbsentAfterRetry))
		result, err := client.Stat(context.Background(), "fixture@example.invalid")
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		if len(result.Attempts) != 2 || result.Attempts[0].Outcome != OutcomeHardArticleAbsence || result.Attempts[1].Outcome != OutcomeSuccess {
			t.Fatalf("attempts = %+v, want hard absence then success", result.Attempts)
		}
	})

	for _, test := range []struct {
		name        string
		retryStatus string
	}{
		{name: "second_451", retryStatus: "451 mapped article absence\r\n"},
		{name: "retry_423", retryStatus: "423 no article with that number\r\n"},
		{name: "retry_430", retryStatus: "430 no such article\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &regressionProvider{
				host: "mapped-" + test.name + ".invalid:119",
				respond: func(connection int, _ string) []byte {
					if connection == 1 {
						return []byte("451 mapped article absence\r\n")
					}
					return []byte(test.retryStatus)
				},
			}
			client := newResponse451Client(t, response451Provider(server, "mapped-"+test.name, false, Response451AbsentAfterRetry))
			_, err := client.Stat(context.Background(), "fixture@example.invalid")
			transportErr := require451TransportError(t, err, OutcomeHardArticleAbsence)
			if !errors.Is(err, ErrArticleNotFound) {
				t.Fatalf("error = %v, want ErrArticleNotFound compatibility", err)
			}
			if len(transportErr.Attempts) != 2 || transportErr.Attempts[0].ResponseCode != 451 {
				t.Fatalf("attempts = %+v, want raw mapped 451 then conclusive retry", transportErr.Attempts)
			}
			for _, attempt := range transportErr.Attempts {
				if attempt.Outcome != OutcomeHardArticleAbsence {
					t.Fatalf("attempt = %+v, want hard article absence", attempt)
				}
			}
		})
	}
}

func response451ProviderStats(t *testing.T, client *Client, providerID string) ProviderStats {
	t.Helper()
	for _, stats := range client.Stats().Providers {
		if stats.ProviderID == providerID {
			return stats
		}
	}
	t.Fatalf("provider %q missing from Stats()", providerID)
	return ProviderStats{}
}

func Test451AProviderStatsCountWireResponses(t *testing.T) {
	for _, test := range []struct {
		name        string
		policy      Response451Policy
		respond     func(int, string) []byte
		wantMissing int64
		wantErrors  int64
		wantErr     bool
	}{
		{
			name:   "mapped_then_success",
			policy: Response451AbsentAfterRetry,
			respond: func(connection int, _ string) []byte {
				if connection == 1 {
					return []byte("451 mapped article absence\r\n")
				}
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
			wantMissing: 1,
		},
		{
			name:        "mapped_then_mapped",
			policy:      Response451AbsentAfterRetry,
			respond:     func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
			wantMissing: 2,
			wantErr:     true,
		},
		{
			name:       "temporary_remains_error",
			policy:     Response451Temporary,
			respond:    func(_ int, _ string) []byte { return []byte("451 temporary article response\r\n") },
			wantErrors: 2,
			wantErr:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &regressionProvider{host: "stats-" + test.name + ".invalid:119", respond: test.respond}
			providerID := "stats-" + test.name
			client := newResponse451Client(t, response451Provider(server, providerID, false, test.policy))
			_, err := client.Stat(context.Background(), "fixture@example.invalid")
			if (err != nil) != test.wantErr {
				t.Fatalf("Stat() error = %v, wantErr %v", err, test.wantErr)
			}
			stats := response451ProviderStats(t, client, providerID)
			if stats.Missing != test.wantMissing || stats.Errors != test.wantErrors {
				t.Fatalf("provider stats missing/errors = %d/%d, want %d/%d", stats.Missing, stats.Errors, test.wantMissing, test.wantErrors)
			}
		})
	}
}

func Test451AMapped451ThenResponseTimeoutIsInconclusive(t *testing.T) {
	var connections atomic.Int32
	factory := func(ctx context.Context) (net.Conn, error) {
		connection := connections.Add(1)
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 timeout regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			if connection == 1 {
				_, _ = server.Write([]byte("451 mapped article absence\r\n"))
				return
			}
			<-ctx.Done()
		}()
		return client, nil
	}
	client := newResponse451Client(t, Provider{
		ID:                "mapped-timeout",
		Host:              "mapped-timeout.invalid:119",
		Factory:           factory,
		Connections:       1,
		SkipPing:          true,
		AttemptTimeout:    30 * time.Millisecond,
		Response451Policy: Response451AbsentAfterRetry,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Stat(ctx, "fixture@example.invalid")
	transportErr := require451TransportError(t, err, OutcomeInconclusive)
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("error = %v, mapped 451 plus timeout must not be absence", err)
	}
	if len(transportErr.Attempts) != 2 {
		t.Fatalf("attempts = %+v, want mapped 451 then timeout", transportErr.Attempts)
	}
	if first, second := transportErr.Attempts[0], transportErr.Attempts[1]; first.Outcome != OutcomeHardArticleAbsence || first.ResponseCode != 451 ||
		second.Outcome != OutcomeTransportFailure || !second.ProviderResponseTimeout {
		t.Fatalf("attempts = %+v, want hard raw 451 then provider response timeout", transportErr.Attempts)
	}
}

func Test451AMapped451ThenTransportOrUnmappedResponseIsInconclusive(t *testing.T) {
	t.Run("transport_failure", func(t *testing.T) {
		var factoryCalls atomic.Int32
		dialErr := errors.New("deterministic retry dial failure")
		factory := func(context.Context) (net.Conn, error) {
			if factoryCalls.Add(1) > 1 {
				return nil, dialErr
			}
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 transport regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				_, _ = server.Write([]byte("451 mapped article absence\r\n"))
			}()
			return client, nil
		}
		client := newResponse451Client(t, Provider{
			ID:                "mapped-transport",
			Host:              "mapped-transport.invalid:119",
			Factory:           factory,
			Connections:       1,
			SkipPing:          true,
			Response451Policy: Response451AbsentAfterRetry,
		})
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		transportErr := require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) || !errors.Is(err, dialErr) {
			t.Fatalf("error = %v, want inconclusive wrapped transport failure", err)
		}
		if len(transportErr.Attempts) != 2 ||
			transportErr.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
			transportErr.Attempts[1].Outcome != OutcomeTransportFailure {
			t.Fatalf("attempts = %+v, want mapped 451 then transport failure", transportErr.Attempts)
		}
	})

	t.Run("connection_death_ends_the_required_retry", func(t *testing.T) {
		var factoryCalls atomic.Int32
		factory := func(context.Context) (net.Conn, error) {
			connection := factoryCalls.Add(1)
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 connection-death regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				switch connection {
				case 1:
					_, _ = server.Write([]byte("451 mapped article absence\r\n"))
				case 2:
					// The required fresh retry loses its transport before a
					// response. A third same-provider attempt would exceed the
					// accepted one-retry boundary.
					return
				default:
					_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
				}
			}()
			return client, nil
		}
		client := newResponse451Client(t, Provider{
			ID:                "mapped-connection-death",
			Host:              "mapped-connection-death.invalid:119",
			Factory:           factory,
			Connections:       1,
			SkipPing:          true,
			Response451Policy: Response451AbsentAfterRetry,
		})
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		transportErr := require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("error = %v, mapped 451 plus connection death must not be absence", err)
		}
		if factoryCalls.Load() != 2 {
			t.Fatalf("factory calls = %d, want exactly the initial attempt and required retry", factoryCalls.Load())
		}
		if len(transportErr.Attempts) != 2 ||
			transportErr.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
			transportErr.Attempts[1].Outcome != OutcomeTransportFailure {
			t.Fatalf("attempts = %+v, want mapped 451 then connection death", transportErr.Attempts)
		}
	})

	t.Run("provider_unavailable", func(t *testing.T) {
		var factoryCalls atomic.Int32
		factory := func(context.Context) (net.Conn, error) {
			if factoryCalls.Add(1) > 1 {
				return nil, ErrAuthRejected
			}
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 unavailable regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				_, _ = server.Write([]byte("451 mapped article absence\r\n"))
			}()
			return client, nil
		}
		client := newResponse451Client(t, Provider{
			ID:                "mapped-unavailable",
			Host:              "mapped-unavailable.invalid:119",
			Factory:           factory,
			Connections:       1,
			SkipPing:          true,
			Response451Policy: Response451AbsentAfterRetry,
		})
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		transportErr := require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) || !errors.Is(err, ErrAuthRejected) {
			t.Fatalf("error = %v, want inconclusive wrapped provider-unavailable cause", err)
		}
		if len(transportErr.Attempts) != 2 ||
			transportErr.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
			transportErr.Attempts[1].Outcome != OutcomeProviderUnavailable {
			t.Fatalf("attempts = %+v, want mapped 451 then provider unavailable", transportErr.Attempts)
		}
	})

	t.Run("unmapped_protocol_response", func(t *testing.T) {
		server := &regressionProvider{
			host: "mapped-unrecognized.invalid:119",
			respond: func(connection int, _ string) []byte {
				if connection == 1 {
					return []byte("451 mapped article absence\r\n")
				}
				return []byte("499 unrecognized vendor response\r\n")
			},
		}
		client := newResponse451Client(t, response451Provider(server, "mapped-unrecognized", false, Response451AbsentAfterRetry))
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		transportErr := require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("error = %v, mapped 451 plus unknown response must not be absence", err)
		}
		if len(transportErr.Attempts) != 2 ||
			transportErr.Attempts[0].Outcome != OutcomeHardArticleAbsence ||
			transportErr.Attempts[1].Outcome != OutcomeInconclusive ||
			transportErr.Attempts[1].ResponseCode != 499 {
			t.Fatalf("attempts = %+v, want mapped 451 then raw inconclusive 499", transportErr.Attempts)
		}
	})
}

func Test451ACallerCancellationWinsAndStopsFallback(t *testing.T) {
	t.Run("before_dispatch", func(t *testing.T) {
		server := &regressionProvider{
			host:    "cancel-before.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		}
		client := newResponse451Client(t, response451Provider(server, "cancel-before", false, Response451AbsentAfterRetry))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.Stat(ctx, "fixture@example.invalid")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stat() error = %v, want context.Canceled", err)
		}
		if server.commandCount("STAT") != 0 {
			t.Fatalf("pre-cancel dispatched %d STAT commands", server.commandCount("STAT"))
		}
	})

	t.Run("during_fresh_retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var once sync.Once
		var commands atomic.Int32
		factory := func(context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 cancellation regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				commands.Add(1)
				if _, err := server.Write([]byte("451 mapped article absence\r\n")); err == nil {
					// readerLoop retires every 451 transport only after it has built
					// and delivered the attempt. Waiting for that close places the
					// cancellation deterministically inside the retry-delay boundary.
					_, _ = reader.ReadByte()
					once.Do(cancel)
				}
			}()
			return client, nil
		}
		backup := &regressionProvider{
			host:    "cancel-backup.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
		}
		client := newResponse451Client(t,
			Provider{
				ID:                "cancel-mapped",
				Host:              "cancel-mapped.invalid:119",
				Factory:           factory,
				Connections:       1,
				SkipPing:          true,
				Response451Policy: Response451AbsentAfterRetry,
			},
			response451Provider(backup, "cancel-backup", true, Response451Temporary),
		)
		_, err := client.Stat(ctx, "fixture@example.invalid")
		transportErr := require451TransportError(t, err, OutcomeCancellation)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stat() error = %v, want caller cancellation", err)
		}
		if commands.Load() != 1 || backup.commandCount("STAT") != 0 {
			t.Fatalf("primary/backup commands = %d/%d, cancellation must stop retry and fallback", commands.Load(), backup.commandCount("STAT"))
		}
		if len(transportErr.Attempts) == 0 || transportErr.Attempts[0].ResponseCode != 451 {
			t.Fatalf("attempts = %+v, want the completed raw 451 before cancellation", transportErr.Attempts)
		}
	})

	t.Run("after_fresh_retry_dispatch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		retryDispatched := make(chan struct{})
		var connections atomic.Int32
		var commands atomic.Int32
		factory := func(context.Context) (net.Conn, error) {
			connection := connections.Add(1)
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 dispatched-cancel regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				commands.Add(1)
				if connection == 1 {
					_, _ = server.Write([]byte("451 mapped article absence\r\n"))
					return
				}
				close(retryDispatched)
				_, _ = reader.ReadByte()
			}()
			return client, nil
		}
		backup := &regressionProvider{
			host:    "cancel-dispatched-backup.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
		}
		client := newResponse451Client(t,
			Provider{
				ID:                "cancel-dispatched-mapped",
				Host:              "cancel-dispatched-mapped.invalid:119",
				Factory:           factory,
				Connections:       1,
				SkipPing:          true,
				Response451Policy: Response451AbsentAfterRetry,
			},
			response451Provider(backup, "cancel-dispatched-backup", true, Response451Temporary),
		)
		errCh := make(chan error, 1)
		go func() {
			_, err := client.Stat(ctx, "fixture@example.invalid")
			errCh <- err
		}()
		select {
		case <-retryDispatched:
			cancel()
		case <-time.After(2 * time.Second):
			t.Fatal("fresh retry was not dispatched")
		}
		err := <-errCh
		transportErr := require451TransportError(t, err, OutcomeCancellation)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stat() error = %v, want context.Canceled", err)
		}
		if commands.Load() != 2 || backup.commandCount("STAT") != 0 {
			t.Fatalf("primary/backup commands = %d/%d, cancellation must stop fallback", commands.Load(), backup.commandCount("STAT"))
		}
		if len(transportErr.Attempts) != 2 ||
			transportErr.Attempts[0].ResponseCode != 451 ||
			transportErr.Attempts[1].Outcome != OutcomeCancellation {
			t.Fatalf("attempts = %+v, want completed mapped 451 then dispatched cancellation", transportErr.Attempts)
		}
	})
}

func Test451AOrderedFallbackAfterMappedAbsence(t *testing.T) {
	mapped := &regressionProvider{
		host:    "mapped-primary.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
	}
	missing := &regressionProvider{
		host:    "missing-secondary.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	backup := &regressionProvider{
		host:    "available-backup.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
	}
	client := newResponse451Client(t,
		response451Provider(mapped, "mapped-primary", false, Response451AbsentAfterRetry),
		response451Provider(missing, "missing-secondary", false, Response451Temporary),
		response451Provider(backup, "available-backup", true, Response451Temporary),
	)
	result, err := client.Stat(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if result.ProviderID != "available-backup" {
		t.Fatalf("serving provider = %q, want available-backup", result.ProviderID)
	}
	if mapped.commandCount("STAT") != 2 || missing.commandCount("STAT") != 1 || backup.commandCount("STAT") != 1 {
		t.Fatalf("mapped/missing/backup STATs = %d/%d/%d, want 2/1/1", mapped.commandCount("STAT"), missing.commandCount("STAT"), backup.commandCount("STAT"))
	}
	want := []struct {
		provider string
		outcome  OutcomeKind
	}{
		{provider: "mapped-primary", outcome: OutcomeHardArticleAbsence},
		{provider: "mapped-primary", outcome: OutcomeHardArticleAbsence},
		{provider: "missing-secondary", outcome: OutcomeHardArticleAbsence},
		{provider: "available-backup", outcome: OutcomeSuccess},
	}
	if len(result.Attempts) != len(want) {
		t.Fatalf("attempts = %+v, want %d", result.Attempts, len(want))
	}
	for index, expected := range want {
		if got := result.Attempts[index]; got.ProviderID != expected.provider || got.Outcome != expected.outcome {
			t.Errorf("attempt[%d] = %+v, want provider=%s outcome=%s", index, got, expected.provider, expected.outcome)
		}
	}
}

func Test451AMappedAbsenceUsesExistingParallelProbeFallback(t *testing.T) {
	mapped := &regressionProvider{
		host:    "mapped-probe-primary.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
	}
	available := &regressionProvider{
		host: "mapped-probe-available.invalid:119",
		respond: func(_ int, command string) []byte {
			switch {
			case strings.HasPrefix(command, "STAT"):
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			case strings.HasPrefix(command, "BODY"):
				return yencSinglePart([]byte("probe fallback payload"), "probe.bin")
			default:
				return []byte("500 unexpected command\r\n")
			}
		},
	}
	missing := &regressionProvider{
		host:    "mapped-probe-missing.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	client := newRegressionClient(t,
		response451Provider(mapped, "mapped-probe-primary", false, Response451AbsentAfterRetry),
		response451Provider(available, "mapped-probe-available", false, Response451Temporary),
		response451Provider(missing, "mapped-probe-missing", false, Response451Temporary),
	)
	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("probe fallback payload")) {
		t.Fatalf("Body() bytes = %q", body.Bytes)
	}
	if available.commandCount("STAT") != 1 || available.commandCount("BODY") != 1 || missing.commandCount("STAT") != 1 {
		t.Fatalf("available STAT/BODY and missing STAT = %d/%d/%d, want existing probe flow 1/1/1",
			available.commandCount("STAT"), available.commandCount("BODY"), missing.commandCount("STAT"))
	}
}

func Test451AMappedAbsenceInsideParallelProbeAndWinner(t *testing.T) {
	initialMissing := &regressionProvider{
		host:    "parallel-inner-initial.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	mappedProbe := &regressionProvider{
		host:    "parallel-inner-probe.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
	}
	mappedWinner := &regressionProvider{
		host: "parallel-inner-winner.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return []byte("451 mapped article absence\r\n")
		},
	}
	available := &regressionProvider{
		host: "parallel-inner-available.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("parallel inner fallback"), "parallel-inner.bin")
		},
	}
	client := newRegressionClient(t,
		response451Provider(initialMissing, "parallel-inner-initial", false, Response451Temporary),
		response451Provider(mappedProbe, "parallel-inner-probe", false, Response451AbsentAfterRetry),
		response451Provider(mappedWinner, "parallel-inner-winner", false, Response451AbsentAfterRetry),
		response451Provider(available, "parallel-inner-available", false, Response451Temporary),
	)

	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("parallel inner fallback")) {
		t.Fatalf("Body() bytes = %q", body.Bytes)
	}
	if mappedProbe.commandCount("STAT") != 2 || mappedProbe.commandCount("BODY") != 0 {
		t.Fatalf("mapped probe STAT/BODY = %d/%d, want conclusive mapped probe 2/0", mappedProbe.commandCount("STAT"), mappedProbe.commandCount("BODY"))
	}
	if mappedWinner.commandCount("STAT") != 1 || mappedWinner.commandCount("BODY") != 2 {
		t.Fatalf("mapped winner STAT/BODY = %d/%d, want positive probe then conclusive mapped BODY 1/2", mappedWinner.commandCount("STAT"), mappedWinner.commandCount("BODY"))
	}
	if available.commandCount("STAT") != 1 || available.commandCount("BODY") != 1 {
		t.Fatalf("available STAT/BODY = %d/%d, want 1/1", available.commandCount("STAT"), available.commandCount("BODY"))
	}

	wantMapped := map[string]int{
		"parallel-inner-probe":  2,
		"parallel-inner-winner": 2,
	}
	for _, attempt := range body.Attempts {
		if remaining := wantMapped[attempt.ProviderID]; remaining > 0 && attempt.ResponseCode == 451 {
			if attempt.Outcome != OutcomeHardArticleAbsence {
				t.Fatalf("mapped parallel attempt = %+v, want hard article absence", attempt)
			}
			wantMapped[attempt.ProviderID] = remaining - 1
		}
	}
	for providerID, remaining := range wantMapped {
		if remaining != 0 {
			t.Fatalf("provider %s missing %d mapped parallel attempts: %+v", providerID, remaining, body.Attempts)
		}
	}
}

func Test451AMappedAbsenceInsideBackupParallelProbe(t *testing.T) {
	initialMissing := &regressionProvider{
		host:    "backup-probe-initial.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	mappedBackup := &regressionProvider{
		host:    "backup-probe-mapped.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
	}
	availableBackup := &regressionProvider{
		host: "backup-probe-available.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("backup parallel fallback"), "backup-parallel.bin")
		},
	}
	client := newRegressionClient(t,
		response451Provider(initialMissing, "backup-probe-initial", false, Response451Temporary),
		response451Provider(mappedBackup, "backup-probe-mapped", true, Response451AbsentAfterRetry),
		response451Provider(availableBackup, "backup-probe-available", true, Response451Temporary),
	)

	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("backup parallel fallback")) {
		t.Fatalf("Body() bytes = %q", body.Bytes)
	}
	if mappedBackup.commandCount("STAT") != 2 || mappedBackup.commandCount("BODY") != 0 {
		t.Fatalf("mapped backup STAT/BODY = %d/%d, want 2/0", mappedBackup.commandCount("STAT"), mappedBackup.commandCount("BODY"))
	}
	if availableBackup.commandCount("STAT") != 1 || availableBackup.commandCount("BODY") != 1 {
		t.Fatalf("available backup STAT/BODY = %d/%d, want 1/1", availableBackup.commandCount("STAT"), availableBackup.commandCount("BODY"))
	}
}

func Test451AMappedAbsenceInSingleLiveAndSequentialBackupPaths(t *testing.T) {
	t.Run("single_live_parallel_candidate", func(t *testing.T) {
		initialMissing := &regressionProvider{
			host:    "single-live-initial.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
		}
		mapped := &regressionProvider{
			host:    "single-live-mapped.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		}
		availableBackup := &regressionProvider{
			host:    "single-live-backup.invalid:119",
			respond: func(_ int, _ string) []byte { return yencSinglePart([]byte("single live fallback"), "single-live.bin") },
		}
		client := newRegressionClient(t,
			response451Provider(initialMissing, "single-live-initial", false, Response451Temporary),
			response451Provider(mapped, "single-live-mapped", false, Response451AbsentAfterRetry),
			response451Provider(availableBackup, "single-live-backup", true, Response451Temporary),
		)
		body, err := client.Body(context.Background(), "fixture@example.invalid")
		if err != nil {
			t.Fatalf("Body() error = %v", err)
		}
		if !bytes.Equal(body.Bytes, []byte("single live fallback")) || mapped.commandCount("BODY") != 2 {
			t.Fatalf("body/mapped BODY attempts = %q/%d, want successful fallback after two mapped attempts", body.Bytes, mapped.commandCount("BODY"))
		}
	})

	t.Run("sequential_backup", func(t *testing.T) {
		initialMissing := &regressionProvider{
			host:    "sequential-backup-initial.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
		}
		mappedBackup := &regressionProvider{
			host:    "sequential-backup-mapped.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		}
		availableBackup := &regressionProvider{
			host:    "sequential-backup-available.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
		}
		client := newResponse451Client(t,
			response451Provider(initialMissing, "sequential-backup-initial", false, Response451Temporary),
			response451Provider(mappedBackup, "sequential-backup-mapped", true, Response451AbsentAfterRetry),
			response451Provider(availableBackup, "sequential-backup-available", true, Response451Temporary),
		)
		result, err := client.Stat(context.Background(), "fixture@example.invalid")
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		if result.ProviderID != "sequential-backup-available" || mappedBackup.commandCount("STAT") != 2 {
			t.Fatalf("provider/mapped attempts = %q/%d, want available backup after two mapped attempts", result.ProviderID, mappedBackup.commandCount("STAT"))
		}
	})
}

func Test451AAllProviderHardAbsenceIsSentinelCompatible(t *testing.T) {
	mapped := &regressionProvider{
		host:    "all-hard-mapped.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 vendor article removal\r\n") },
	}
	missing430 := &regressionProvider{
		host:    "all-hard-430.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	missing423 := &regressionProvider{
		host:    "all-hard-423.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("423 no article with that number\r\n") },
	}
	client := newResponse451Client(t,
		response451Provider(mapped, "mapped", false, Response451AbsentAfterRetry),
		response451Provider(missing430, "missing-430", false, Response451Temporary),
		response451Provider(missing423, "missing-423", true, Response451Temporary),
	)
	_, err := client.Stat(context.Background(), "fixture@example.invalid")
	transportErr := require451TransportError(t, err, OutcomeHardArticleAbsence)
	if !errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("error = %v, want ErrArticleNotFound compatibility", err)
	}
	if len(transportErr.Attempts) != 4 {
		t.Fatalf("attempts = %+v, want two mapped plus 430 and 423", transportErr.Attempts)
	}
	if transportErr.Attempts[0].ResponseCode != 451 || transportErr.Attempts[1].ResponseCode != 451 {
		t.Fatalf("mapped attempts = %+v, raw 451 must not be fabricated as 430", transportErr.Attempts[:2])
	}
}

func Test451AMixedEvidenceNeverBecomesGlobalAbsence(t *testing.T) {
	t.Run("unmapped_temporary", func(t *testing.T) {
		mapped := &regressionProvider{
			host:    "mixed-mapped.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		}
		temporary := &regressionProvider{
			host:    "mixed-temporary.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 temporary article response\r\n") },
		}
		client := newResponse451Client(t,
			response451Provider(mapped, "mapped", false, Response451AbsentAfterRetry),
			response451Provider(temporary, "temporary", false, Response451Temporary),
		)
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		_ = require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("error = %v, hard plus temporary must remain inconclusive", err)
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		mapped := &regressionProvider{
			host:    "mixed-unavailable-mapped.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		}
		unavailable := &regressionProvider{
			host:    "mixed-unavailable.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
		}
		unavailableConfig := response451Provider(unavailable, "unavailable", false, Response451Temporary)
		unavailableConfig.QuotaBytes = 1
		unavailableConfig.QuotaUsed = 1
		client := newResponse451Client(t,
			response451Provider(mapped, "mapped", false, Response451AbsentAfterRetry),
			unavailableConfig,
		)
		_, err := client.Stat(context.Background(), "fixture@example.invalid")
		_ = require451TransportError(t, err, OutcomeInconclusive)
		if errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("error = %v, unavailable provider must prevent global absence", err)
		}
	})

	t.Run("omitted_or_incomplete_evidence", func(t *testing.T) {
		raw451 := &Error{Code: 451, Message: "mapped article absence"}
		err := newTransportError([]AttemptEvidence{
			{ProviderID: "mapped", Operation: OperationStat, Outcome: OutcomeHardArticleAbsence, ResponseCode: 451, Cause: raw451},
			{ProviderID: "undispatched", Operation: OperationStat, Outcome: OutcomeInconclusive, Cause: errors.New("provider result omitted")},
		}, raw451)
		if err.Kind != OutcomeInconclusive || errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("aggregate = %+v, omitted/incomplete provider must prevent global absence", err)
		}
	})
}

func Test451ARawMappedEvidenceIsPreserved(t *testing.T) {
	server := &regressionProvider{
		host:    "raw-evidence.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("451 documented vendor article removal\r\n") },
	}
	client := newResponse451Client(t, response451Provider(server, "stable-provider-id", false, Response451AbsentAfterRetry))
	response := <-client.Send(context.Background(), []byte("ARTICLE <fixture@example.invalid>\r\n"), nil)
	err := responseError(response)
	transportErr := require451TransportError(t, err, OutcomeHardArticleAbsence)
	if transportErr.ProviderID != "stable-provider-id" || transportErr.ResponseCode != 451 {
		t.Fatalf("transport attribution = provider %q code %d, want stable-provider-id/raw 451", transportErr.ProviderID, transportErr.ResponseCode)
	}
	if !errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("error = %v, want ErrArticleNotFound compatibility", err)
	}
	var raw *Error
	if !errors.As(err, &raw) || raw.Code != 451 || !strings.Contains(raw.Message, "documented vendor article removal") {
		t.Fatalf("wrapped raw error = %#v, want original vendor 451", raw)
	}
	if len(transportErr.Attempts) != 2 {
		t.Fatalf("attempts = %+v, want two", transportErr.Attempts)
	}
	for index, attempt := range transportErr.Attempts {
		if attempt.ProviderID != "stable-provider-id" || attempt.Operation != OperationArticle ||
			attempt.Outcome != OutcomeHardArticleAbsence || attempt.ResponseCode != 451 {
			t.Errorf("attempt[%d] = %+v, want stable raw ARTICLE hard absence", index, attempt)
		}
		if attempt.PoolQueueDuration < 0 || attempt.PipelineHeadWaitDuration < 0 || attempt.ResponseServiceDuration < 0 {
			t.Errorf("attempt[%d] has negative timing: %+v", index, attempt)
		}
		var attemptRaw *Error
		if !errors.As(attempt.Cause, &attemptRaw) || attemptRaw.Code != 451 {
			t.Errorf("attempt[%d] cause = %v, want raw 451", index, attempt.Cause)
		}
	}
}

func Test451AOrdinaryInitial423430DoNotGainRetry(t *testing.T) {
	for _, code := range []int{423, 430} {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			missing := &regressionProvider{
				host: "deferred-missing.invalid:119",
				respond: func(_ int, _ string) []byte {
					return []byte(fmt.Sprintf("%d ordinary article absence\r\n", code))
				},
			}
			available := &regressionProvider{
				host:    "deferred-available.invalid:119",
				respond: func(_ int, _ string) []byte { return []byte("223 1 <fixture@example.invalid> exists\r\n") },
			}
			client := newResponse451Client(t,
				response451Provider(missing, "missing", false, Response451AbsentAfterRetry),
				response451Provider(available, "available", false, Response451Temporary),
			)
			if _, err := client.Stat(context.Background(), "fixture@example.invalid"); err != nil {
				t.Fatalf("Stat() error = %v", err)
			}
			if missing.commandCount("STAT") != 1 || missing.connections.Load() != 1 {
				t.Fatalf("ordinary %d attempts/connections = %d/%d, same-provider retry is deferred", code, missing.commandCount("STAT"), missing.connections.Load())
			}
			if available.commandCount("STAT") != 1 {
				t.Fatalf("fallback STAT attempts = %d, want 1", available.commandCount("STAT"))
			}
		})
	}
}

func seedBreakerFailures(t *testing.T, client *Client, providerID string, count int) *providerGroup {
	t.Helper()
	group := client.findGroup(providerID)
	if group == nil {
		t.Fatalf("provider %q not found", providerID)
	}
	for range count {
		lease, err := group.breaker.acquire(providerID)
		if err != nil {
			t.Fatalf("breaker acquire before seed failure: %v", err)
		}
		group.breaker.complete(lease, circuitBreakerFailure)
	}
	return group
}

func waitFor451ProviderConnections(t *testing.T, client *Client, providerID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := response451ProviderStats(t, client, providerID)
		if stats.ActiveConnections == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider %q active connections = %d, want %d", providerID, stats.ActiveConnections, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func Test451AMappedResultsAreCircuitBreakerNeutral(t *testing.T) {
	t.Run("absence_does_not_increment_or_open", func(t *testing.T) {
		clock := newBreakerFakeClock()
		server, provider := breakerProvider(
			"mapped-neutral",
			"mapped-neutral.invalid:119",
			func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		)
		provider.Response451Policy = Response451AbsentAfterRetry
		client := newBreakerClient(t, clock, provider)
		for request := range 3 {
			err := targetedBreakerStat(client, "mapped-neutral")
			if !errors.Is(err, ErrArticleNotFound) {
				var transportErr *TransportError
				_ = errors.As(err, &transportErr)
				var attempts []AttemptEvidence
				if transportErr != nil {
					attempts = transportErr.Attempts
				}
				t.Fatalf("request %d error = %v, attempts = %+v, want mapped hard absence", request, err, attempts)
			}
			stats := providerBreakerStats(t, client, "mapped-neutral")
			if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 || stats.Cooldown != 0 {
				t.Fatalf("request %d breaker stats = %+v, mapped absence must be neutral", request, stats)
			}
			// Every 451 retires its socket. Wait for that lifecycle transition so
			// the next independent request does not intentionally inherit a stale
			// transport attempt, which would correctly make its evidence mixed.
			waitFor451ProviderConnections(t, client, "mapped-neutral", 0)
		}
		if server.commandCount("STAT") != 6 {
			t.Fatalf("mapped STAT commands = %d, want 6", server.commandCount("STAT"))
		}
	})

	t.Run("retry_success_does_not_reset_failure_window", func(t *testing.T) {
		clock := newBreakerFakeClock()
		var commands atomic.Int32
		_, provider := breakerProvider(
			"mapped-success-neutral",
			"mapped-success-neutral.invalid:119",
			func(_ int, _ string) []byte {
				if commands.Add(1)%2 == 1 {
					return []byte("451 mapped article absence\r\n")
				}
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
		)
		provider.Response451Policy = Response451AbsentAfterRetry
		client := newBreakerClient(t, clock, provider)
		seedBreakerFailures(t, client, "mapped-success-neutral", 2)
		if err := targetedBreakerStat(client, "mapped-success-neutral"); err != nil {
			t.Fatalf("mapped retry success error = %v", err)
		}
		stats := providerBreakerStats(t, client, "mapped-success-neutral")
		if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 2 {
			t.Fatalf("breaker stats after mapped retry success = %+v, want closed with prior two failures retained", stats)
		}
	})

	t.Run("retry_timeout_does_not_increment_failure_window", func(t *testing.T) {
		clock := newBreakerFakeClock()
		var connections atomic.Int32
		factory := func(ctx context.Context) (net.Conn, error) {
			connection := connections.Add(1)
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 breaker-timeout regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				if connection == 1 {
					_, _ = server.Write([]byte("451 mapped article absence\r\n"))
					return
				}
				<-ctx.Done()
			}()
			return client, nil
		}
		provider := Provider{
			ID:                "mapped-timeout-neutral",
			Host:              "mapped-timeout-neutral.invalid:119",
			Factory:           factory,
			Connections:       1,
			SkipPing:          true,
			AttemptTimeout:    30 * time.Millisecond,
			Response451Policy: Response451AbsentAfterRetry,
		}
		client := newBreakerClient(t, clock, provider)
		seedBreakerFailures(t, client, "mapped-timeout-neutral", 2)
		err := targetedBreakerStat(client, "mapped-timeout-neutral")
		_ = require451TransportError(t, err, OutcomeInconclusive)
		stats := providerBreakerStats(t, client, "mapped-timeout-neutral")
		if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 2 {
			t.Fatalf("breaker stats after mapped retry timeout = %+v, want closed with prior two failures retained", stats)
		}
	})

	t.Run("half_open_probe_is_released_without_close_or_extension", func(t *testing.T) {
		clock := newBreakerFakeClock()
		server, provider := breakerProvider(
			"mapped-half-open",
			"mapped-half-open.invalid:119",
			func(_ int, _ string) []byte { return []byte("451 mapped article absence\r\n") },
		)
		provider.Response451Policy = Response451AbsentAfterRetry
		client := newBreakerClient(t, clock, provider)
		seedBreakerFailures(t, client, "mapped-half-open", providerBreakerFailureThreshold)
		opened := providerBreakerStats(t, client, "mapped-half-open")
		if opened.State != CircuitBreakerOpen {
			t.Fatalf("seeded breaker stats = %+v, want open", opened)
		}
		clock.Advance(providerBreakerCooldowns[0])

		for probe := range 2 {
			err := targetedBreakerStat(client, "mapped-half-open")
			if !errors.Is(err, ErrArticleNotFound) {
				t.Fatalf("neutral probe %d error = %v, want mapped absence", probe, err)
			}
			stats := providerBreakerStats(t, client, "mapped-half-open")
			if stats.State != CircuitBreakerHalfOpen || stats.ProbeInFlight || stats.Cooldown != opened.Cooldown || !stats.OpenUntil.Equal(opened.OpenUntil) {
				t.Fatalf("neutral probe %d stats = %+v, want unchanged eligible half-open state from %+v", probe, stats, opened)
			}
			waitFor451ProviderConnections(t, client, "mapped-half-open", 0)
		}
		if server.commandCount("STAT") != 4 {
			t.Fatalf("half-open mapped STAT commands = %d, want two fresh retry pairs", server.commandCount("STAT"))
		}
	})

	t.Run("half_open_retry_success_is_released_without_closing", func(t *testing.T) {
		clock := newBreakerFakeClock()
		var commands atomic.Int32
		server, provider := breakerProvider(
			"mapped-half-open-success",
			"mapped-half-open-success.invalid:119",
			func(_ int, _ string) []byte {
				if commands.Add(1)%2 == 1 {
					return []byte("451 mapped article absence\r\n")
				}
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			},
		)
		provider.Response451Policy = Response451AbsentAfterRetry
		client := newBreakerClient(t, clock, provider)
		seedBreakerFailures(t, client, "mapped-half-open-success", providerBreakerFailureThreshold)
		opened := providerBreakerStats(t, client, "mapped-half-open-success")
		clock.Advance(providerBreakerCooldowns[0])

		for probe := range 2 {
			if err := targetedBreakerStat(client, "mapped-half-open-success"); err != nil {
				t.Fatalf("neutral successful probe %d error = %v", probe, err)
			}
			stats := providerBreakerStats(t, client, "mapped-half-open-success")
			if stats.State != CircuitBreakerHalfOpen || stats.ProbeInFlight || stats.Cooldown != opened.Cooldown || !stats.OpenUntil.Equal(opened.OpenUntil) {
				t.Fatalf("neutral successful probe %d stats = %+v, want unchanged eligible half-open state from %+v", probe, stats, opened)
			}
		}
		if server.commandCount("STAT") != 4 {
			t.Fatalf("half-open mapped-success STAT commands = %d, want two fresh retry pairs", server.commandCount("STAT"))
		}
	})
}
