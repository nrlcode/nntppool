package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func fncoreCHG003Provider(server *regressionProvider, backup bool) Provider {
	provider := server.provider(backup)
	provider.ID = server.host
	return provider
}

func fncoreCHG003Client(t *testing.T, providers ...Provider) *Client {
	t.Helper()
	client, err := NewClient(
		context.Background(),
		providers,
		WithDispatchStrategy(DispatchFIFO),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func fncoreCHG003UUChar(value byte) byte {
	return (value & 0x3f) + ' '
}

func fncoreCHG003UULine(data []byte) []byte {
	if len(data) == 0 || len(data) > 45 {
		panic("fncoreCHG003UULine requires 1-45 bytes")
	}
	encoded := make([]byte, 1, 1+((len(data)+2)/3)*4)
	encoded[0] = fncoreCHG003UUChar(byte(len(data)))
	for offset := 0; offset < len(data); offset += 3 {
		var a, b, c byte
		a = data[offset]
		if offset+1 < len(data) {
			b = data[offset+1]
		}
		if offset+2 < len(data) {
			c = data[offset+2]
		}
		encoded = append(encoded,
			fncoreCHG003UUChar(a>>2),
			fncoreCHG003UUChar(a<<4|b>>4),
			fncoreCHG003UUChar(b<<2|c>>6),
			fncoreCHG003UUChar(c),
		)
	}
	return encoded
}

func fncoreCHG003UULines(data []byte) []byte {
	var encoded bytes.Buffer
	for len(data) > 0 {
		lineLength := min(len(data), 45)
		encoded.Write(fncoreCHG003UULine(data[:lineLength]))
		encoded.WriteString("\r\n")
		data = data[lineLength:]
	}
	return encoded.Bytes()
}

func fncoreCHG003UUResponse(data []byte, wrapped bool) []byte {
	var response bytes.Buffer
	response.WriteString("222 0 <uu@example.invalid> body\r\n")
	if wrapped {
		response.WriteString("begin 644 fixture.bin\r\n")
	}
	response.Write(fncoreCHG003UULines(data))
	if wrapped {
		response.WriteString("`\r\nend\r\n")
	}
	response.WriteString(".\r\n")
	return response.Bytes()
}

func fncoreCHG003YEndPart(t *testing.T, response []byte, replacement string) []byte {
	t.Helper()
	result := bytes.Clone(response)
	lineStart := bytes.Index(result, []byte("\r\n=yend "))
	if lineStart < 0 {
		t.Fatal("fixture has no =yend line")
	}
	lineStart += 2
	lineLength := bytes.Index(result[lineStart:], []byte("\r\n"))
	if lineLength < 0 {
		t.Fatal("fixture has unterminated =yend line")
	}
	lineEnd := lineStart + lineLength
	partStart := bytes.Index(result[lineStart:lineEnd], []byte(" part=1"))
	if partStart < 0 {
		t.Fatal("fixture =yend line has no part=1 field")
	}
	partStart += lineStart
	partEnd := partStart + len(" part=1")
	field := []byte(nil)
	if replacement != "" {
		field = []byte(" part=" + replacement)
	}
	mutated := make([]byte, 0, len(result)-partEnd+partStart+len(field))
	mutated = append(mutated, result[:partStart]...)
	mutated = append(mutated, field...)
	mutated = append(mutated, result[partEnd:]...)
	return mutated
}

func TestFNCORECHG003BodyTargetedPublishesBufferedPayload(t *testing.T) {
	want := []byte("targeted retrieval payload")
	provider := &regressionProvider{
		host: "fncore-targeted.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart(want, "targeted.bin")
		},
	}
	client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

	body, err := client.BodyTargeted(context.Background(), "targeted@example.invalid", TargetedBodyOptions{
		Provider: provider.host,
	})
	if err != nil {
		t.Fatalf("BodyTargeted() error = %v", err)
	}
	if body.ProviderID != provider.host || !bytes.Equal(body.Bytes, want) || body.BytesDecoded != len(want) {
		t.Fatalf("BodyTargeted() = provider %q, bytes %q, decoded %d; want %q, %q, %d",
			body.ProviderID, body.Bytes, body.BytesDecoded, provider.host, want, len(want))
	}
}

func TestFNCORECHG003StrictBodyFramingAndMultipartTrailerIdentity(t *testing.T) {
	part := []byte("multipart identity payload")
	multipart := yencMultiPart(part, "multipart.bin", 1, 2, 0)
	tests := []struct {
		name     string
		response []byte
	}{
		{name: "wrong positive status", response: []byte("221 wrong response for BODY\r\n.\r\n")},
		{name: "missing trailer part", response: fncoreCHG003YEndPart(t, multipart, "")},
		{name: "mismatched trailer part", response: fncoreCHG003YEndPart(t, multipart, "2")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := &regressionProvider{
				host:    "fncore-invalid-body.invalid:119",
				respond: func(int, string) []byte { return test.response },
			}
			valid := &regressionProvider{
				host: "fncore-valid-body.invalid:119",
				respond: func(int, string) []byte {
					return yencSinglePart([]byte("validated fallback"), "valid.bin")
				},
			}
			client := fncoreCHG003Client(t,
				fncoreCHG003Provider(invalid, false),
				fncoreCHG003Provider(valid, true),
			)

			body, err := client.Body(context.Background(), "framing@example.invalid")
			if err != nil {
				t.Fatalf("Body() error = %v", err)
			}
			if body.ProviderID != valid.host || !bytes.Equal(body.Bytes, []byte("validated fallback")) {
				t.Fatalf("Body() = provider %q, bytes %q; want validated backup", body.ProviderID, body.Bytes)
			}
			if invalid.commandCount("BODY") != 1 || valid.commandCount("BODY") != 1 {
				t.Fatalf("BODY commands invalid/valid = %d/%d, want 1/1",
					invalid.commandCount("BODY"), valid.commandCount("BODY"))
			}
		})
	}
}

func TestFNCORECHG003UUValidationMatrix(t *testing.T) {
	t.Run("full wrapper through Body", func(t *testing.T) {
		want := bytes.Repeat([]byte("full-uu-"), 8)
		primary := &regressionProvider{
			host:    "fncore-uu-full.invalid:119",
			respond: func(int, string) []byte { return fncoreCHG003UUResponse(want, true) },
		}
		backup := &regressionProvider{
			host: "fncore-uu-unused.invalid:119",
			respond: func(int, string) []byte {
				return yencSinglePart([]byte("must not win"), "fallback.bin")
			},
		}
		client := fncoreCHG003Client(t,
			fncoreCHG003Provider(primary, false),
			fncoreCHG003Provider(backup, true),
		)

		body, err := client.Body(context.Background(), "uu-full@example.invalid")
		if err != nil {
			t.Fatalf("Body() error = %v", err)
		}
		if body.ProviderID != primary.host || body.Encoding != EncodingUU ||
			body.BytesDecoded != len(want) || !bytes.Equal(body.Bytes, want) {
			t.Fatalf("Body() = provider %q, encoding %v, decoded %d, bytes %q",
				body.ProviderID, body.Encoding, body.BytesDecoded, body.Bytes)
		}
		if backup.commandCount("BODY") != 0 {
			t.Fatalf("recognized UU fell back to backup %d times", backup.commandCount("BODY"))
		}
	})

	t.Run("middle article through BodyPriority", func(t *testing.T) {
		want := bytes.Repeat([]byte("m"), 52)
		provider := &regressionProvider{
			host:    "fncore-uu-middle.invalid:119",
			respond: func(int, string) []byte { return fncoreCHG003UUResponse(want, false) },
		}
		client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

		body, err := client.BodyPriority(context.Background(), "uu-middle@example.invalid")
		if err != nil {
			t.Fatalf("BodyPriority() error = %v", err)
		}
		if body.Encoding != EncodingUU || body.BytesDecoded != len(want) || !bytes.Equal(body.Bytes, want) {
			t.Fatalf("BodyPriority() = encoding %v, decoded %d, bytes %q",
				body.Encoding, body.BytesDecoded, body.Bytes)
		}
	})

	t.Run("BodyStream delivers before completion and applies backpressure", func(t *testing.T) {
		want := bytes.Repeat([]byte("s"), 512*1024)
		first := want[:45]
		remainder := want[45:]
		tailWriteStarted := make(chan struct{})
		tailWriteDone := make(chan error, 1)
		factory := func(context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				if _, err := server.Write([]byte("200 regression server ready\r\n")); err != nil {
					tailWriteDone <- err
					return
				}
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					tailWriteDone <- err
					return
				}
				var prefix bytes.Buffer
				prefix.WriteString("222 0 <uu-stream@example.invalid> body\r\nbegin 644 stream.bin\r\n")
				prefix.Write(fncoreCHG003UULines(first))
				if _, err := server.Write(prefix.Bytes()); err != nil {
					tailWriteDone <- err
					return
				}
				var tail bytes.Buffer
				tail.Write(fncoreCHG003UULines(remainder))
				tail.WriteString("`\r\nend\r\n.\r\n")
				close(tailWriteStarted)
				_, err := server.Write(tail.Bytes())
				tailWriteDone <- err
			}()
			return client, nil
		}
		client := fncoreCHG003Client(t, Provider{
			ID:          "fncore-uu-stream",
			Host:        "fncore-uu-stream.invalid:119",
			Factory:     factory,
			Connections: 1,
			Inflight:    1,
			SkipPing:    true,
		})
		writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
		released := false
		defer func() {
			if !released {
				close(writer.release)
			}
		}()
		result := make(chan BodyResult, 1)
		go func() {
			body, err := client.BodyStream(context.Background(), "uu-stream@example.invalid", writer)
			result <- BodyResult{Body: body, Err: err}
		}()

		select {
		case <-writer.started:
		case <-time.After(3 * time.Second):
			t.Fatal("UU stream did not publish its first decoded line")
		}
		select {
		case <-tailWriteStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("server did not start the remaining response write")
		}
		select {
		case err := <-tailWriteDone:
			t.Fatalf("server completed tail write while caller writer was blocked: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		close(writer.release)
		released = true
		select {
		case got := <-result:
			if got.Err != nil {
				t.Fatalf("BodyStream() error = %v", got.Err)
			}
			if got.Body.Encoding != EncodingUU || got.Body.Bytes != nil || got.Body.BytesDecoded != len(want) {
				t.Fatalf("BodyStream() = encoding %v, buffered %d, decoded %d",
					got.Body.Encoding, len(got.Body.Bytes), got.Body.BytesDecoded)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("BodyStream() did not finish after releasing backpressure")
		}
		if err := <-tailWriteDone; err != nil {
			t.Fatalf("server tail write error = %v", err)
		}
	})

	for _, test := range []struct {
		name     string
		response []byte
	}{
		{
			name: "malformed recognized data line",
			response: append(
				append([]byte("222 0 <uu-malformed@example.invalid> body\r\n"), fncoreCHG003UULines(bytes.Repeat([]byte("v"), 45))...),
				[]byte("M!!!!\r\n.\r\n")...,
			),
		},
		{
			name: "truncated wrapped article",
			response: append(
				append([]byte("222 0 <uu-truncated@example.invalid> body\r\nbegin 644 truncated.bin\r\n"), fncoreCHG003UULines(bytes.Repeat([]byte("t"), 45))...),
				[]byte("`\r\n.\r\n")...,
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &regressionProvider{
				host:    "fncore-uu-corrupt.invalid:119",
				respond: func(int, string) []byte { return test.response },
			}
			client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))
			_, err := client.Body(context.Background(), "uu-corrupt@example.invalid")
			if !errors.Is(err, ErrBodyCorrupt) {
				t.Fatalf("Body() error = %v, want recognized UU corruption", err)
			}
		})
	}

	t.Run("unrecognized content remains unknown", func(t *testing.T) {
		provider := &regressionProvider{
			host: "fncore-unknown-body.invalid:119",
			respond: func(int, string) []byte {
				return []byte("222 0 <unknown@example.invalid> body\r\nplain unrecognized content\r\n.\r\n")
			},
		}
		client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))
		_, err := client.Body(context.Background(), "unknown@example.invalid")
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeInconclusive {
			t.Fatalf("Body() error = %v, want inconclusive unknown encoding", err)
		}
		if errors.Is(err, ErrBodyCorrupt) {
			t.Fatalf("unrecognized content was fabricated as corruption: %v", err)
		}
	})
}

func TestFNCORECHG003SingleCandidateFailureContinuesToBackup(t *testing.T) {
	for _, test := range []struct {
		name            string
		firstStatus     string
		secondStatus    string
		wantSecondCalls int
	}{
		{name: "423 then 451", firstStatus: "423 no article with that number", secondStatus: "451 temporary failure", wantSecondCalls: 2},
		{name: "430 then 500", firstStatus: "430 no such article", secondStatus: "500 vendor failure", wantSecondCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			first := &regressionProvider{
				host:    "fncore-main-a.invalid:119",
				respond: func(int, string) []byte { return []byte(test.firstStatus + "\r\n") },
			}
			second := &regressionProvider{
				host:    "fncore-main-b.invalid:119",
				respond: func(int, string) []byte { return []byte(test.secondStatus + "\r\n") },
			}
			backup := &regressionProvider{
				host: "fncore-backup-c.invalid:119",
				respond: func(int, string) []byte {
					return yencSinglePart([]byte("healthy backup"), "backup.bin")
				},
			}
			client := fncoreCHG003Client(t,
				fncoreCHG003Provider(first, false),
				fncoreCHG003Provider(second, false),
				fncoreCHG003Provider(backup, true),
			)

			body, err := client.Body(context.Background(), "fallback@example.invalid")
			if err != nil {
				t.Fatalf("Body() error = %v", err)
			}
			if body.ProviderID != backup.host || !bytes.Equal(body.Bytes, []byte("healthy backup")) {
				t.Fatalf("Body() = provider %q, bytes %q; want healthy backup", body.ProviderID, body.Bytes)
			}
			if got := second.commandCount("BODY"); got != test.wantSecondCalls {
				t.Fatalf("second main BODY calls = %d, want %d", got, test.wantSecondCalls)
			}
			if got := backup.commandCount("BODY"); got != 1 {
				t.Fatalf("backup BODY calls = %d, want 1", got)
			}
		})
	}
}

func TestFNCORECHG003RawSendDoesNotRetry451(t *testing.T) {
	var calls atomic.Int32
	provider := &regressionProvider{
		host: "fncore-raw-451.invalid:119",
		respond: func(int, string) []byte {
			if calls.Add(1) == 1 {
				return []byte("451 temporary stateful failure\r\n")
			}
			return []byte("211 replayed stateful command\r\n")
		},
	}
	client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

	response := <-client.Send(context.Background(), []byte("GROUP alt.fncore\r\n"), nil)
	if response.Err != nil || response.StatusCode != 451 {
		t.Fatalf("raw Send response = code %d, error %v; want original raw 451", response.StatusCode, response.Err)
	}
	if got := provider.commandCount("GROUP"); got != 1 {
		t.Fatalf("raw GROUP commands = %d, want exactly one", got)
	}
	if got := provider.connections.Load(); got != 1 {
		t.Fatalf("raw GROUP connections = %d, want exactly one", got)
	}
	if len(response.Attempts) != 1 || response.Attempts[0].Operation != OperationUnknown {
		t.Fatalf("raw GROUP attempts = %+v, want one UNKNOWN operation", response.Attempts)
	}
}

func TestFNCORECHG003BootstrapErrorsPreserveProtocolEvidence(t *testing.T) {
	greetingFactory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("499 vendor-specific greeting failure\r\n"))
		}()
		return client, nil
	}
	tests := []struct {
		name     string
		provider Provider
		code     int
		outcome  OutcomeKind
		is       error
	}{
		{
			name: "vendor greeting",
			provider: Provider{
				ID: "fncore-greeting", Host: "fncore-greeting.invalid:119", Factory: greetingFactory,
				Connections: 1, Inflight: 1, SkipPing: true,
			},
			code: 499, outcome: OutcomeInconclusive,
		},
		{
			name: "authentication rejection",
			provider: Provider{
				ID: "fncore-auth", Host: "fncore-auth.invalid:119", Factory: authRejectingBreakerFactory,
				Connections: 1, Inflight: 1, SkipPing: true,
				Auth: Auth{Username: "fixture", Password: "rejected"},
			},
			code: 481, outcome: OutcomeProviderUnavailable, is: ErrAuthRejected,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fncoreCHG003Client(t, test.provider)
			_, err := client.Body(context.Background(), "bootstrap@example.invalid")
			var transportErr *TransportError
			if !errors.As(err, &transportErr) {
				t.Fatalf("Body() error = %v, want TransportError", err)
			}
			if transportErr.Kind != test.outcome || transportErr.ResponseCode != test.code {
				t.Fatalf("transport outcome/code = %s/%d, want %s/%d",
					transportErr.Kind, transportErr.ResponseCode, test.outcome, test.code)
			}
			if test.is != nil && !errors.Is(err, test.is) {
				t.Fatalf("Body() error = %v, want errors.Is(%v)", err, test.is)
			}
			var protocolErr *Error
			if !errors.As(err, &protocolErr) || protocolErr.Code != test.code {
				t.Fatalf("Body() protocol error = %#v, want code %d", protocolErr, test.code)
			}
			if len(transportErr.Attempts) != 1 || transportErr.Attempts[0].ResponseCode != test.code {
				t.Fatalf("bootstrap attempts = %+v, want exact response code", transportErr.Attempts)
			}
		})
	}
}

func TestFNCORECHG003CancellationPrecedesQuotaEligibility(t *testing.T) {
	provider := &regressionProvider{
		host:    "fncore-quota.invalid:119",
		respond: func(int, string) []byte { return []byte("500 request must not be sent\r\n") },
	}
	spec := fncoreCHG003Provider(provider, false)
	spec.QuotaBytes = 1
	spec.QuotaUsed = 1
	client := fncoreCHG003Client(t, spec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Body(ctx, "quota@example.invalid")
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeCancellation {
		t.Fatalf("Body() error = %v, want cancellation TransportError", err)
	}
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("Body() error = %v, want caller cancellation and not quota", err)
	}
	if provider.connections.Load() != 0 || provider.commandCount("BODY") != 0 {
		t.Fatalf("canceled quota request dialed/sent: connections %d, BODY %d",
			provider.connections.Load(), provider.commandCount("BODY"))
	}
}

func TestFNCORECHG003AttemptEvidenceIsFactual(t *testing.T) {
	t.Run("wall discontinuity cannot expand duration partition", func(t *testing.T) {
		submitted := time.Now()
		completed := submitted.Add(2 * time.Second)
		request := &Request{
			Ctx:         context.Background(),
			Payload:     []byte("STAT <timing@example.invalid>\r\n"),
			submittedAt: submitted,
		}
		request.writtenAt.Store(submitted.Add(-time.Second).UnixNano())
		request.responseHeadAt.Store(submitted.Add(time.Second).UnixNano())
		attempt := buildAttemptEvidence(request, "timing-provider", Response{StatusCode: 223}, completed)
		phases := attempt.PoolQueueDuration + attempt.PipelineHeadWaitDuration + attempt.ResponseServiceDuration
		if attempt.PoolQueueDuration < 0 || attempt.PipelineHeadWaitDuration < 0 || attempt.ResponseServiceDuration < 0 {
			t.Fatalf("attempt phases are negative: %+v", attempt)
		}
		if total := completed.Sub(submitted); phases > total {
			t.Fatalf("attempt phases total %v, greater than factual request duration %v", phases, total)
		}
	})

	t.Run("cancellation summary identifies canceled attempt", func(t *testing.T) {
		response := cancellationResponse([]AttemptEvidence{
			{ProviderID: "canceled-provider", Outcome: OutcomeCancellation, Cause: context.Canceled},
			{ProviderID: "other-provider", Outcome: OutcomeTransportFailure, Cause: ErrConnectionDied},
		}, context.Canceled)
		var transportErr *TransportError
		if !errors.As(response.Err, &transportErr) {
			t.Fatalf("cancellation response error = %v, want TransportError", response.Err)
		}
		if transportErr.ProviderID != "canceled-provider" {
			t.Fatalf("cancellation provider = %q, want canceled-provider", transportErr.ProviderID)
		}
	})

	t.Run("ARTICLE operation remains factual", func(t *testing.T) {
		request := &Request{Ctx: context.Background(), Payload: []byte("aRtIcLe <article@example.invalid>\r\n")}
		attempt := buildAttemptEvidence(request, "article-provider", Response{StatusCode: 220}, time.Now())
		if attempt.Operation != operationArticle || string(attempt.Operation) != "ARTICLE" {
			t.Fatalf("ARTICLE operation = %q, want ARTICLE", attempt.Operation)
		}
	})
}
