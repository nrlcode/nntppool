package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnightingale/rapidyenc"
)

func sameHostProvider(
	id, username string,
	commands *atomic.Int32,
	respond func(string) []byte,
) Provider {
	return Provider{
		ID:          id,
		Host:        "shared-retention.invalid:119",
		Auth:        Auth{Username: username, Password: "test-only"},
		Connections: 1,
		Inflight:    1,
		SkipPing:    true,
		Factory: func(context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 audit regression server ready\r\n"))
				reader := bufio.NewReader(server)
				for {
					command, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					command = strings.TrimSpace(command)
					switch {
					case strings.HasPrefix(command, "AUTHINFO USER"):
						_, _ = server.Write([]byte("381 password required\r\n"))
					case strings.HasPrefix(command, "AUTHINFO PASS"):
						_, _ = server.Write([]byte("281 authentication accepted\r\n"))
					default:
						commands.Add(1)
						_, _ = server.Write(respond(command))
					}
				}
			}()
			return client, nil
		},
	}
}

func TestPR1SameHostAccountsRemainIndependentlyEligible(t *testing.T) {
	t.Run("later account succeeds", func(t *testing.T) {
		var firstCommands, secondCommands atomic.Int32
		first := sameHostProvider("shared-a", "account-a", &firstCommands, func(string) []byte {
			return []byte("430 no such article\r\n")
		})
		second := sameHostProvider("shared-b", "account-b", &secondCommands, func(command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("same-host fallback"), "same-host.bin")
		})
		client := newRegressionClient(t, first, second)

		body, err := client.Body(context.Background(), "fixture@example.invalid")
		if err != nil {
			t.Fatalf("Body() error = %v", err)
		}
		if body.ProviderID != "shared-b" || !bytes.Equal(body.Bytes, []byte("same-host fallback")) {
			t.Fatalf("serving provider/body = %q/%q, want independently eligible second account", body.ProviderID, body.Bytes)
		}
		if firstCommands.Load() == 0 || secondCommands.Load() == 0 {
			t.Fatalf("same-host commands = %d/%d, want evidence from both accounts", firstCommands.Load(), secondCommands.Load())
		}
	})

	t.Run("later account is temporary", func(t *testing.T) {
		var firstCommands, secondCommands atomic.Int32
		first := sameHostProvider("shared-a", "account-a", &firstCommands, func(string) []byte {
			return []byte("430 no such article\r\n")
		})
		second := sameHostProvider("shared-b", "account-b", &secondCommands, func(string) []byte {
			return []byte("451 temporary failure\r\n")
		})
		client := newRegressionClient(t, first, second)

		_, err := client.Body(context.Background(), "fixture@example.invalid")
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("Body() error = %v, want TransportError", err)
		}
		if transportErr.Kind != OutcomeInconclusive || errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("mixed same-host result = %v, want inconclusive and not hard absence", err)
		}
		if firstCommands.Load() == 0 || secondCommands.Load() != 2 {
			t.Fatalf("same-host commands = %d/%d, want first absence and two temporary attempts", firstCommands.Load(), secondCommands.Load())
		}
	})
}

func TestPR1SingleProbeWinnerCannotRestartCommittedWriterOnBackup(t *testing.T) {
	missing := &regressionProvider{
		host:    "single-winner-missing.invalid:119",
		respond: func(int, string) []byte { return []byte("430 no such article\r\n") },
	}
	partialFull := yencSinglePart(bytes.Repeat([]byte("p"), 256*1024), "partial.bin")
	partial := partialFull[:len(partialFull)/2]
	committing := &regressionProvider{
		host:    "single-winner-commits.invalid:119",
		respond: func(int, string) []byte { return partial },
	}
	backup := &regressionProvider{
		host: "single-winner-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not be appended"), "backup.bin")
		},
	}
	client := newRegressionClient(t,
		missing.provider(false),
		Provider{
			ID:             committing.host,
			Host:           committing.host,
			Factory:        committing.factory,
			Connections:    1,
			Inflight:       1,
			SkipPing:       true,
			AttemptTimeout: 100 * time.Millisecond,
			StallTimeout:   30 * time.Millisecond,
		},
		backup.provider(true),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sink bytes.Buffer
	_, err := client.BodyStream(ctx, "fixture@example.invalid", &sink)
	if err == nil {
		t.Fatal("BodyStream() error = nil, want committed provider failure")
	}
	if sink.Len() == 0 {
		t.Fatal("committing provider did not deliver the required prefix")
	}
	if backup.commandCount("BODY") != 0 {
		t.Fatalf("backup BODY attempts = %d, want no restart after writer commitment", backup.commandCount("BODY"))
	}
}

func TestPR1451RetryRejectsEveryPreexistingHotTransport(t *testing.T) {
	var connections atomic.Int32
	var bodyAttempts atomic.Int32
	warmSeen := make(chan int, 3)
	warmRelease := make(chan struct{})
	bodyConnections := make(chan int, 2)
	valid := yencSinglePart([]byte("fresh retry"), "fresh.bin")

	factory := func(context.Context) (net.Conn, error) {
		connection := int(connections.Add(1))
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 audit regression server ready\r\n"))
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
						_, _ = server.Write([]byte("451 temporary failure\r\n"))
					} else {
						_, _ = server.Write(valid)
					}
				}
			}
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		ID:           "multi-hot",
		Host:         "multi-hot.invalid:119",
		Factory:      factory,
		Connections:  2,
		Inflight:     1,
		StatInflight: 1,
		SkipPing:     true,
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
	if !bytes.Equal(body.Bytes, []byte("fresh retry")) {
		t.Fatalf("Body() bytes = %q", body.Bytes)
	}
	firstConnection := <-bodyConnections
	secondConnection := <-bodyConnections
	if _, existed := warmConnections[firstConnection]; !existed {
		t.Fatalf("first 451 connection = %d, want a preexisting hot transport from %v", firstConnection, warmConnections)
	}
	if _, existed := warmConnections[secondConnection]; existed {
		t.Fatalf("451 retry connection = %d, want a newly created transport, not preexisting %v", secondConnection, warmConnections)
	}
	if connections.Load() < 3 {
		t.Fatalf("created transports = %d, want at least one new connection after 451", connections.Load())
	}
}

func replaceTrailerValue(t *testing.T, response []byte, field, value string) []byte {
	t.Helper()
	marker := []byte(" " + field + "=")
	start := bytes.Index(response, marker)
	if start < 0 {
		t.Fatalf("fixture does not contain %s", field)
	}
	start += len(marker)
	rest := response[start:]
	end := bytes.IndexAny(rest, " \r\n")
	if end < 0 {
		t.Fatalf("fixture has unterminated %s", field)
	}
	result := make([]byte, 0, len(response)-end+len(value))
	result = append(result, response[:start]...)
	result = append(result, value...)
	result = append(result, rest[end:]...)
	return result
}

func appendTrailerField(t *testing.T, response []byte, field, value string) []byte {
	t.Helper()
	trailer := bytes.Index(response, []byte("\r\n=yend "))
	if trailer < 0 {
		t.Fatal("fixture does not contain =yend")
	}
	end := bytes.Index(response[trailer+2:], []byte("\r\n"))
	if end < 0 {
		t.Fatal("fixture has unterminated =yend")
	}
	end += trailer + 2
	addition := []byte(" " + field + "=" + value)
	result := make([]byte, 0, len(response)+len(addition))
	result = append(result, response[:end]...)
	result = append(result, addition...)
	result = append(result, response[end:]...)
	return result
}

func requireCorruptFallback(t *testing.T, invalidResponse []byte) {
	t.Helper()
	invalid := &regressionProvider{
		host:    "crc-invalid.invalid:119",
		respond: func(int, string) []byte { return invalidResponse },
	}
	valid := &regressionProvider{
		host: "crc-valid.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("validated fallback"), "valid.bin")
		},
	}
	client := newRegressionClient(t, invalid.provider(false), valid.provider(false))
	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("validated fallback")) {
		t.Fatalf("Body() bytes = %q, want fallback after corrupt supplied CRC", body.Bytes)
	}
	if len(body.Attempts) != 2 || body.Attempts[0].Outcome != OutcomeCorruptBody || body.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("attempts = %+v, want corrupt then success", body.Attempts)
	}
}

func TestPR1EveryApplicableSuppliedCRCIsValidated(t *testing.T) {
	payload := []byte("every supplied checksum must be considered")
	base := yencSinglePart(payload, "checksums.bin")
	actual := fmt.Sprintf("%08x", crc32.ChecksumIEEE(payload))

	t.Run("valid pcrc32 and mismatching crc32", func(t *testing.T) {
		requireCorruptFallback(t, appendTrailerField(t, base, "crc32", "deadbeef"))
	})
	t.Run("valid pcrc32 and malformed crc32", func(t *testing.T) {
		requireCorruptFallback(t, appendTrailerField(t, base, "crc32", "not-hex"))
	})
	t.Run("malformed pcrc32 and valid crc32", func(t *testing.T) {
		response := replaceTrailerValue(t, base, "pcrc32", "not-hex")
		requireCorruptFallback(t, appendTrailerField(t, response, "crc32", actual))
	})
	t.Run("mismatching pcrc32 and valid crc32", func(t *testing.T) {
		response := replaceTrailerValue(t, base, "pcrc32", "deadbeef")
		requireCorruptFallback(t, appendTrailerField(t, response, "crc32", actual))
	})
}

func TestPR1CRCEncodingMustBeExactlyEightHexDigits(t *testing.T) {
	// This payload has an IEEE CRC32 of exactly zero. It prevents permissive
	// padding/truncation from being hidden by a subsequent checksum mismatch.
	zeroCRC := []byte{0x9d, 0x0a, 0xd9, 0x6d}
	base := yencSinglePart(zeroCRC, "zero.bin")
	for name, value := range map[string]string{
		"empty":    "",
		"short":    "0",
		"overlong": "100000000",
		"non_hex":  "zzzzzzzz",
	} {
		t.Run(name, func(t *testing.T) {
			provider := &regressionProvider{
				host:    "malformed-crc.invalid:119",
				respond: func(int, string) []byte { return replaceTrailerValue(t, base, "pcrc32", value) },
			}
			client := newRegressionClient(t, provider.provider(false))
			_, err := client.Body(context.Background(), "fixture@example.invalid")
			if !errors.Is(err, ErrBodyCorrupt) {
				t.Fatalf("Body() error = %v, want malformed CRC corruption", err)
			}
		})
	}
}

func TestPR1MultipartWholeFileCRCApplicability(t *testing.T) {
	part := []byte("first multipart payload")
	base := yencMultiPart(part, "multipart.bin", 1, 2, 0)

	t.Run("well formed whole-file CRC is not compared to one partial part", func(t *testing.T) {
		provider := &regressionProvider{
			host: "multipart-crc.invalid:119",
			respond: func(int, string) []byte {
				return appendTrailerField(t, base, "crc32", "deadbeef")
			},
		}
		client := newRegressionClient(t, provider.provider(false))
		body, err := client.Body(context.Background(), "fixture@example.invalid")
		if err != nil {
			t.Fatalf("Body() error = %v, whole-file CRC is not verifiable from a partial part", err)
		}
		if !bytes.Equal(body.Bytes, part) {
			t.Fatalf("Body() bytes = %q", body.Bytes)
		}
	})

	t.Run("malformed whole-file CRC is rejected", func(t *testing.T) {
		provider := &regressionProvider{
			host: "multipart-malformed-crc.invalid:119",
			respond: func(int, string) []byte {
				return appendTrailerField(t, base, "crc32", "not-hex")
			},
		}
		client := newRegressionClient(t, provider.provider(false))
		_, err := client.Body(context.Background(), "fixture@example.invalid")
		if !errors.Is(err, ErrBodyCorrupt) {
			t.Fatalf("Body() error = %v, want malformed whole-file CRC corruption", err)
		}
	})
}

func TestPR1ControlledFeedChecksTimeLimitAfterBufferedCompletion(t *testing.T) {
	buffered := bytes.Repeat([]byte("x"), 64*1024)
	rb := readBuffer{buf: bytes.Clone(buffered), end: len(buffered)}
	started := time.Now()
	feeder := &mockFeeder{feedFunc: func(in []byte, _ io.Writer) (int, bool, error) {
		time.Sleep(10 * time.Millisecond)
		return len(in), true, nil
	}}
	err := rb.feedUntilDoneControlled(nil, feeder, io.Discard, func(int, int) (time.Time, bool, int, error) {
		if time.Since(started) >= time.Millisecond {
			return time.Time{}, false, 0, errAbandonedBodyDrainLimit
		}
		return time.Time{}, false, 0, nil
	})
	if !errors.Is(err, errAbandonedBodyDrainLimit) {
		t.Fatalf("buffered feed error = %v, want post-feed time-limit enforcement", err)
	}
}

func TestPR1SlowWriterDoesNotConsumeProviderProgressTimeout(t *testing.T) {
	payload := bytes.Repeat([]byte("w"), 512*1024)
	response := yencSinglePart(payload, "slow-writer.bin")
	provider := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 audit regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			_, _ = server.Write(response)
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		ID:             "slow-writer",
		Host:           "slow-writer.invalid:119",
		Factory:        provider,
		Connections:    1,
		Inflight:       1,
		SkipPing:       true,
		AttemptTimeout: 200 * time.Millisecond,
		StallTimeout:   20 * time.Millisecond,
	})
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
		result <- err
	}()
	select {
	case <-writer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("BODY did not reach the blocking caller writer")
	}
	time.Sleep(80 * time.Millisecond)
	close(writer.release)
	select {
	case err := <-result:
		if err != nil {
			var transportErr *TransportError
			if errors.As(err, &transportErr) {
				t.Fatalf("BodyStream() error = %v; attempts = %+v; caller writer delay is not provider timeout", err, transportErr.Attempts)
			}
			t.Fatalf("BodyStream() error = %v, caller writer delay is not provider timeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BODY did not complete after releasing caller writer")
	}
}

func TestPR1RawDecoderErrorsRetainV4Semantics(t *testing.T) {
	nativeErr := errors.New("raw native decoder regression")
	response := NNTPResponse{
		body:   true,
		Format: rapidyenc.FormatYenc,
		decodeFn: func([]byte, []byte, *rapidyenc.State) (int, int, rapidyenc.End, error) {
			return 0, 0, rapidyenc.EndNone, nativeErr
		},
	}
	if _, err := response.decodeYenc([]byte("encoded"), io.Discard); err != nil {
		t.Fatalf("raw-compatible decodeYenc() error = %v, v4 ignored native decoder errors", err)
	}
}

func TestPR1TransportErrorSummaryCannotMixAttempts(t *testing.T) {
	temporary := &Error{Code: 451, Message: "temporary"}
	absenceA := &Error{Code: 430, Message: "missing a"}
	absenceB := &Error{Code: 430, Message: "missing b"}

	t.Run("mixed pool outcome has no provider attribution", func(t *testing.T) {
		err := newTransportError([]AttemptEvidence{
			{ProviderID: "temporary-provider", Outcome: OutcomeTemporaryFailure, ResponseCode: 451, Cause: temporary},
			{ProviderID: "missing-provider", Outcome: OutcomeHardArticleAbsence, ResponseCode: 430, Cause: absenceB},
		}, temporary)
		if err.Kind != OutcomeInconclusive || err.ProviderID != "" || err.ResponseCode != 0 || err.Cause != temporary {
			t.Fatalf("mixed TransportError = %+v, want unattributed aggregate with coherent temporary cause", err)
		}
		if errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("mixed TransportError matched hard absence: %v", err)
		}
	})

	t.Run("uniform pool outcome selects one complete attempt", func(t *testing.T) {
		err := newTransportError([]AttemptEvidence{
			{ProviderID: "missing-a", Outcome: OutcomeHardArticleAbsence, ResponseCode: 430, Cause: absenceA},
			{ProviderID: "missing-b", Outcome: OutcomeHardArticleAbsence, ResponseCode: 430, Cause: absenceB},
		}, absenceA)
		if err.Kind != OutcomeHardArticleAbsence || err.ProviderID != "missing-b" || err.ResponseCode != 430 || err.Cause != absenceB {
			t.Fatalf("uniform TransportError = %+v, want fields and cause from missing-b", err)
		}
		if !errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("uniform TransportError lost hard-absence sentinel: %v", err)
		}
	})
}
