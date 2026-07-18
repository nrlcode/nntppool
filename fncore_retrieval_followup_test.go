package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
)

func fncoreCHG003WrappedUUResponse(lines ...[]byte) []byte {
	var response bytes.Buffer
	response.WriteString("222 0 <uu-followup@example.invalid> body\r\nbegin 644 followup.bin\r\n")
	for _, line := range lines {
		response.Write(line)
		response.WriteString("\r\n")
	}
	response.WriteString("`\r\nend\r\n.\r\n")
	return response.Bytes()
}

func TestFNCORECHG003InvalidUULengthByteCannotBecomeHealthy(t *testing.T) {
	t.Run("unrecognized first line remains inconclusive", func(t *testing.T) {
		provider := &regressionProvider{
			host: "fncore-uu-invalid-length.invalid:119",
			respond: func(int, string) []byte {
				return []byte("222 0 <invalid-length@example.invalid> body\r\na    \r\n.\r\n")
			},
		}
		client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

		body, err := client.Body(context.Background(), "invalid-length@example.invalid")
		var transportErr *TransportError
		if body != nil || !errors.As(err, &transportErr) || transportErr.Kind != OutcomeInconclusive {
			t.Fatalf("Body() = body %v, error %v; want unknown/inconclusive content", body, err)
		}
		if errors.Is(err, ErrBodyCorrupt) {
			t.Fatalf("unrecognized invalid length was fabricated as corruption: %v", err)
		}
	})

	t.Run("invalid line after recognition is corrupt", func(t *testing.T) {
		response := []byte("222 0 <invalid-recognized@example.invalid> body\r\n")
		response = append(response, fncoreCHG003UULines(bytes.Repeat([]byte("v"), 45))...)
		response = append(response, []byte("a    \r\n.\r\n")...)
		provider := &regressionProvider{
			host:    "fncore-uu-recognized-invalid.invalid:119",
			respond: func(int, string) []byte { return response },
		}
		client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

		if _, err := client.Body(context.Background(), "invalid-recognized@example.invalid"); !errors.Is(err, ErrBodyCorrupt) {
			t.Fatalf("Body() error = %v, want recognized malformed UU corruption", err)
		}
	})
}

func TestFNCORECHG003EmptyWrappedUUIsValid(t *testing.T) {
	primary := &regressionProvider{
		host: "fncore-uu-empty.invalid:119",
		respond: func(int, string) []byte {
			return fncoreCHG003WrappedUUResponse()
		},
	}
	backup := &regressionProvider{
		host: "fncore-uu-empty-backup.invalid:119",
		respond: func(int, string) []byte {
			return yencSinglePart([]byte("must not win"), "backup.bin")
		},
	}
	client := fncoreCHG003Client(t,
		fncoreCHG003Provider(primary, false),
		fncoreCHG003Provider(backup, true),
	)

	body, err := client.Body(context.Background(), "empty@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if body.ProviderID != primary.host || body.Encoding != EncodingUU || body.BytesDecoded != 0 || len(body.Bytes) != 0 {
		t.Fatalf("Body() = provider %q, encoding %v, decoded %d, bytes %q; want empty UU",
			body.ProviderID, body.Encoding, body.BytesDecoded, body.Bytes)
	}
	if backup.commandCount("BODY") != 0 {
		t.Fatalf("valid empty UU fell back to backup %d times", backup.commandCount("BODY"))
	}
}

func TestFNCORECHG003UUWireCompatibilityGuards(t *testing.T) {
	t.Run("trimmed padding", func(t *testing.T) {
		for _, want := range [][]byte{{0x41}, {0x41, 0x42}} {
			line := fncoreCHG003UULine(want)
			minimum := (4*len(want) + 2) / 3
			line = line[:1+minimum]
			provider := &regressionProvider{
				host:    "fncore-uu-trimmed.invalid:119",
				respond: func(int, string) []byte { return fncoreCHG003WrappedUUResponse(line) },
			}
			client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))
			body, err := client.Body(context.Background(), "trimmed@example.invalid")
			if err != nil || !bytes.Equal(body.Bytes, want) {
				t.Fatalf("Body() = body %v, error %v; want trimmed UU bytes %x", body, err, want)
			}
		}
	})

	t.Run("dot stuffed length line", func(t *testing.T) {
		want := bytes.Repeat([]byte("d"), 14)
		line := fncoreCHG003UULine(want)
		if line[0] != '.' {
			t.Fatalf("fixture length byte = %q, want '.'", line[0])
		}
		line = append([]byte{'.'}, line...)
		provider := &regressionProvider{
			host:    "fncore-uu-dot.invalid:119",
			respond: func(int, string) []byte { return fncoreCHG003WrappedUUResponse(line) },
		}
		client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))
		body, err := client.Body(context.Background(), "dot@example.invalid")
		if err != nil || !bytes.Equal(body.Bytes, want) {
			t.Fatalf("Body() = body %v, error %v; want dot-unstuffed UU", body, err)
		}
	})

	t.Run("bytewise response chunks", func(t *testing.T) {
		want := bytes.Repeat([]byte("c"), 51)
		response := fncoreCHG003UUResponse(want, true)
		factory := func(context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				_, _ = server.Write([]byte("200 regression server ready\r\n"))
				reader := bufio.NewReader(server)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				for _, value := range response {
					if _, err := server.Write([]byte{value}); err != nil {
						return
					}
				}
			}()
			return client, nil
		}
		client := fncoreCHG003Client(t, Provider{
			ID: "fncore-uu-bytewise", Host: "fncore-uu-bytewise.invalid:119", Factory: factory,
			Connections: 1, Inflight: 1, SkipPing: true,
		})
		body, err := client.Body(context.Background(), "bytewise@example.invalid")
		if err != nil || !bytes.Equal(body.Bytes, want) {
			t.Fatalf("Body() = body %v, error %v; want bytewise UU payload", body, err)
		}
	})
}

func TestFNCORECHG003UnknownBodyReusesAlignedSocket(t *testing.T) {
	var calls atomic.Int32
	provider := &regressionProvider{
		host: "fncore-unknown-reuse.invalid:119",
		respond: func(int, string) []byte {
			if calls.Add(1) == 1 {
				return []byte("222 0 <unknown@example.invalid> body\r\nplain unrecognized content\r\n.\r\n")
			}
			return yencSinglePart([]byte("reused safely"), "reuse.bin")
		},
	}
	client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

	if _, err := client.Body(context.Background(), "unknown@example.invalid"); err == nil {
		t.Fatal("first unknown Body() error = nil, want inconclusive result")
	}
	body, err := client.Body(context.Background(), "reuse@example.invalid")
	if err != nil || !bytes.Equal(body.Bytes, []byte("reused safely")) {
		t.Fatalf("second Body() = body %v, error %v; want safe socket reuse", body, err)
	}
	if provider.connections.Load() != 1 {
		t.Fatalf("connections = %d, want aligned unknown response to reuse one socket", provider.connections.Load())
	}
}

func TestFNCORECHG003RawSendKeepsLegacyUUBehavior(t *testing.T) {
	provider := &regressionProvider{
		host: "fncore-raw-uu.invalid:119",
		respond: func(int, string) []byte {
			return fncoreCHG003UUResponse(bytes.Repeat([]byte("r"), 52), true)
		},
	}
	client := fncoreCHG003Client(t, fncoreCHG003Provider(provider, false))

	response := <-client.Send(context.Background(), []byte("BODY <raw-uu@example.invalid>\r\n"), nil)
	if response.Err != nil || response.StatusCode != 222 || mapFormat(response.Meta.Format) != EncodingUU {
		t.Fatalf("raw Send response = %+v, want detected raw UU success", response)
	}
	if response.Meta.BytesDecoded != 0 || response.Body.Len() != 0 {
		t.Fatalf("raw Send decoded/buffered = %d/%d, want legacy detect-only behavior",
			response.Meta.BytesDecoded, response.Body.Len())
	}
}
