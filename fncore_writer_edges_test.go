package nntppool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fncoreResultWriter struct {
	calls  atomic.Int32
	result func(length int) (int, error)
}

func (w *fncoreResultWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	return w.result(len(p))
}

type fncoreRecordedCommand struct {
	connection int32
	command    string
}

type fncoreCommandLog struct {
	mu       sync.Mutex
	commands []fncoreRecordedCommand
}

func (l *fncoreCommandLog) add(connection int32, command string) {
	l.mu.Lock()
	l.commands = append(l.commands, fncoreRecordedCommand{connection: connection, command: command})
	l.mu.Unlock()
}

func (l *fncoreCommandLog) snapshot() []fncoreRecordedCommand {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]fncoreRecordedCommand(nil), l.commands...)
}

func fncoreArticleResponse(payload []byte, name string) []byte {
	response := yencSinglePart(payload, name)
	copy(response[:3], "220")
	return response
}

func requireFNCORELocalWriterError(t *testing.T, err, cause error) *TransportError {
	t.Helper()
	if !errors.Is(err, cause) {
		t.Fatalf("writer result error = %v, want cause %v", err, cause)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("writer result error = %v, want TransportError", err)
	}
	if transportErr.Kind != OutcomeKind("local_failure") || len(transportErr.Attempts) != 1 {
		t.Fatalf("writer TransportError = %+v, want one local_failure attempt", transportErr)
	}
	attempt := transportErr.Attempts[0]
	if attempt.Outcome != OutcomeKind("local_failure") || attempt.ProviderResponseTimeout || !errors.Is(attempt.Cause, cause) {
		t.Fatalf("writer attempt = %+v, want local cause without provider timeout", attempt)
	}
	return transportErr
}

func TestFNCOREMalformedWriterResultsAreTerminalLocalFailures(t *testing.T) {
	writerErr := errors.New("full-count local writer failure")
	for _, test := range []struct {
		name      string
		result    func(length int) (int, error)
		wantCause error
	}{
		{
			name: "positive partial and nil",
			result: func(length int) (int, error) {
				return max(1, length/2), nil
			},
			wantCause: io.ErrShortWrite,
		},
		{
			name:      "negative count",
			result:    func(int) (int, error) { return -1, nil },
			wantCause: io.ErrShortWrite,
		},
		{
			name: "excessive count",
			result: func(length int) (int, error) {
				return length + 1, nil
			},
			wantCause: io.ErrShortWrite,
		},
		{
			name: "full count and error",
			result: func(length int) (int, error) {
				return length, writerErr
			},
			wantCause: writerErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary, provider := breakerProvider(
				"malformed-writer-"+strings.ReplaceAll(test.name, " ", "-"),
				"malformed-writer-"+strings.ReplaceAll(test.name, " ", "-")+".invalid:119",
				func(int, string) []byte {
					return yencSinglePart([]byte("malformed writer payload"), "writer.bin")
				},
			)
			backup, backupProvider := breakerProvider(
				provider.ID+"-backup",
				provider.Host+"-backup",
				func(int, string) []byte {
					return yencSinglePart([]byte("must not replay"), "backup.bin")
				},
			)
			backupProvider.Backup = true
			client := newBreakerClient(t, newBreakerFakeClock(), provider, backupProvider)
			writer := &fncoreResultWriter{result: test.result}

			_, err := client.BodyStream(context.Background(), "fixture@example.invalid", writer)
			transportErr := requireFNCORELocalWriterError(t, err, test.wantCause)
			if transportErr.Attempts[0].Operation != OperationBody || transportErr.Attempts[0].ProviderID != provider.ID {
				t.Fatalf("writer attempt = %+v, want factual provider BODY evidence", transportErr.Attempts[0])
			}
			if got := writer.calls.Load(); got != 1 {
				t.Fatalf("caller writer invocations = %d, want exactly one", got)
			}
			if got := primary.commandCount("BODY"); got != 1 {
				t.Fatalf("primary BODY commands = %d, want one", got)
			}
			if got := backup.commandCount("BODY"); got != 0 {
				t.Fatalf("backup BODY commands = %d, want no replay", got)
			}
			group := fncoreProviderGroup(t, client, provider.ID)
			if got := group.stats.Errors.Load(); got != 0 {
				t.Fatalf("provider errors = %d, want malformed local result excluded", got)
			}
			stats := group.breaker.snapshot()
			if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
				t.Fatalf("malformed local writer result changed breaker: %+v", stats)
			}
		})
	}
}

func TestFNCOREArticleWriterFaultRetiresConnectionForNextRequest(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{name: "mixed argument", payload: []byte("aRtIcLe <fixture@example.invalid>\r\n")},
		{name: "lower no argument", payload: []byte("article\r\n")},
	} {
		t.Run(test.name, func(t *testing.T) {
			writerErr := errors.New("ARTICLE local writer failure")
			var connections atomic.Int32
			var commands fncoreCommandLog
			factory := func(context.Context) (net.Conn, error) {
				connection := connections.Add(1)
				client, server := net.Pipe()
				go func() {
					defer func() { _ = server.Close() }()
					_, _ = server.Write([]byte("200 regression server ready\r\n"))
					reader := bufio.NewReader(server)
					for {
						command, err := reader.ReadString('\n')
						if err != nil {
							return
						}
						commands.add(connection, command)
						fields := strings.Fields(command)
						switch {
						case len(fields) > 0 && strings.EqualFold(fields[0], "ARTICLE"):
							_, _ = server.Write(fncoreArticleResponse([]byte("article payload"), "article.bin"))
						case len(fields) > 0 && strings.EqualFold(fields[0], "STAT"):
							_, _ = server.Write([]byte("223 1 <next@example.invalid> exists\r\n"))
						default:
							_, _ = server.Write([]byte("500 unexpected command\r\n"))
						}
					}
				}()
				return client, nil
			}
			providerID := "article-retirement-" + strings.ReplaceAll(test.name, " ", "-")
			provider := Provider{
				ID:          providerID,
				Host:        providerID + ".invalid:119",
				Factory:     factory,
				Connections: 1,
				Inflight:    1,
				SkipPing:    true,
			}
			backup, backupProvider := breakerProvider(
				providerID+"-backup",
				providerID+"-backup.invalid:119",
				func(int, string) []byte {
					return fncoreArticleResponse([]byte("must not replay"), "backup.bin")
				},
			)
			backupProvider.Backup = true
			client := newBreakerClient(t, newBreakerFakeClock(), provider, backupProvider)

			response := <-client.Send(context.Background(), test.payload, failingWriter{err: writerErr})
			transportErr := requireFNCORELocalWriterError(t, responseError(response), writerErr)
			if transportErr.Attempts[0].Operation != Operation("ARTICLE") || transportErr.Attempts[0].ProviderID != provider.ID {
				t.Fatalf("ARTICLE attempt = %+v, want factual ARTICLE operation", transportErr.Attempts[0])
			}
			group := fncoreProviderGroup(t, client, provider.ID)
			if got := group.stats.Errors.Load(); got != 0 {
				t.Fatalf("provider errors after ARTICLE writer fault = %d, want zero", got)
			}
			stats := group.breaker.snapshot()
			if stats.State != CircuitBreakerClosed || stats.QualifyingFailures != 0 {
				t.Fatalf("ARTICLE writer fault changed breaker: %+v", stats)
			}
			if backup.connections.Load() != 0 {
				t.Fatal("ARTICLE writer fault replayed on backup")
			}

			next := fncoreTargetedStat(context.Background(), client, provider.ID, "next@example.invalid")
			if next.Err != nil || next.Result == nil {
				t.Fatalf("request after ARTICLE writer fault = %+v", next)
			}
			recorded := commands.snapshot()
			if len(recorded) != 2 {
				t.Fatalf("recorded commands = %+v, want one ARTICLE then one STAT", recorded)
			}
			if recorded[0].connection != 1 || !strings.EqualFold(strings.Fields(recorded[0].command)[0], "ARTICLE") {
				t.Fatalf("first command = %+v, want ARTICLE on connection 1", recorded[0])
			}
			if recorded[1].connection != 2 || !strings.EqualFold(strings.Fields(recorded[1].command)[0], "STAT") {
				t.Fatalf("next command = %+v, want STAT on fresh connection 2", recorded[1])
			}
			if got := connections.Load(); got != 2 {
				t.Fatalf("provider connections = %d, want exactly two after unsafe socket retirement", got)
			}
		})
	}
}

func TestFNCOREPipelinedLocalWriterFailureLetsCollateralRetry(t *testing.T) {
	writerErr := errors.New("pipelined local writer failure")
	var connections atomic.Int32
	var commands fncoreCommandLog
	bodySeen := make(chan struct{})
	bothSeen := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		connection := connections.Add(1)
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if connection == 1 {
				first, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				commands.add(connection, first)
				close(bodySeen)
				second, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				commands.add(connection, second)
				close(bothSeen)
				_, _ = server.Write(yencSinglePart([]byte("primary pipeline payload"), "pipeline.bin"))
				_, _ = io.Copy(io.Discard, reader)
				return
			}
			command, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			commands.add(connection, command)
			if fields := strings.Fields(command); len(fields) > 0 && strings.EqualFold(fields[0], "STAT") {
				_, _ = server.Write([]byte("223 1 <collateral@example.invalid> exists\r\n"))
				return
			}
			_, _ = server.Write([]byte("500 unexpected retry command\r\n"))
		}()
		return client, nil
	}
	provider := Provider{
		ID:           "pipeline-local-writer",
		Host:         "pipeline-local-writer.invalid:119",
		Factory:      factory,
		Connections:  1,
		Inflight:     1,
		StatInflight: 2,
		SkipPing:     true,
	}
	backup, backupProvider := breakerProvider(
		"pipeline-local-writer-backup",
		"pipeline-local-writer-backup.invalid:119",
		func(int, string) []byte {
			return yencSinglePart([]byte("must not replay"), "backup.bin")
		},
	)
	backupProvider.Backup = true
	client := newBreakerClient(t, newBreakerFakeClock(), provider, backupProvider)
	bodyResult := make(chan error, 1)
	go func() {
		_, err := client.BodyStream(context.Background(), "pipeline@example.invalid", failingWriter{err: writerErr})
		bodyResult <- err
	}()
	select {
	case <-bodySeen:
	case <-time.After(5 * time.Second):
		t.Fatal("primary BODY did not enter the pipeline")
	}
	statResult := make(chan StatManyResult, 1)
	go func() {
		statResult <- fncoreTargetedStat(context.Background(), client, provider.ID, "collateral@example.invalid")
	}()
	select {
	case <-bothSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("uncommitted collateral STAT did not share the first pipeline")
	}

	var bodyErr error
	select {
	case bodyErr = <-bodyResult:
	case <-time.After(5 * time.Second):
		t.Fatal("local writer failure did not settle")
	}
	requireFNCORELocalWriterError(t, bodyErr, writerErr)
	var stat StatManyResult
	select {
	case stat = <-statResult:
	case <-time.After(5 * time.Second):
		t.Fatal("collateral STAT did not retry and settle")
	}
	if stat.Err != nil || stat.Result == nil {
		t.Fatalf("collateral STAT result = %+v, want own retry success", stat)
	}
	if len(stat.Result.Attempts) != 2 || stat.Result.Attempts[0].Outcome != OutcomeTransportFailure ||
		stat.Result.Attempts[1].Outcome != OutcomeSuccess {
		t.Fatalf("collateral STAT attempts = %+v, want connection loss then own success", stat.Result.Attempts)
	}
	if !errors.Is(stat.Result.Attempts[0].Cause, ErrConnectionDied) {
		t.Fatalf("collateral first cause = %v, want retired pipeline connection", stat.Result.Attempts[0].Cause)
	}
	for index, attempt := range stat.Result.Attempts {
		if attempt.Operation != OperationStat || attempt.ProviderID != provider.ID {
			t.Fatalf("collateral attempt %d = %+v, want factual primary STAT evidence", index+1, attempt)
		}
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Fatalf("backup BODY commands = %d, want no local-writer replay", got)
	}
	recorded := commands.snapshot()
	if len(recorded) != 3 {
		t.Fatalf("pipeline commands = %+v, want BODY+STAT on first socket and STAT retry", recorded)
	}
	if recorded[0].connection != 1 || !strings.HasPrefix(recorded[0].command, "BODY ") ||
		recorded[1].connection != 1 || !strings.HasPrefix(recorded[1].command, "STAT ") ||
		recorded[2].connection != 2 || !strings.HasPrefix(recorded[2].command, "STAT ") {
		t.Fatalf("pipeline command ownership = %+v, want [1:BODY 1:STAT 2:STAT]", recorded)
	}
	group := fncoreProviderGroup(t, client, provider.ID)
	if got := group.stats.Errors.Load(); got != 0 {
		t.Fatalf("provider errors after local/collateral settlement = %d, want zero", got)
	}
	stats := group.breaker.snapshot()
	if stats.State != CircuitBreakerClosed || stats.ProbeInFlight || stats.QualifyingFailures != 0 {
		t.Fatalf("local writer or collateral loss changed breaker: %+v", stats)
	}
}
