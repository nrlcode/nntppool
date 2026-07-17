package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fncoreLateDeadlineContext struct {
	deadline time.Time
	expired  atomic.Bool
}

func (c *fncoreLateDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (*fncoreLateDeadlineContext) Done() <-chan struct{}         { return nil }
func (c *fncoreLateDeadlineContext) Err() error {
	if c.expired.Load() {
		return context.DeadlineExceeded
	}
	return nil
}
func (*fncoreLateDeadlineContext) Value(any) any { return nil }

type fncoreProviderOwnedTimeoutConn struct {
	net.Conn
	timeoutRead int32
	reads       atomic.Int32

	deadlineMu sync.Mutex
	deadline   time.Time

	selected     chan struct{}
	release      chan struct{}
	selectedOnce sync.Once
	expire       func()
}

func (c *fncoreProviderOwnedTimeoutConn) SetReadDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.deadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *fncoreProviderOwnedTimeoutConn) Read(p []byte) (int, error) {
	c.deadlineMu.Lock()
	armed := !c.deadline.IsZero()
	c.deadlineMu.Unlock()
	if !armed {
		return c.Conn.Read(p)
	}
	if c.reads.Add(1) < c.timeoutRead {
		return c.Conn.Read(p)
	}
	c.selectedOnce.Do(func() { close(c.selected) })
	<-c.release
	c.expire()
	return 0, fncoreDeadlineTimeout{}
}

func (c *fncoreProviderOwnedTimeoutConn) selectedDeadline() time.Time {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	return c.deadline
}

func TestFNCOREProviderOwnedTimeoutSurvivesLateCallerDeadline(t *testing.T) {
	for _, test := range []struct {
		name                    string
		partial                 string
		timeoutRead             int32
		providerResponseTimeout bool
	}{
		{name: "response head", timeoutRead: 1, providerResponseTimeout: true},
		{name: "response stall", partial: "223 partial response", timeoutRead: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			caller := &fncoreLateDeadlineContext{deadline: time.Now().Add(time.Hour)}
			clientConn, serverConn := net.Pipe()
			controlled := &fncoreProviderOwnedTimeoutConn{
				Conn:        clientConn,
				timeoutRead: test.timeoutRead,
				selected:    make(chan struct{}),
				release:     make(chan struct{}),
				expire:      func() { caller.expired.Store(true) },
			}
			go func() {
				defer func() { _ = serverConn.Close() }()
				_, _ = serverConn.Write([]byte("200 regression server ready\r\n"))
				reader := bufio.NewReader(serverConn)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				if test.partial != "" {
					_, _ = serverConn.Write([]byte(test.partial))
				}
				_, _ = io.Copy(io.Discard, reader)
			}()
			stats := &providerStats{}
			reqCh := make(chan *Request, 1)
			connection, err := newNNTPConnectionFromConn(
				context.Background(), controlled, 1, reqCh, nil, Auth{}, "", nil, stats,
			)
			if err != nil {
				t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
			}
			providerID := "late-" + test.name
			connection.providerID = providerID
			connection.stallTimeout = 10 * time.Second
			t.Cleanup(func() {
				_ = serverConn.Close()
				_ = connection.Close()
			})
			request := &Request{
				Ctx:             caller,
				Payload:         []byte("STAT <late-deadline@example.invalid>\r\n"),
				RespCh:          make(chan Response, 1),
				submittedAt:     time.Now(),
				responseTimeout: 10 * time.Second,
			}
			go connection.Run()
			reqCh <- request
			select {
			case <-controlled.selected:
			case <-time.After(5 * time.Second):
				t.Fatal("provider-owned socket deadline was not selected")
			}
			if deadline := controlled.selectedDeadline(); deadline.IsZero() || !deadline.Before(caller.deadline) {
				t.Fatalf("selected socket deadline = %v, want provider deadline before caller %v", deadline, caller.deadline)
			}
			close(controlled.release)

			response := awaitFNCOREPhaseResponse(t, request.RespCh, "provider-owned timeout")
			if len(response.Attempts) != 1 {
				t.Fatalf("late caller deadline attempts = %+v, want one", response.Attempts)
			}
			attempt := response.Attempts[0]
			if attempt.Outcome != OutcomeTransportFailure ||
				attempt.ProviderResponseTimeout != test.providerResponseTimeout {
				t.Fatalf("provider-owned timeout evidence = %+v", attempt)
			}
			if errors.Is(attempt.Cause, context.DeadlineExceeded) {
				t.Fatalf("provider-owned timeout cause was replaced by late caller deadline: %v", attempt.Cause)
			}
			var networkError net.Error
			if !errors.As(attempt.Cause, &networkError) || !networkError.Timeout() {
				t.Fatalf("provider-owned timeout cause = %v, want socket timeout", attempt.Cause)
			}
			if got := stats.Errors.Load(); got != 1 {
				t.Fatalf("provider error metric = %d, want one factual timeout", got)
			}
			if completion := classifyCircuitBreakerCompletion(response, true, false); completion != circuitBreakerFailure {
				t.Fatalf("provider-owned timeout breaker completion = %v, want failure", completion)
			}
			breaker := newProviderCircuitBreaker(true, newBreakerFakeClock())
			lease, err := breaker.acquire(providerID)
			if err != nil {
				t.Fatalf("breaker acquire error = %v", err)
			}
			breaker.complete(lease, classifyCircuitBreakerCompletion(response, true, false))
			if snapshot := breaker.snapshot(); snapshot.State != CircuitBreakerClosed || snapshot.QualifyingFailures != 1 {
				t.Fatalf("provider-owned timeout breaker accounting = %+v", snapshot)
			}
		})
	}
}

type fncoreGatedPostBody struct {
	data []byte
	off  int

	started     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	startedOnce sync.Once
	closedOnce  sync.Once
}

func newFNCOREGatedPostBody(data []byte) *fncoreGatedPostBody {
	return &fncoreGatedPostBody{
		data:    data,
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (b *fncoreGatedPostBody) Read(p []byte) (int, error) {
	b.startedOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-b.closed:
		return 0, io.ErrClosedPipe
	}
	if b.off == len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func (b *fncoreGatedPostBody) Close() error {
	b.closedOnce.Do(func() { close(b.closed) })
	return nil
}

func TestFNCOREHighLevelPostUsesProviderResponseAndStallBounds(t *testing.T) {
	const timeout = 40 * time.Millisecond
	article := []byte("From: test@example.invalid\r\n\r\narticle body\r\n.\r\n")
	for _, test := range []struct {
		name                    string
		initialResponse         string
		partialFinal            string
		gateUpload              bool
		providerResponseTimeout bool
	}{
		{name: "silent initial response", providerResponseTimeout: true},
		{name: "silent final response", initialResponse: "340 send article\r\n", gateUpload: true, providerResponseTimeout: true},
		{name: "partial final response stalls", initialResponse: "340 send article\r\n", partialFinal: "24", gateUpload: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			commandSeen := make(chan struct{})
			bodySeen := make(chan struct{})
			transportClosed := make(chan error, 1)
			factory := func(context.Context) (net.Conn, error) {
				clientConn, serverConn := net.Pipe()
				go func() {
					defer func() { _ = serverConn.Close() }()
					if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
						transportClosed <- err
						return
					}
					reader := bufio.NewReader(serverConn)
					if _, err := reader.ReadString('\n'); err != nil {
						transportClosed <- err
						return
					}
					close(commandSeen)
					if test.initialResponse != "" {
						if _, err := serverConn.Write([]byte(test.initialResponse)); err != nil {
							transportClosed <- err
							return
						}
						var received bytes.Buffer
						for !bytes.Contains(received.Bytes(), []byte("\r\n.\r\n")) {
							chunk := make([]byte, 1024)
							n, err := reader.Read(chunk)
							if n > 0 {
								received.Write(chunk[:n])
							}
							if err != nil {
								transportClosed <- err
								return
							}
						}
						close(bodySeen)
						if test.partialFinal != "" {
							if _, err := serverConn.Write([]byte(test.partialFinal)); err != nil {
								transportClosed <- err
								return
							}
						}
					}
					_, err := reader.ReadByte()
					transportClosed <- err
				}()
				return clientConn, nil
			}
			provider := Provider{
				ID:             "post-timeout-" + test.name,
				Host:           "post-timeout-" + test.name + ".invalid:119",
				Factory:        factory,
				Connections:    1,
				Inflight:       1,
				SkipPing:       true,
				AttemptTimeout: timeout,
				StallTimeout:   timeout,
			}
			client := fncoreClient(t, provider)
			var body io.Reader = bytes.NewReader(article)
			var gated *fncoreGatedPostBody
			if test.gateUpload {
				gated = newFNCOREGatedPostBody(article)
				body = gated
				t.Cleanup(func() { _ = gated.Close() })
			}
			responseCh := client.sendPost(context.Background(), body)
			select {
			case <-commandSeen:
			case <-time.After(5 * time.Second):
				t.Fatal("high-level POST command did not reach provider")
			}
			if gated != nil {
				select {
				case <-gated.started:
				case <-time.After(5 * time.Second):
					t.Fatal("POST body upload did not reach controlled source")
				}
				select {
				case response := <-responseCh:
					t.Fatalf("POST final-response clock consumed upload time: %+v", response)
				case <-time.After(3 * timeout):
				}
				close(gated.release)
				select {
				case <-bodySeen:
				case <-time.After(5 * time.Second):
					t.Fatal("POST body was not completely flushed")
				}
			}

			var response Response
			select {
			case response = <-responseCh:
			case <-time.After(750 * time.Millisecond):
				_ = client.Close()
				t.Fatal("background-context POST did not terminate under provider timeout")
			}
			if response.Err == nil || len(response.Attempts) != 1 {
				t.Fatalf("provider-timeout POST response = %+v, want one failed attempt", response)
			}
			attempt := response.Attempts[0]
			if attempt.Operation != OperationPost || attempt.Outcome != OutcomeTransportFailure ||
				attempt.ProviderResponseTimeout != test.providerResponseTimeout {
				t.Fatalf("provider-timeout POST evidence = %+v", attempt)
			}
			var networkError net.Error
			if !errors.As(attempt.Cause, &networkError) || !networkError.Timeout() {
				t.Fatalf("provider-timeout POST cause = %v, want socket timeout", attempt.Cause)
			}
			select {
			case closeErr := <-transportClosed:
				if closeErr == nil {
					t.Fatal("silent POST provider observed an unexpected byte instead of retirement")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("provider-timeout POST did not retire its socket")
			}
			group := fncoreProviderGroup(t, client, provider.ID)
			if got := group.stats.Errors.Load(); got != 1 {
				t.Fatalf("POST provider error metric = %d, want one", got)
			}
		})
	}
}

func TestFNCOREFullyWrittenPendingPostCancellationClosesBodyAndPreservesFIFO(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	commandsSeen := make(chan struct{})
	releaseResponses := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		close(commandsSeen)
		<-releaseResponses
		if _, err := serverConn.Write([]byte("223 1 <earlier@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("440 posting not permitted\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 2 <reuse@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	reqCh := make(chan *Request, 2)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), clientConn, 2, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	body := newFNCOREBlockedReadCloser()
	t.Cleanup(func() { _ = body.Close() })
	earlier := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <earlier@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	postCtx, cancelPost := context.WithCancel(context.Background())
	defer cancelPost()
	post := &Request{
		Ctx:         postCtx,
		Payload:     []byte("POST\r\n"),
		PayloadBody: body,
		PostMode:    true,
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	go connection.Run()
	reqCh <- earlier
	reqCh <- post
	select {
	case <-commandsSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the fully-written STAT and POST pipeline")
	}
	writtenDeadline := time.After(5 * time.Second)
	for post.writtenAt.Load() == 0 {
		select {
		case <-writtenDeadline:
			t.Fatal("FIFO-pending POST was not marked written")
		default:
			runtime.Gosched()
		}
	}
	cancelPost()
	closedPromptly := false
	select {
	case <-body.closed:
		closedPromptly = true
	case <-time.After(250 * time.Millisecond):
		// Release the fixture so the remainder of the framing proof cannot hang.
		_ = body.Close()
	}
	close(releaseResponses)

	if response := awaitFNCOREPhaseResponse(t, earlier.RespCh, "earlier STAT"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("earlier response after pending POST cancellation = %+v", response)
	}
	postResponse := awaitFNCOREPhaseResponse(t, post.RespCh, "cancelled FIFO-pending POST")
	if !errors.Is(postResponse.Err, context.Canceled) || postResponse.StatusCode != 440 {
		t.Fatalf("cancelled FIFO-pending POST response = %+v", postResponse)
	}
	reuse := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <reuse@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- reuse
	if response := awaitFNCOREPhaseResponse(t, reuse.RespCh, "post-cancellation reuse"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("same-socket response after pending POST cancellation = %+v", response)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("pending POST FIFO server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending POST FIFO server did not complete")
	}
	if !closedPromptly {
		t.Error("cancelled FIFO-pending POST did not close its closable body before its 440 response")
	}
}

var errFNCOREDrainDeadlineClear = errors.New("deterministic drain deadline clear failure")

type fncoreFailDrainDeadlineClearConn struct {
	net.Conn
	armed       atomic.Bool
	clearFailed chan struct{}
	clearOnce   sync.Once
}

func (c *fncoreFailDrainDeadlineClearConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.armed.Load() {
		c.clearOnce.Do(func() { close(c.clearFailed) })
		return errFNCOREDrainDeadlineClear
	}
	if !deadline.IsZero() {
		c.armed.Store(true)
	}
	return c.Conn.SetReadDeadline(deadline)
}

func TestFNCOREFailedDrainDeadlineClearRetiresTransport(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 8*1024)
	response := yencSinglePart(payload, "clear-failure.bin")
	split := len(response) / 2
	clientConn, serverConn := net.Pipe()
	controlled := &fncoreFailDrainDeadlineClearConn{
		Conn:        clientConn,
		clearFailed: make(chan struct{}),
	}
	releaseTail := make(chan struct{})
	serverObservedClose := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
			serverObservedClose <- err
			return
		}
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverObservedClose <- err
			return
		}
		if _, err := serverConn.Write(response[:split]); err != nil {
			serverObservedClose <- err
			return
		}
		<-releaseTail
		if _, err := serverConn.Write(response[split:]); err != nil {
			serverObservedClose <- err
			return
		}
		_, err := reader.ReadByte()
		serverObservedClose <- err
	}()

	stats := &providerStats{}
	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), controlled, 1, reqCh, nil, Auth{}, "", nil, stats,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	writer := &fncoreProgressSignalWriter{progress: make(chan struct{})}
	request := &Request{
		Ctx:          ctx,
		Payload:      []byte("BODY <clear-failure@example.invalid>\r\n"),
		RespCh:       make(chan Response, 1),
		BodyWriter:   writer,
		ValidateBody: true,
		submittedAt:  time.Now(),
	}
	go connection.Run()
	reqCh <- request
	select {
	case <-writer.progress:
	case <-time.After(5 * time.Second):
		t.Fatal("BODY did not cross the decoded progress boundary")
	}
	cancel()
	close(releaseTail)
	result := awaitFNCOREPhaseResponse(t, request.RespCh, "bounded drain with failed deadline clear")
	if !errors.Is(result.Err, context.Canceled) || result.StatusCode != 222 ||
		len(result.Attempts) != 1 || result.Attempts[0].Outcome != OutcomeCancellation {
		t.Fatalf("bounded drain response = %+v, want neutral complete cancellation", result)
	}
	if got := stats.Errors.Load(); got != 0 {
		t.Fatalf("failed deadline-clear cancellation provider errors = %d, want zero", got)
	}
	select {
	case <-controlled.clearFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("transport did not attempt to clear the watcher-installed deadline")
	}
	retired := false
	select {
	case <-connection.Done():
		retired = true
	case <-time.After(300 * time.Millisecond):
	}
	if !retired {
		t.Error("transport was left reusable after its cancellation-drain deadline could not be cleared")
		_ = connection.Close()
	}
	select {
	case closeErr := <-serverObservedClose:
		if closeErr == nil {
			t.Fatal("deadline-clear server observed unexpected reuse data")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed deadline clear did not close the poisoned socket")
	}
}
