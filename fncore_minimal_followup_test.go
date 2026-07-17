package nntppool

import (
	"bytes"
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

type fncoreFollowupRead struct {
	data []byte
	err  error
}

// fncoreFollowupConn supplies one result per Read and records the ordering of
// transport retirement and selected writes. A blocked Close makes it possible
// to prove that retirement starts before response delivery without timing.
type fncoreFollowupConn struct {
	reads chan fncoreFollowupRead

	closed       chan struct{}
	closeEntered chan struct{}
	closeRelease chan struct{}
	blockClose   bool
	closeOnce    sync.Once
	enteredOnce  sync.Once
	releaseOnce  sync.Once
	closeCalls   atomic.Int32

	writeNeedle []byte
	writeSeen   chan struct{}
	writeOnce   sync.Once
}

func newFNCOREFollowupConn(blockClose bool, reads ...fncoreFollowupRead) *fncoreFollowupConn {
	c := &fncoreFollowupConn{
		reads:        make(chan fncoreFollowupRead, len(reads)),
		closed:       make(chan struct{}),
		closeEntered: make(chan struct{}),
		closeRelease: make(chan struct{}),
		blockClose:   blockClose,
		writeSeen:    make(chan struct{}),
	}
	for _, result := range reads {
		c.reads <- result
	}
	return c
}

func (c *fncoreFollowupConn) Read(p []byte) (int, error) {
	select {
	case result := <-c.reads:
		if len(result.data) > len(p) {
			return 0, io.ErrShortBuffer
		}
		return copy(p, result.data), result.err
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *fncoreFollowupConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	if len(c.writeNeedle) > 0 && bytes.Contains(p, c.writeNeedle) {
		c.writeOnce.Do(func() { close(c.writeSeen) })
	}
	return len(p), nil
}

func (c *fncoreFollowupConn) Close() error {
	c.closeCalls.Add(1)
	c.enteredOnce.Do(func() { close(c.closeEntered) })
	if c.blockClose {
		<-c.closeRelease
	}
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *fncoreFollowupConn) releaseClose() {
	c.releaseOnce.Do(func() { close(c.closeRelease) })
}

func (*fncoreFollowupConn) LocalAddr() net.Addr              { return nil }
func (*fncoreFollowupConn) RemoteAddr() net.Addr             { return nil }
func (*fncoreFollowupConn) SetDeadline(time.Time) error      { return nil }
func (*fncoreFollowupConn) SetReadDeadline(time.Time) error  { return nil }
func (*fncoreFollowupConn) SetWriteDeadline(time.Time) error { return nil }

func fncoreFollowupConnection(t *testing.T, conn net.Conn, inflight int, reqCh <-chan *Request) *NNTPConnection {
	t.Helper()
	connection, err := newNNTPConnectionFromConn(
		context.Background(), conn, inflight, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	connection.providerID = "fncore-followup-provider"
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func fncoreReceiveAfterRetirementStarts(
	t *testing.T,
	connection *NNTPConnection,
	conn *fncoreFollowupConn,
	responseCh <-chan Response,
	label string,
) (Response, bool) {
	t.Helper()
	defer conn.releaseClose()
	select {
	case <-conn.closeEntered:
		select {
		case response := <-responseCh:
			return response, false
		default:
		}
		if got := len(connection.inflightSem); got != 1 {
			t.Errorf("%s inflight occupancy while Close is blocked = %d, want 1", label, got)
		}
		conn.releaseClose()
		return awaitFNCOREPhaseResponse(t, responseCh, label), true
	case response, ok := <-responseCh:
		if !ok {
			t.Fatalf("%s response channel closed without a response", label)
		}
		return response, false
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s retirement or response", label)
		return Response{}, false
	}
}

func TestFNCOREConcurrentFinalizeWaitsForTheSharedWatcherJoin(t *testing.T) {
	firstCause := errors.New("first final cause")
	secondCause := errors.New("later final cause")
	request := &Request{
		Ctx:       context.Background(),
		phase:     requestResponseActive,
		watchStop: make(chan struct{}),
		watchDone: make(chan struct{}),
	}
	var releaseWatch sync.Once
	releaseWatcher := func() { releaseWatch.Do(func() { close(request.watchDone) }) }
	t.Cleanup(releaseWatcher)

	firstResult := make(chan error, 1)
	go func() { firstResult <- request.finalize(firstCause) }()
	select {
	case <-request.watchStop:
	case <-time.After(5 * time.Second):
		t.Fatal("first finalize caller did not enter watcher join")
	}

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondResult <- request.finalize(secondCause)
	}()
	<-secondStarted
	select {
	case result := <-secondResult:
		releaseWatcher()
		<-firstResult
		t.Fatalf("concurrent finalize returned %v before the watcher joined", result)
	case <-time.After(100 * time.Millisecond):
	}

	releaseWatcher()
	if result := <-firstResult; !errors.Is(result, firstCause) {
		t.Fatalf("first finalize result = %v, want %v", result, firstCause)
	}
	if result := <-secondResult; !errors.Is(result, firstCause) {
		t.Fatalf("concurrent finalize result = %v, want preserved first cause %v", result, firstCause)
	}
}

func TestFNCOREPostTerminalFinalRetiresBeforeDeliveryAndCollateral(t *testing.T) {
	for _, test := range []struct {
		name        string
		final       fncoreFollowupRead
		wantCode    int
		wantReadErr error
	}{
		{name: "EOF", final: fncoreFollowupRead{err: io.EOF}, wantReadErr: io.EOF},
		{name: "unexpected EOF", final: fncoreFollowupRead{err: io.ErrUnexpectedEOF}, wantReadErr: io.ErrUnexpectedEOF},
		{name: "451", final: fncoreFollowupRead{data: []byte("451 temporary failure\r\n")}, wantCode: 451},
		{name: "502", final: fncoreFollowupRead{data: []byte("502 service unavailable\r\n")}, wantCode: 502},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := newFNCOREFollowupConn(true,
				fncoreFollowupRead{data: []byte("200 POST server ready\r\n")},
				fncoreFollowupRead{data: []byte("340 send article\r\n")},
				test.final,
			)
			conn.writeNeedle = []byte("STAT <collateral@example.invalid>")
			t.Cleanup(conn.releaseClose)
			reqCh := make(chan *Request, 2)
			connection := fncoreFollowupConnection(t, conn, 2, reqCh)
			post := &Request{
				Ctx:         context.Background(),
				Payload:     []byte("POST\r\n"),
				PayloadBody: io.NopCloser(strings.NewReader("article\r\n.\r\n")),
				PostMode:    true,
				RespCh:      make(chan Response, 1),
				submittedAt: time.Now(),
			}
			go connection.Run()
			reqCh <- post
			response, retiredFirst := fncoreReceiveAfterRetirementStarts(t, connection, conn, post.RespCh, test.name+" POST final")

			if test.wantReadErr != nil {
				if !errors.Is(response.Err, test.wantReadErr) {
					t.Errorf("%s POST error = %v, want %v", test.name, response.Err, test.wantReadErr)
				}
			} else if response.Err != nil || response.StatusCode != test.wantCode {
				t.Errorf("%s POST response = %+v, want raw status %d", test.name, response, test.wantCode)
			}

			collateral := &Request{
				Ctx:         context.Background(),
				Payload:     []byte("STAT <collateral@example.invalid>\r\n"),
				RespCh:      make(chan Response, 1),
				submittedAt: time.Now(),
			}
			reqCh <- collateral
			collateralWritten := false
			select {
			case <-conn.writeSeen:
				collateralWritten = true
			case <-connection.Done():
			case <-time.After(2 * time.Second):
				t.Error("terminal POST neither retired nor accepted the collateral request")
			}
			if !retiredFirst {
				t.Error("terminal POST response was delivered before transport retirement began")
			}
			if collateralWritten {
				t.Error("terminal POST socket accepted collateral work")
			}
		})
	}
}

func TestFNCORECompleteResponseWithReadEOFIsSuccessfulButRetiresFirst(t *testing.T) {
	for _, readErr := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(readErr.Error(), func(t *testing.T) {
			conn := newFNCOREFollowupConn(true,
				fncoreFollowupRead{data: []byte("200 response server ready\r\n")},
				fncoreFollowupRead{
					data: []byte("223 1 <complete@example.invalid> exists\r\n"),
					err:  readErr,
				},
			)
			t.Cleanup(conn.releaseClose)
			reqCh := make(chan *Request, 1)
			connection := fncoreFollowupConnection(t, conn, 1, reqCh)
			request := &Request{
				Ctx:         context.Background(),
				Payload:     []byte("STAT <complete@example.invalid>\r\n"),
				RespCh:      make(chan Response, 1),
				submittedAt: time.Now(),
			}
			go connection.Run()
			reqCh <- request
			response, retiredFirst := fncoreReceiveAfterRetirementStarts(t, connection, conn, request.RespCh, readErr.Error()+" complete response")
			if response.Err != nil || response.StatusCode != 223 {
				t.Errorf("complete response with %v = %+v, want successful 223", readErr, response)
			}
			if !retiredFirst {
				t.Error("complete response was delivered before its n>0 read error began transport retirement")
			}
		})
	}
}

type fncoreBlockingCloseBody struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (b *fncoreBlockingCloseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *fncoreBlockingCloseBody) Close() error {
	b.enteredOnce.Do(func() { close(b.entered) })
	<-b.release
	return nil
}
func (b *fncoreBlockingCloseBody) releaseClose() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func TestFNCORECancelOwnedPublishesCauseAndRetiresBeforeBodyCloseReturns(t *testing.T) {
	conn := newFNCOREFollowupConn(false)
	ctx, cancel := context.WithCancel(context.Background())
	connection := &NNTPConnection{conn: conn, ctx: ctx, cancel: cancel}
	body := &fncoreBlockingCloseBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(body.releaseClose)
	request := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("POST\r\n"),
		PayloadBody: body,
		phase:       requestOwnedWriting,
	}
	cause := errors.New("owned request cancellation")
	done := make(chan struct{})
	go func() {
		request.cancelOwned(connection, cause)
		close(done)
	}()

	select {
	case <-body.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("PayloadBody.Close did not reach its deterministic barrier")
	}
	request.lifecycleMu.Lock()
	published := errors.Is(request.lifecycleCause, cause)
	request.lifecycleMu.Unlock()
	retired := false
	select {
	case <-conn.closeEntered:
		retired = true
	default:
	}
	body.releaseClose()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelOwned did not finish after PayloadBody.Close was released")
	}
	if !published {
		t.Error("cancelOwned had not published its cause when PayloadBody.Close blocked")
	}
	if !retired {
		t.Error("cancelOwned had not begun transport Close when PayloadBody.Close blocked")
	}
}

func TestFNCOREBadKeepalivePhysicallyClosesTransportOnce(t *testing.T) {
	conn := newFNCOREFollowupConn(false,
		fncoreFollowupRead{data: []byte("200 keepalive server ready\r\n")},
		fncoreFollowupRead{data: []byte("500 bad keepalive\r\n")},
	)
	conn.writeNeedle = []byte("DATE\r\n")
	reqCh := make(chan *Request)
	connection := fncoreFollowupConnection(t, conn, 1, reqCh)
	connection.keepaliveInterval = time.Nanosecond
	connection.keepaliveCommand = "DATE"
	go connection.Run()
	select {
	case <-conn.writeSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("keepalive command was not written")
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("bad keepalive did not settle the connection")
	}
	if got := conn.closeCalls.Load(); got != 1 {
		t.Fatalf("physical Close calls after bad keepalive = %d, want exactly one", got)
	}
}
