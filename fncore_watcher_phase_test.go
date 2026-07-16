package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func awaitFNCOREPhaseResponse(t *testing.T, responseCh <-chan Response, label string) Response {
	t.Helper()
	select {
	case response := <-responseCh:
		return response
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s response", label)
		return Response{}
	}
}

func TestFNCORECompletedRequestCancellationCannotRetireReusedSocket(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	secondCommandSeen := make(chan struct{})
	releaseSecondResponse := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		reader := bufio.NewReader(serverConn)
		if _, err := serverConn.Write([]byte("200 regression server ready\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 1 <first@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		close(secondCommandSeen)
		<-releaseSecondResponse
		if _, err := serverConn.Write([]byte("223 2 <second@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 3 <third@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), clientConn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	go connection.Run()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := &Request{
		Ctx:         firstCtx,
		Payload:     []byte("STAT <first@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- first
	if response := awaitFNCOREPhaseResponse(t, first.RespCh, "first"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("first response = %+v, want success", response)
	}

	second := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <second@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- second
	select {
	case <-secondCommandSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("second request was not visibly running on the reused socket")
	}
	cancelFirst()
	close(releaseSecondResponse)
	if response := awaitFNCOREPhaseResponse(t, second.RespCh, "second"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("second response after old-context cancellation = %+v, want success", response)
	}

	third := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <third@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- third
	if response := awaitFNCOREPhaseResponse(t, third.RespCh, "third"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("third response after safe socket reuse = %+v, want success", response)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("same-socket server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("same-socket server did not complete all three requests")
	}
	select {
	case <-connection.Done():
		t.Fatal("old completed-request cancellation retired the healthy reused socket")
	default:
	}
}

func TestFNCOREPostCancellationWhileAwaitingSilentContinueSettles(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	postCommandSeen := make(chan struct{})
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
		close(postCommandSeen)
		_, err := reader.ReadByte()
		serverObservedClose <- err
	}()

	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), clientConn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	body := newFNCOREBlockedReadCloser()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = body.Close()
		_ = serverConn.Close()
		_ = connection.Close()
	})
	req := &Request{
		Ctx:         ctx,
		Payload:     []byte("POST\r\n"),
		PayloadBody: body,
		PostMode:    true,
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	go connection.Run()
	reqCh <- req
	select {
	case <-postCommandSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("POST command did not reach the silent server")
	}
	cancel()
	select {
	case <-body.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation awaiting 340 did not close the owned POST body")
	}
	select {
	case err := <-serverObservedClose:
		if err == nil {
			t.Fatal("silent 340 server read unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation awaiting 340 did not retire the transport")
	}
	response := awaitFNCOREPhaseResponse(t, req.RespCh, "silent 340 POST")
	if !errors.Is(response.Err, context.Canceled) {
		t.Fatalf("silent 340 POST error = %v, want context cancellation", response.Err)
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("POST cancellation awaiting 340 did not join writer and reader")
	}
	if got := len(connection.inflightSem); got != 0 {
		t.Fatalf("inflight capacity after silent 340 cancellation = %d, want zero", got)
	}
}

func TestFNCOREPostCancellationWhileAwaitingSilentFinalSettles(t *testing.T) {
	article := []byte("From: test@example.invalid\r\n\r\narticle body\r\n.\r\n")
	clientConn, serverConn := net.Pipe()
	bodyFlushed := make(chan struct{})
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
		if _, err := serverConn.Write([]byte("340 send article\r\n")); err != nil {
			serverObservedClose <- err
			return
		}
		received := make([]byte, len(article))
		if _, err := io.ReadFull(reader, received); err != nil {
			serverObservedClose <- err
			return
		}
		if !bytes.Equal(received, article) {
			serverObservedClose <- errors.New("POST body did not match flushed article")
			return
		}
		close(bodyFlushed)
		_, err := reader.ReadByte()
		serverObservedClose <- err
	}()

	reqCh := make(chan *Request, 1)
	connection, err := newNNTPConnectionFromConn(
		context.Background(), clientConn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = connection.Close()
	})
	req := &Request{
		Ctx:         ctx,
		Payload:     []byte("POST\r\n"),
		PayloadBody: bytes.NewReader(article),
		PostMode:    true,
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	go connection.Run()
	reqCh <- req
	select {
	case <-bodyFlushed:
	case <-time.After(5 * time.Second):
		t.Fatal("POST body did not complete and flush after 340")
	}
	cancel()
	select {
	case err := <-serverObservedClose:
		if err == nil {
			t.Fatal("silent final-response server read unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation awaiting final POST response did not retire transport")
	}
	response := awaitFNCOREPhaseResponse(t, req.RespCh, "silent final POST")
	if !errors.Is(response.Err, context.Canceled) {
		t.Fatalf("silent final POST error = %v, want context cancellation", response.Err)
	}
	select {
	case <-connection.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("POST cancellation awaiting final response did not join writer and reader")
	}
	if got := len(connection.inflightSem); got != 0 {
		t.Fatalf("inflight capacity after silent final cancellation = %d, want zero", got)
	}
}

func TestFNCORELaterPipelinedCancellationPreservesEarlierResponseAndReuse(t *testing.T) {
	firstPayload := []byte("healthy first response")
	firstResponse := yencSinglePart(firstPayload, "first.bin")
	clientConn, serverConn := net.Pipe()
	twoCommandsSeen := make(chan struct{})
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
		close(twoCommandsSeen)
		<-releaseResponses
		if _, err := serverConn.Write(firstResponse); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 2 <later@example.invalid> exists\r\n")); err != nil {
			serverDone <- err
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte("223 3 <reuse@example.invalid> exists\r\n")); err != nil {
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
	go connection.Run()

	var streamed bytes.Buffer
	first := &Request{
		Ctx:          context.Background(),
		Payload:      []byte("BODY <first@example.invalid>\r\n"),
		RespCh:       make(chan Response, 1),
		BodyWriter:   &streamed,
		ValidateBody: true,
		submittedAt:  time.Now(),
	}
	laterCtx, cancelLater := context.WithCancel(context.Background())
	later := &Request{
		Ctx:         laterCtx,
		Payload:     []byte("STAT <later@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- first
	reqCh <- later
	select {
	case <-twoCommandsSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive both fully-written pipelined requests")
	}
	cancelLater()
	close(releaseResponses)

	firstResult := awaitFNCOREPhaseResponse(t, first.RespCh, "earlier BODY")
	if firstResult.Err != nil || firstResult.StatusCode != 222 {
		t.Fatalf("earlier BODY after later cancellation = %+v, want success", firstResult)
	}
	if !bytes.Equal(streamed.Bytes(), firstPayload) {
		t.Fatalf("earlier streamed payload = %q, want %q", streamed.Bytes(), firstPayload)
	}
	laterResult := awaitFNCOREPhaseResponse(t, later.RespCh, "cancelled later STAT")
	if !errors.Is(laterResult.Err, context.Canceled) {
		t.Fatalf("later pipelined result error = %v, want context cancellation", laterResult.Err)
	}
	if laterResult.StatusCode != 223 {
		t.Fatalf("later pipelined status = %d, want framing-preserving 223", laterResult.StatusCode)
	}

	reuse := &Request{
		Ctx:         context.Background(),
		Payload:     []byte("STAT <reuse@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	reqCh <- reuse
	if response := awaitFNCOREPhaseResponse(t, reuse.RespCh, "safe reuse"); response.Err != nil || response.StatusCode != 223 {
		t.Fatalf("safe reuse response = %+v, want success", response)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("pipeline-preservation server error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline-preservation server did not complete")
	}
	select {
	case <-connection.Done():
		t.Fatal("settled later cancellation left a stale watcher that retired safe reuse")
	default:
	}
}
