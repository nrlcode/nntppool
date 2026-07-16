package nntppool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type fncoreBlockedTransportWriteConn struct {
	net.Conn
	started     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
	closedOnce  sync.Once
}

func newFNCOREBlockedTransportWriteConn(conn net.Conn) *fncoreBlockedTransportWriteConn {
	return &fncoreBlockedTransportWriteConn{
		Conn:    conn,
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (c *fncoreBlockedTransportWriteConn) Write([]byte) (int, error) {
	c.startedOnce.Do(func() { close(c.started) })
	<-c.release
	return 0, net.ErrClosed
}

func (c *fncoreBlockedTransportWriteConn) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *fncoreBlockedTransportWriteConn) Close() error {
	c.closedOnce.Do(func() { close(c.closed) })
	c.unblock()
	return c.Conn.Close()
}

type fncoreBlockedReadCloser struct {
	started     chan struct{}
	closed      chan struct{}
	startedOnce sync.Once
	closedOnce  sync.Once
}

func newFNCOREBlockedReadCloser() *fncoreBlockedReadCloser {
	return &fncoreBlockedReadCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (r *fncoreBlockedReadCloser) Read([]byte) (int, error) {
	r.startedOnce.Do(func() { close(r.started) })
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *fncoreBlockedReadCloser) Close() error {
	r.closedOnce.Do(func() { close(r.closed) })
	return nil
}

func TestFNCOREBlockedTransportWriteCancellationSettlesOwnership(t *testing.T) {
	for _, mode := range []string{"request cancellation", "provider removal", "client shutdown"} {
		t.Run(mode, func(t *testing.T) {
			connections := make(chan *fncoreBlockedTransportWriteConn, 2)
			factory := func(context.Context) (net.Conn, error) {
				client, server := net.Pipe()
				blocked := newFNCOREBlockedTransportWriteConn(client)
				connections <- blocked
				go func() {
					defer func() { _ = server.Close() }()
					_, _ = server.Write([]byte("200 regression server ready\r\n"))
					_, _ = io.Copy(io.Discard, server)
				}()
				return blocked, nil
			}
			provider := Provider{
				ID:          "blocked-write-" + mode,
				Host:        "blocked-write-" + mode + ".invalid:119",
				Factory:     factory,
				Connections: 1,
				Inflight:    1,
				SkipPing:    true,
			}
			client := fncoreClient(t, provider)
			group := fncoreProviderGroup(t, client, provider.ID)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan StatManyResult, 1)
			go func() {
				result <- fncoreTargetedStat(ctx, client, provider.ID, "blocked-write@example.invalid")
			}()

			var blocked *fncoreBlockedTransportWriteConn
			select {
			case blocked = <-connections:
			case <-time.After(5 * time.Second):
				t.Fatal("provider did not create blocked-write connection")
			}
			defer blocked.unblock()
			select {
			case <-blocked.started:
			case <-time.After(5 * time.Second):
				t.Fatal("request never reached blocked transport Write")
			}

			shutdownDone := make(chan error, 1)
			var want error
			switch mode {
			case "request cancellation":
				cancel()
				want = context.Canceled
				close(shutdownDone)
			case "provider removal":
				want = ErrConnectionDied
				go func() { shutdownDone <- client.RemoveProvider(provider.Host) }()
			case "client shutdown":
				want = context.Canceled
				go func() { shutdownDone <- client.Close() }()
			}

			select {
			case <-blocked.closed:
			case <-time.After(2 * time.Second):
				blocked.unblock()
				t.Fatal("lifecycle cancellation did not close blocked transport")
			}
			select {
			case stat := <-result:
				if !errors.Is(stat.Err, want) {
					t.Fatalf("blocked-write result error = %v, want %v", stat.Err, want)
				}
			case <-time.After(2 * time.Second):
				blocked.unblock()
				t.Fatal("transport-owned request did not settle after blocked Write cancellation")
			}
			select {
			case shutdownErr, ok := <-shutdownDone:
				if ok && shutdownErr != nil {
					t.Fatalf("%s error = %v", mode, shutdownErr)
				}
			case <-time.After(2 * time.Second):
				blocked.unblock()
				t.Fatalf("%s did not finish after blocked Write cancellation", mode)
			}
			if got := group.stats.PipelineInUse.Load(); got != 0 {
				t.Fatalf("pipeline occupancy after settlement = %d, want zero", got)
			}
			waitForFNCOREGateAvailability(t, group, int32(provider.Connections))
		})
	}
}

func TestFNCOREBlockedClosablePayloadBodyIsCancelled(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		defer func() { _ = serverConn.Close() }()
		_, _ = serverConn.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		_, _ = serverConn.Write([]byte("340 send article\r\n"))
		_, _ = io.Copy(io.Discard, reader)
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
	case <-body.started:
	case <-time.After(5 * time.Second):
		_ = body.Close()
		_ = connection.Close()
		t.Fatal("POST payload body did not reach blocked Read")
	}
	cancel()
	select {
	case <-body.closed:
	case <-time.After(2 * time.Second):
		_ = body.Close()
		_ = connection.Close()
		t.Fatal("request cancellation did not close the owned PayloadBody source")
	}
	select {
	case response := <-req.RespCh:
		if !errors.Is(response.Err, context.Canceled) {
			t.Fatalf("blocked PayloadBody response error = %v, want context cancellation", response.Err)
		}
	case <-time.After(2 * time.Second):
		_ = body.Close()
		_ = connection.Close()
		t.Fatal("blocked PayloadBody request did not settle after cancellation")
	}
	select {
	case <-connection.Done():
	case <-time.After(2 * time.Second):
		_ = connection.Close()
		t.Fatal("connection runner returned before blocked PayloadBody ownership settled")
	}
	if got := len(connection.inflightSem); got != 0 {
		t.Fatalf("inflight capacity after blocked PayloadBody settlement = %d, want zero", got)
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe retired POST connection")
	}
}

func TestFNCOREBlockedResponseReadCancellationSettlesOwnership(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	commandSeen := make(chan struct{})
	serverObservedClose := make(chan error, 1)
	go func() {
		defer func() { _ = serverConn.Close() }()
		_, _ = serverConn.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadString('\n'); err != nil {
			serverObservedClose <- err
			return
		}
		close(commandSeen)
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
	req := &Request{
		Ctx:         ctx,
		Payload:     []byte("STAT <silent-response@example.invalid>\r\n"),
		RespCh:      make(chan Response, 1),
		submittedAt: time.Now(),
	}
	go connection.Run()
	reqCh <- req
	select {
	case <-commandSeen:
	case <-time.After(5 * time.Second):
		_ = serverConn.Close()
		_ = connection.Close()
		t.Fatal("server did not receive the request before cancellation")
	}

	cancel()
	select {
	case err := <-serverObservedClose:
		if err == nil {
			t.Fatal("server read unexpectedly succeeded after request cancellation")
		}
	case <-time.After(2 * time.Second):
		_ = serverConn.Close()
		_ = connection.Close()
		t.Fatal("request cancellation did not interrupt the blocked response read")
	}
	select {
	case response := <-req.RespCh:
		if !errors.Is(response.Err, context.Canceled) {
			t.Fatalf("blocked response read error = %v, want context cancellation", response.Err)
		}
	case <-time.After(2 * time.Second):
		_ = serverConn.Close()
		_ = connection.Close()
		t.Fatal("transport-owned request did not settle after blocked response cancellation")
	}
	select {
	case <-connection.Done():
	case <-time.After(2 * time.Second):
		_ = serverConn.Close()
		_ = connection.Close()
		t.Fatal("connection remained reusable after blocked response cancellation")
	}
	if got := len(connection.inflightSem); got != 0 {
		t.Fatalf("inflight capacity after blocked response settlement = %d, want zero", got)
	}
}
