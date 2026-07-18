package nntppool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fncoreAdmissionFIFOProvider struct {
	id          string
	release     chan struct{}
	releaseOnce sync.Once
	commands    chan string
	connections atomic.Int32
}

func newFNCOREAdmissionFIFOProvider(id string) *fncoreAdmissionFIFOProvider {
	return &fncoreAdmissionFIFOProvider{
		id:       id,
		release:  make(chan struct{}),
		commands: make(chan string, 32),
	}
}

func (p *fncoreAdmissionFIFOProvider) provider(inflight, statInflight, backgroundStatInflight, priorityHeadroom, connections int, backup bool) Provider {
	return Provider{
		ID:                     p.id,
		Host:                   p.id + ".invalid:119",
		Factory:                p.factory,
		Connections:            connections,
		Inflight:               inflight,
		StatInflight:           statInflight,
		BackgroundStatInflight: backgroundStatInflight,
		PriorityHeadroom:       priorityHeadroom,
		Backup:                 backup,
		SkipPing:               true,
	}
}

func (p *fncoreAdmissionFIFOProvider) factory(context.Context) (net.Conn, error) {
	p.connections.Add(1)
	client, server := net.Pipe()
	go p.serve(server)
	return client, nil
}

func (p *fncoreAdmissionFIFOProvider) serve(server net.Conn) {
	defer func() { _ = server.Close() }()
	if _, err := server.Write([]byte("200 admission fixture ready\r\n")); err != nil {
		return
	}

	pending := make(chan string, 32)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for command := range pending {
			<-p.release
			var response []byte
			if strings.HasPrefix(command, "BODY ") {
				response = yencSinglePart([]byte(p.id), p.id+".bin")
			} else {
				response = []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			if _, err := server.Write(response); err != nil {
				return
			}
		}
	}()

	reader := bufio.NewReader(server)
	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			close(pending)
			<-writerDone
			return
		}
		command = strings.TrimSpace(command)
		p.commands <- command
		pending <- command
	}
}

func (p *fncoreAdmissionFIFOProvider) unblock() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func fncoreAdmissionFIFOClient(t *testing.T, providers ...Provider) *Client {
	t.Helper()
	client, err := NewClient(
		context.Background(),
		providers,
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func fncoreAdmissionFIFOWaitCommand(t *testing.T, provider *fncoreAdmissionFIFOProvider, prefix string) string {
	t.Helper()
	select {
	case command := <-provider.commands:
		if !strings.HasPrefix(command, prefix) {
			t.Fatalf("%s command = %q, want %s", provider.id, command, prefix)
		}
		return command
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not receive %s", provider.id, prefix)
		return ""
	}
}

func fncoreAdmissionFIFOAwaitErrors(t *testing.T, results <-chan error, count int) {
	t.Helper()
	for range count {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("request error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request did not settle after fixture release")
		}
	}
}

func TestFNCORECHG004FIFOUsesHotEarliestBodyCapacity(t *testing.T) {
	primary := newFNCOREAdmissionFIFOProvider("fncore-admission-hot-primary")
	later := newFNCOREAdmissionFIFOProvider("fncore-admission-hot-later")
	defer primary.unblock()
	defer later.unblock()

	client := fncoreAdmissionFIFOClient(t,
		primary.provider(2, 2, 2, 0, 1, false),
		later.provider(2, 2, 2, 0, 1, false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 2)

	go func() {
		_, err := client.Body(ctx, "first@example.invalid")
		results <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, primary, "BODY <first@example.invalid>")

	go func() {
		_, err := client.Body(ctx, "second@example.invalid")
		results <- err
	}()
	select {
	case command := <-primary.commands:
		if !strings.HasPrefix(command, "BODY <second@example.invalid>") {
			t.Fatalf("second primary command = %q, want second BODY", command)
		}
	case command := <-later.commands:
		t.Fatalf("second BODY spilled to later primary %q while earliest primary retained Inflight capacity", command)
	case <-time.After(2 * time.Second):
		t.Fatal("second BODY reached neither regular primary")
	}

	primary.unblock()
	later.unblock()
	fncoreAdmissionFIFOAwaitErrors(t, results, 2)
	if got := later.connections.Load(); got != 0 {
		t.Errorf("later primary connections = %d, want zero while earliest primary has BODY capacity", got)
	}
}

func TestFNCORECHG004FIFOConcurrentSelectorsRespectBodyReservations(t *testing.T) {
	primary := newFNCOREAdmissionFIFOProvider("fncore-admission-burst-primary")
	later := newFNCOREAdmissionFIFOProvider("fncore-admission-burst-later")
	defer primary.unblock()
	defer later.unblock()

	client := fncoreAdmissionFIFOClient(t,
		primary.provider(2, 2, 2, 0, 1, false),
		later.provider(1, 1, 1, 0, 8, false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 9)

	go func() {
		_, err := client.Body(ctx, "held@example.invalid")
		results <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, primary, "BODY <held@example.invalid>")

	const selectors = 8
	start := make(chan struct{})
	ready := make(chan struct{}, selectors)
	for index := range selectors {
		go func(index int) {
			ready <- struct{}{}
			<-start
			_, err := client.Body(ctx, fmt.Sprintf("burst-%d@example.invalid", index))
			results <- err
		}(index)
	}
	for range selectors {
		<-ready
	}
	close(start)

	primaryCount := 0
	laterCount := 0
	deadline := time.After(2 * time.Second)
	for primaryCount+laterCount < selectors {
		select {
		case command := <-primary.commands:
			if !strings.HasPrefix(command, "BODY <burst-") {
				t.Fatalf("unexpected primary burst command %q", command)
			}
			primaryCount++
		case command := <-later.commands:
			if !strings.HasPrefix(command, "BODY <burst-") {
				t.Fatalf("unexpected later-primary burst command %q", command)
			}
			laterCount++
		case <-deadline:
			t.Fatalf("burst wire commands = primary %d + later %d, want %d without over-reservation", primaryCount, laterCount, selectors)
		}
	}
	if primaryCount != 1 || laterCount != selectors-1 {
		t.Fatalf("burst reservations = primary %d, later %d; want exactly 1 remaining earliest-primary slot and %d later slots", primaryCount, laterCount, selectors-1)
	}

	primary.unblock()
	later.unblock()
	fncoreAdmissionFIFOAwaitErrors(t, results, selectors+1)
}

func TestFNCORECHG004FIFOAdmissionDistinguishesBodyAndStatCapacity(t *testing.T) {
	t.Run("STAT uses spare total pipeline while BODY is full", func(t *testing.T) {
		primary := newFNCOREAdmissionFIFOProvider("fncore-admission-stat-primary")
		later := newFNCOREAdmissionFIFOProvider("fncore-admission-stat-later")
		defer primary.unblock()
		defer later.unblock()

		client := fncoreAdmissionFIFOClient(t,
			primary.provider(1, 2, 2, 0, 1, false),
			later.provider(1, 2, 2, 0, 1, false),
		)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		results := make(chan error, 2)

		go func() {
			_, err := client.Body(ctx, "body-full@example.invalid")
			results <- err
		}()
		fncoreAdmissionFIFOWaitCommand(t, primary, "BODY <body-full@example.invalid>")

		go func() {
			_, err := client.Stat(ctx, "stat-spare@example.invalid")
			results <- err
		}()
		select {
		case command := <-primary.commands:
			if !strings.HasPrefix(command, "STAT <stat-spare@example.invalid>") {
				t.Fatalf("spare primary command = %q, want STAT", command)
			}
		case command := <-later.commands:
			t.Fatalf("STAT spilled to later primary %q despite spare STAT/pipeline capacity", command)
		case <-time.After(2 * time.Second):
			t.Fatal("STAT reached neither regular primary")
		}

		primary.unblock()
		later.unblock()
		fncoreAdmissionFIFOAwaitErrors(t, results, 2)
	})

	t.Run("BODY respects its operation bound", func(t *testing.T) {
		primary := newFNCOREAdmissionFIFOProvider("fncore-admission-body-primary")
		later := newFNCOREAdmissionFIFOProvider("fncore-admission-body-later")
		defer primary.unblock()
		defer later.unblock()

		client := fncoreAdmissionFIFOClient(t,
			primary.provider(1, 2, 2, 0, 1, false),
			later.provider(1, 2, 2, 0, 1, false),
		)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		results := make(chan error, 2)

		go func() {
			_, err := client.Body(ctx, "first-body@example.invalid")
			results <- err
		}()
		fncoreAdmissionFIFOWaitCommand(t, primary, "BODY <first-body@example.invalid>")

		go func() {
			_, err := client.Body(ctx, "bounded-body@example.invalid")
			results <- err
		}()
		command := fncoreAdmissionFIFOWaitCommand(t, later, "BODY <bounded-body@example.invalid>")
		select {
		case extra := <-primary.commands:
			t.Fatalf("BODY operation bound overbooked primary with %q after later received %q", extra, command)
		default:
		}

		primary.unblock()
		later.unblock()
		fncoreAdmissionFIFOAwaitErrors(t, results, 2)
	})
}

func TestFNCORECHG004FIFOPriorityUsesReservedHeadroom(t *testing.T) {
	primary := newFNCOREAdmissionFIFOProvider("fncore-admission-priority-primary")
	later := newFNCOREAdmissionFIFOProvider("fncore-admission-priority-later")
	defer primary.unblock()
	defer later.unblock()

	client := fncoreAdmissionFIFOClient(t,
		primary.provider(1, 2, 1, 1, 1, false),
		later.provider(1, 2, 1, 1, 2, false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 3)

	go func() {
		_, err := client.Stat(ctx, "primary-background@example.invalid")
		results <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, primary, "STAT <primary-background@example.invalid>")

	go func() {
		_, err := client.Stat(ctx, "later-background@example.invalid")
		results <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, later, "STAT <later-background@example.invalid>")

	go func() {
		_, err := client.StatPriority(ctx, "priority-headroom@example.invalid")
		results <- err
	}()
	select {
	case command := <-primary.commands:
		if !strings.HasPrefix(command, "STAT <priority-headroom@example.invalid>") {
			t.Fatalf("primary headroom command = %q, want priority STAT", command)
		}
	case command := <-later.commands:
		t.Fatalf("priority STAT spilled to later primary %q instead of earliest reserved headroom", command)
	case <-time.After(2 * time.Second):
		t.Fatal("priority STAT reached neither regular primary")
	}

	primary.unblock()
	later.unblock()
	fncoreAdmissionFIFOAwaitErrors(t, results, 3)
}

func TestFNCORECHG004FIFOSaturationWaitsWithoutBackupOverflow(t *testing.T) {
	first := newFNCOREAdmissionFIFOProvider("fncore-admission-saturated-first")
	second := newFNCOREAdmissionFIFOProvider("fncore-admission-saturated-second")
	backup := newFNCOREAdmissionFIFOProvider("fncore-admission-saturated-backup")
	defer first.unblock()
	defer second.unblock()
	defer backup.unblock()

	client := fncoreAdmissionFIFOClient(t,
		first.provider(1, 1, 1, 0, 1, false),
		second.provider(1, 1, 1, 0, 1, false),
		backup.provider(1, 1, 1, 0, 1, true),
	)
	heldCtx, heldCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer heldCancel()
	heldResults := make(chan error, 2)

	go func() {
		_, err := client.Body(heldCtx, "saturate-first@example.invalid")
		heldResults <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, first, "BODY <saturate-first@example.invalid>")
	go func() {
		_, err := client.Body(heldCtx, "saturate-second@example.invalid")
		heldResults <- err
	}()
	fncoreAdmissionFIFOWaitCommand(t, second, "BODY <saturate-second@example.invalid>")

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waitResult := make(chan error, 1)
	waitStarted := make(chan struct{})
	go func() {
		close(waitStarted)
		_, err := client.Body(waitCtx, "wait-for-regular@example.invalid")
		waitResult <- err
	}()
	<-waitStarted
	select {
	case err := <-waitResult:
		t.Fatalf("regular-tier saturated request returned before capacity/cancellation: %v", err)
	case command := <-backup.commands:
		t.Fatalf("regular-tier occupancy promoted failure-only backup: %q", command)
	case <-time.After(100 * time.Millisecond):
	}
	if got := backup.connections.Load(); got != 0 {
		t.Fatalf("backup connections during regular-tier saturation = %d, want zero", got)
	}

	cancelWait()
	select {
	case err := <-waitResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("saturated admission error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller cancellation did not terminate saturated admission wait")
	}
	if got := backup.connections.Load(); got != 0 {
		t.Errorf("backup connections after canceled saturation wait = %d, want zero", got)
	}

	first.unblock()
	second.unblock()
	backup.unblock()
	fncoreAdmissionFIFOAwaitErrors(t, heldResults, 2)

	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRecovery()
	recovery, err := client.Body(recoveryCtx, "recovered-capacity@example.invalid")
	if err != nil {
		t.Fatalf("request after saturation release error = %v", err)
	}
	if recovery.ProviderID != first.id {
		t.Errorf("request after saturation release provider = %q, want earliest primary %q", recovery.ProviderID, first.id)
	}
	for _, stats := range client.Stats().Providers {
		if stats.PipelineInUse != 0 || stats.BackgroundStatInUse != 0 {
			t.Errorf("provider %q retained admission occupancy after recovery: pipeline=%d background=%d", stats.ProviderID, stats.PipelineInUse, stats.BackgroundStatInUse)
		}
	}
	if got := backup.connections.Load(); got != 0 {
		t.Errorf("backup connections after regular-tier recovery = %d, want zero", got)
	}
}

func TestFNCORECHG004StoppedGateRejectsReservedRequestBeforeWire(t *testing.T) {
	provider := newFNCOREAdmissionFIFOProvider("fncore-admission-stopped-handoff")
	defer provider.unblock()

	client := fncoreAdmissionFIFOClient(t,
		provider.provider(1, 1, 1, 0, 1, false),
	)
	group := client.registry.Load().mains[0]
	payload := []byte("BODY <stopped-handoff@example.invalid>\r\n")
	lease, rejection, status := client.beginProviderAttempt(
		context.Background(), group, payload, false, true, true, true,
	)
	if status != providerAdmissionGranted {
		t.Fatalf("initial reservation status = %v, response = %+v; want granted", status, rejection)
	}

	// Model the gate side of provider removal after selection but before the
	// admitted runner hands the request to transport.
	group.gate.stop()
	type attemptResult struct {
		resp      Response
		ok        bool
		cancelled bool
	}
	result := make(chan attemptResult, 1)
	go func() {
		resp, ok, cancelled := client.tryGroupResilientAdmitted(
			context.Background(), group, payload, nil, nil, false, true, false, lease,
		)
		result <- attemptResult{resp: resp, ok: ok, cancelled: cancelled}
	}()

	select {
	case command := <-provider.commands:
		provider.unblock()
		select {
		case <-result:
		case <-time.After(2 * time.Second):
			t.Fatalf("stopped wire attempt %q did not settle after release", command)
		}
		t.Fatalf("stopped reservation reached wire as %q", command)
	case got := <-result:
		if got.ok || got.cancelled {
			t.Fatalf("stopped reservation result = %+v, want neutral pretransport rejection", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stopped reservation neither rejected nor reached the wire")
	}

	if got := provider.connections.Load(); got != 0 {
		t.Errorf("stopped reservation connections = %d, want zero", got)
	}
	group.gate.mu.Lock()
	pipeline, body := group.gate.reservedPipeline, group.gate.reservedBody
	group.gate.mu.Unlock()
	if pipeline != 0 || body != 0 {
		t.Errorf("stopped reservation retained capacity: pipeline=%d body=%d", pipeline, body)
	}
}

func TestFNCORECHG004ReservationHandoffRevalidatesThrottleAndStop(t *testing.T) {
	payload := []byte("BODY <reservation-handoff@example.invalid>\r\n")

	t.Run("throttle shrink invalidates only the pending reservation", func(t *testing.T) {
		gate := newConnGate(2, time.Hour)
		t.Cleanup(gate.stop)
		gate.configureRequestCapacity(1, 1, 1, nil)

		first, ok := gate.reserveRequest(payload, false, true)
		if !ok {
			t.Fatal("first reservation was rejected before throttle")
		}
		second, ok := gate.reserveRequest(payload, false, true)
		if !ok {
			first.release()
			t.Fatal("second reservation was rejected before throttle")
		}
		if got := first.handoff(); got != requestHandoffGranted {
			first.release()
			second.release()
			t.Fatalf("first handoff = %v, want granted", got)
		}

		gate.markRunning()
		gate.throttle()
		if got := second.handoff(); got != requestHandoffSaturated {
			first.release()
			second.release()
			t.Fatalf("stale second handoff = %v, want saturated", got)
		}

		gate.mu.Lock()
		pipeline, body := gate.reservedPipeline, gate.reservedBody
		gate.mu.Unlock()
		if pipeline != 1 || body != 1 {
			first.release()
			t.Fatalf("capacity after stale handoff = pipeline %d, body %d; want handed reservation only", pipeline, body)
		}
		second.release()
		gate.mu.Lock()
		pipeline, body = gate.reservedPipeline, gate.reservedBody
		gate.mu.Unlock()
		if pipeline != 1 || body != 1 {
			first.release()
			t.Fatalf("repeated stale release changed capacity: pipeline=%d body=%d", pipeline, body)
		}
		first.release()
		gate.mu.Lock()
		pipeline, body = gate.reservedPipeline, gate.reservedBody
		gate.mu.Unlock()
		if pipeline != 0 || body != 0 {
			t.Fatalf("final capacity = pipeline %d, body %d; want zero", pipeline, body)
		}
	})

	t.Run("stopped gate rejects pending and new reservations", func(t *testing.T) {
		gate := newConnGate(1, time.Hour)
		gate.configureRequestCapacity(1, 1, 1, nil)
		reservation, ok := gate.reserveRequest(payload, false, true)
		if !ok {
			t.Fatal("reservation was rejected before stop")
		}
		gate.stop()
		if got := reservation.handoff(); got != requestHandoffUnavailable {
			reservation.release()
			t.Fatalf("stopped handoff = %v, want unavailable", got)
		}
		if extra, reserved := gate.reserveRequest(payload, false, true); reserved {
			extra.release()
			t.Fatal("stopped gate accepted a new reservation")
		}
		reservation.release()
		gate.mu.Lock()
		pipeline, body := gate.reservedPipeline, gate.reservedBody
		gate.mu.Unlock()
		if pipeline != 0 || body != 0 {
			t.Fatalf("stopped gate retained capacity: pipeline=%d body=%d", pipeline, body)
		}
	})
}
