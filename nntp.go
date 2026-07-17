package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mnightingale/rapidyenc"
)

var ErrMaxConnections = errors.New("nntp: server max connections reached")
var ErrConnectionDied = errors.New("nntp: connection died")

// isConnectionDeathError reports whether err indicates the underlying
// connection failed at the transport layer (as opposed to a protocol-level
// response like 430/502, which is delivered via StatusCode). These are
// retryable on a fresh connection: an established connection that goes stale
// surfaces ErrConnectionDied via failOutstanding, while a connection that dies
// on its bootstrap request surfaces the raw IO error (EOF, closed pipe, reset,
// timeout). Both mean "this socket is gone — open a new one."
func isConnectionDeathError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrAuthRequired) || errors.Is(err, ErrAuthRejected) ||
		errors.Is(err, ErrInvalidProviderConfiguration) {
		return false
	}
	if errors.Is(err, ErrConnectionDied) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return !netErr.Timeout()
	}
	return false
}

const (
	// inflightDrainTimeout is the maximum time to wait for in-flight
	// responses to complete during idle disconnect.
	inflightDrainTimeout = 10 * time.Second

	// defaultThrottleRestore is the default duration before restoring
	// throttled connection slots after a server "max connections" error.
	defaultThrottleRestore = 30 * time.Second

	// connFailureBackoff is the delay before retrying after a connection
	// factory error.
	connFailureBackoff = time.Second

	// maxConnsBackoff is the longer delay used when the server reports
	// max connections reached (502/400).
	maxConnsBackoff = 5 * time.Second

	// defaultKeepAlive is the TCP keep-alive interval used when the
	// provider does not specify one. Negative disables keep-alive.
	defaultKeepAlive = 30 * time.Second

	// defaultHandshakeTimeout caps the TCP dial + TLS handshake phase
	// to avoid hanging against unresponsive servers.
	defaultHandshakeTimeout = 10 * time.Second

	// maxConnDiedRetries bounds same-provider retries when a pooled connection
	// dies mid-request (typically a stale socket the server already closed).
	// The dead connection has drained by the time the error surfaces, so the
	// retry uses a fresh connection on the same provider.
	maxConnDiedRetries = 2

	// minAttemptTimeout is the floor (and default) for the provider response
	// timeout. It begins only when a request reaches FIFO response head and
	// bounds time-to-first-response-byte. Once response bytes start flowing,
	// the rolling stall timeout takes over instead.
	minAttemptTimeout = 2 * time.Second

	// maxAttemptTimeout caps the adaptive per-attempt timeout derived from a
	// provider's measured round-trip time.
	maxAttemptTimeout = 10 * time.Second

	// defaultStallTimeout is the rolling progress deadline applied to a body
	// transfer once bytes are flowing: if no further bytes arrive within this
	// window the connection is considered stalled and torn down. A healthy but
	// slow transfer keeps extending the deadline and never trips it.
	defaultStallTimeout = 8 * time.Second

	// stallDeadlineQuantum coarsens stall-deadline updates so the read path
	// issues at most one SetReadDeadline syscall per quantum instead of one per
	// read.
	stallDeadlineQuantum = 250 * time.Millisecond

	// Abandoned BODY responses are drained only within both bounds. Reaching a
	// bound retires the connection so obsolete pipeline data cannot delay or
	// poison later work.
	defaultAbandonedBodyDrainBytes   = 1 * 1024 * 1024
	defaultAbandonedBodyDrainTimeout = 250 * time.Millisecond
	temporaryRetryMinDelay           = 10 * time.Millisecond
	temporaryRetryJitter             = 15 * time.Millisecond
)

var errAbandonedBodyDrainLimit = errors.New("nntp: abandoned BODY drain limit exceeded")
var errFreshTransportRequired = errors.New("nntp: fresh transport required")
var errBackgroundStatWindowFull = errors.New("nntp: background STAT window full")

// Attempt lifecycle states distinguish local queueing, FIFO response-head
// service, decoded output commitment, and caller abandonment. This prevents a
// cancellation from restarting a request after decoded bytes crossed a caller
// writer boundary, including bytes that were already buffered locally.
const (
	attemptPending      int32 = iota // request has not reached response head
	attemptResponseHead              // request is the FIFO response head
	attemptCommitted                 // decoded bytes crossed the output boundary
)

type requestPhase uint8

const (
	requestQueued requestPhase = iota
	requestOwnedWriting
	requestFIFOPending
	requestResponseActive
	requestSettling
	requestSettled
	requestStoppedBeforeTransport
)

type greetingError struct {
	StatusCode int
	Message    string
}

func (e *greetingError) Error() string {
	return fmt.Sprintf("nntp greeting: %d %s", e.StatusCode, e.Message)
}

func (e *greetingError) Is(target error) bool {
	switch e.StatusCode {
	case 400, 502:
		return target == ErrMaxConnections
	case 480:
		return errors.Is(ErrAuthRequired, target)
	case 481:
		return errors.Is(ErrAuthRejected, target)
	default:
		return false
	}
}

func (e *greetingError) Unwrap() error {
	return &Error{Code: e.StatusCode, Message: e.Message}
}

func (e *greetingError) setupResponseCode() int {
	return e.StatusCode
}

type setupResponseError struct {
	cause      error
	statusCode int
}

func (e *setupResponseError) Error() string {
	if e == nil || e.cause == nil {
		return "<nil>"
	}
	return e.cause.Error()
}

func (e *setupResponseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *setupResponseError) setupResponseCode() int {
	if e == nil {
		return 0
	}
	return e.statusCode
}

type Request struct {
	Ctx context.Context

	Payload []byte
	RespCh  chan Response

	// Optional: decoded body bytes are streamed here. If nil, they are buffered into Response.Body.
	BodyWriter io.Writer

	// ValidateBody enables complete yEnc framing/integrity validation. It is
	// set by the high-level BODY APIs; raw Send remains source/behavior compatible.
	ValidateBody   bool
	FreshTransport bool

	// Optional: called with yEnc metadata once =ybegin/=ypart headers are parsed, before body decoding.
	OnMeta func(YEncMeta)
	// decodeFn is an internal deterministic test seam for native decoder
	// failures. Production requests leave it nil and use rapidyenc directly.
	decodeFn func(dst, src []byte, state *rapidyenc.State) (nDst, nSrc int, end rapidyenc.End, err error)

	// PayloadBody is an optional reader streamed to the connection after Payload.
	// Used by POST to stream article content without buffering in memory.
	PayloadBody  io.Reader
	closePayload func()

	// PostMode signals readerLoop to expect two NNTP responses (340 + 240/441).
	PostMode bool

	// postReadyCh is set by writeLoop for PostMode requests. The readerLoop
	// sends nil after reading 340 (proceed to write body) or a non-nil error
	// otherwise (e.g. 440 posting not allowed). Buffered with capacity 1.
	postReadyCh chan error
	// postWriteDone starts the final-response clock after the body flushes.
	postWriteDone chan struct{}

	// responseTimeout starts only when this request becomes FIFO response head.
	// Pool queue and pipeline-head wait remain governed solely by the caller ctx.
	responseTimeout time.Duration

	// attemptState records response-head admission and decoded commitment.
	// Decoded output advances responseHead→committed. Zero is attemptPending.
	attemptState atomic.Int32

	// lifecycleMu is the sole authority for request ownership, cancellation,
	// caller-writer commitment, response progress, and final cause.
	lifecycleMu      sync.Mutex
	phase            requestPhase
	lifecycleCause   error
	finalCause       error
	localWriterError error
	transportOwned   bool
	writerCommitted  bool
	writerActive     bool
	responseProgress bool
	drainRequired    bool
	deadlineOwner    readDeadlineOwner
	transportCause   func() error

	watchStop     chan struct{}
	watchDone     chan struct{}
	settledDone   chan struct{}
	bodyCloseOnce sync.Once
	responseOnce  sync.Once
	capacityOnce  sync.Once

	// submittedAt, writtenAt, and responseHeadAt delimit the three transport
	// timing components exposed in AttemptEvidence.
	submittedAt      time.Time
	writtenAt        atomic.Int64
	responseHeadAt   atomic.Int64
	writtenTime      time.Time
	responseHeadTime time.Time

	// heldBody is set by writeLoop when this (body-bearing) request acquired a
	// bodySem slot, so readerLoop releases exactly the slots that were taken.
	// Bodyless STAT requests never acquire bodySem and leave this false.
	heldInflight       bool
	heldBody           bool
	heldBackgroundStat bool
	heldPipeline       bool
	Priority           bool
}

type Response struct {
	StatusCode int
	Status     string
	ProviderID string
	Attempts   []AttemptEvidence

	// For non-body multiline responses (CAPABILITIES, etc).
	Lines []string

	// Decoded payload bytes (only if Request.BodyWriter == nil).
	Body bytes.Buffer

	// Decoder metadata/status gathered while parsing.
	Meta NNTPResponse

	Err     error
	Request *Request
}

type Auth struct {
	Username string
	Password string
}

// ConnFactory is used by Client to create connections.
type ConnFactory func(ctx context.Context) (net.Conn, error)

type NNTPConnection struct {
	conn net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	reqCh     <-chan *Request
	prioCh    <-chan *Request // priority channel; nil for standalone connections
	hotReqCh  <-chan *Request // unbuffered; set by runConnSlot before Run()
	hotPrioCh <-chan *Request // unbuffered; set by runConnSlot before Run()
	pending   chan *Request

	// inflightSem bounds the total pipeline depth (cap = StatInflight, i.e.
	// max(Inflight, StatInflight)). bodySem additionally bounds concurrent
	// body-bearing commands (cap = Inflight) so raising the STAT pipeline depth
	// never increases the number of BODY responses buffered/streamed at once.
	// Bodyless STAT commands acquire only inflightSem and so pipeline to the
	// deeper StatInflight depth.
	inflightSem         chan struct{}
	bodySem             chan struct{}
	backgroundStatSem   chan struct{}
	backgroundStatFreed chan struct{}

	rb readBuffer

	Greeting NNTPResponse

	firstReq              *Request      // bootstrap request from connection slot
	secondReq             *Request      // normal request deferred behind bootstrap priority
	idleTimeout           time.Duration // 0 = no idle timeout
	stallTimeout          time.Duration // rolling body-progress deadline; 0 = disabled
	abandonedDrainBytes   int           // maximum obsolete BODY bytes to drain
	abandonedDrainTimeout time.Duration // maximum obsolete BODY drain duration
	keepaliveInterval     time.Duration // 0 = no keepalive
	keepaliveCommand      string        // NNTP command for keepalive probe (e.g. "DATE")
	providerName          string        // set by runConnSlot; used for error context
	providerID            string        // stable identity exposed in results/evidence
	createdAt             time.Time
	userAgent             string

	stats *providerStats // nil for standalone connections

	done   chan struct{}
	doneMu sync.Once

	connCloseOnce sync.Once
	connCloseErr  error

	failMu sync.Mutex
}

func classifyDialConfigurationError(err error) error {
	if err == nil {
		return nil
	}
	var addressError *net.AddrError
	if errors.As(err, &addressError) {
		return withErrorClassification(err, ErrInvalidProviderConfiguration)
	}
	return err
}

func isBootstrapTransportError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func classifyTLSConfigurationError(err error) error {
	if err == nil || isBootstrapTransportError(err) {
		return err
	}
	return withErrorClassification(err, ErrInvalidProviderConfiguration)
}

func newNetConn(ctx context.Context, addr string, tlsConfig *tls.Config, keepAlive time.Duration) (net.Conn, error) {
	if keepAlive == 0 {
		keepAlive = defaultKeepAlive
	}
	ctx, cancel := context.WithTimeout(ctx, defaultHandshakeTimeout)
	defer cancel()
	dialer := net.Dialer{KeepAlive: keepAlive}
	if tlsConfig != nil {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, classifyDialConfigurationError(err)
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, classifyTLSConfigurationError(err)
		}
		return tlsConn, nil
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, classifyDialConfigurationError(err)
	}
	return conn, nil
}

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, prioCh <-chan *Request, auth Auth, userAgent string, sharedBuf *readBuffer, stats *providerStats) (*NNTPConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithCancel(ctx)

	var readBuf []byte
	if sharedBuf != nil && len(sharedBuf.buf) > 0 {
		// Reuse the buffer from a previous connection, reset read positions and deadline cache.
		readBuf = sharedBuf.buf
	} else {
		readBuf = make([]byte, defaultReadBufSize)
	}

	c := &NNTPConnection{
		conn:        conn,
		ctx:         cctx,
		cancel:      cancel,
		reqCh:       reqCh,
		prioCh:      prioCh,
		pending:     make(chan *Request, inflightLimit),
		inflightSem: make(chan struct{}, inflightLimit),
		// Default bodySem to the full pipeline depth (no separate BODY bound);
		// runConnSlot overrides this to Provider.Inflight when a deeper STAT
		// pipeline is configured. Standalone connections keep them equal.
		bodySem:               make(chan struct{}, inflightLimit),
		backgroundStatSem:     make(chan struct{}, inflightLimit),
		backgroundStatFreed:   make(chan struct{}, 1),
		rb:                    readBuffer{buf: readBuf},
		stats:                 stats,
		done:                  make(chan struct{}),
		userAgent:             userAgent,
		abandonedDrainBytes:   defaultAbandonedBodyDrainBytes,
		abandonedDrainTimeout: defaultAbandonedBodyDrainTimeout,
		createdAt:             time.Now(),
	}

	// Server greeting is sent immediately upon connect.
	greeting, err := c.readOneResponse(io.Discard)
	if err != nil {
		return nil, fmt.Errorf("nntp greeting: %w", err)
	}
	c.Greeting = greeting
	if greeting.StatusCode != 200 && greeting.StatusCode != 201 {
		return nil, &greetingError{StatusCode: greeting.StatusCode, Message: greeting.Message}
	}

	// Optional AUTHINFO handshake.
	if auth.Username != "" {
		if auth.Password == "" {
			cause := errors.New("nntp auth: password required when username is set")
			return nil, withErrorClassification(cause, ErrInvalidProviderConfiguration)
		}

		if err := c.auth(auth); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func authResponseError(stage string, resp NNTPResponse) error {
	cause := fmt.Errorf("nntp auth: unexpected response to %s: %s", stage, resp.Message)
	category := toError(resp.StatusCode, resp.Message)
	if category == nil {
		category = &Error{Code: resp.StatusCode, Message: resp.Message}
	}
	return &setupResponseError{
		cause:      withErrorClassification(cause, category),
		statusCode: resp.StatusCode,
	}
}

func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth, userAgent string) (*NNTPConnection, error) {
	conn, err := newNetConn(ctx, addr, tlsConfig, 0)
	if err != nil {
		return nil, err
	}

	c, err := newNNTPConnectionFromConn(ctx, conn, inflightLimit, reqCh, nil, auth, userAgent, nil, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *NNTPConnection) auth(auth Auth) error {
	// AUTHINFO USER
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO USER %s\r\n", auth.Username); err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO USER: %w", err)
	}
	resp, err := c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO USER: %w", err)
	}

	switch resp.StatusCode {
	case 281:
		return nil // authenticated
	case 381:
		// need pass
	default:
		return authResponseError("AUTHINFO USER", resp)
	}

	// AUTHINFO PASS
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO PASS %s\r\n", auth.Password); err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO PASS: %w", err)
	}
	resp, err = c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO PASS: %w", err)
	}
	if resp.StatusCode != 281 {
		return authResponseError("AUTHINFO PASS", resp)
	}
	return nil
}

func (c *NNTPConnection) Done() <-chan struct{} { return c.done }

func (c *NNTPConnection) closeDone() {
	c.doneMu.Do(func() { close(c.done) })
}

func safeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}

// keepaliveExpectedCode returns the expected NNTP status code for the given
// keepalive command: DATE→111, HELP→100, CAPABILITIES→101, default→111.
func keepaliveExpectedCode(cmd string) int {
	switch cmd {
	case "HELP":
		return 100
	case "CAPABILITIES":
		return 101
	default:
		return 111
	}
}

// isCheapCommand reports whether payload is a bodyless command that should
// bypass the BODY concurrency bound (bodySem) and pipeline to the full
// inflightSem (StatInflight) depth.
func isCheapCommand(payload []byte) bool {
	return operationFromPayload(payload) == OperationStat
}

func isStreamingCommand(payload []byte) bool {
	op := operationFromPayload(payload)
	return op == OperationBody || op == operationArticle
}

func (req *Request) closePayloadBody() {
	req.bodyCloseOnce.Do(func() {
		if req.closePayload != nil {
			req.closePayload()
			return
		}
		if closer, ok := req.PayloadBody.(io.Closer); ok {
			_ = closer.Close()
		}
	})
}

func (req *Request) noteCauseLocked(cause error) error {
	if req.lifecycleCause == nil {
		if cause == nil && req.Ctx != nil {
			cause = req.Ctx.Err()
		}
		if cause == nil && req.transportCause != nil {
			cause = req.transportCause()
		}
		if cause != nil {
			req.lifecycleCause = cause
		}
	}
	return req.lifecycleCause
}

func (req *Request) stopBeforeTransport(cause error) bool {
	req.lifecycleMu.Lock()
	_ = req.noteCauseLocked(cause)
	if req.phase != requestQueued {
		req.lifecycleMu.Unlock()
		return false
	}
	req.phase = requestStoppedBeforeTransport
	req.lifecycleMu.Unlock()
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		req.closePayloadBody()
	}
	return true
}

func (req *Request) claimTransport() bool {
	req.lifecycleMu.Lock()
	if req.Ctx != nil && req.Ctx.Err() != nil {
		_ = req.noteCauseLocked(req.Ctx.Err())
	}
	if req.phase != requestQueued || req.lifecycleCause != nil {
		if req.phase == requestQueued {
			req.phase = requestStoppedBeforeTransport
		}
		cause := req.lifecycleCause
		req.lifecycleMu.Unlock()
		if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
			req.closePayloadBody()
		}
		return false
	}
	req.phase = requestOwnedWriting
	req.transportOwned = true
	req.lifecycleMu.Unlock()
	return true
}

func (req *Request) markPending() {
	req.lifecycleMu.Lock()
	req.phase = requestFIFOPending
	req.lifecycleMu.Unlock()
}

func (req *Request) beginResponse() (cancelled bool) {
	req.lifecycleMu.Lock()
	req.phase = requestResponseActive
	cancelled = req.noteCauseLocked(nil) != nil
	if cancelled {
		req.drainRequired = true
	}
	req.lifecycleMu.Unlock()
	req.attemptState.CompareAndSwap(attemptPending, attemptResponseHead)
	return cancelled
}

func (req *Request) startWatch(c *NNTPConnection) {
	stop := make(chan struct{})
	done := make(chan struct{})
	req.watchStop, req.watchDone = stop, done
	go func() {
		defer close(done)
		select {
		case <-req.Ctx.Done():
			req.cancelOwned(c, req.Ctx.Err())
		case <-c.ctx.Done():
			cause := error(ErrConnectionDied)
			if req.transportCause != nil {
				cause = req.transportCause()
			}
			req.cancelOwned(c, cause)
		case <-stop:
		}
	}()
}

func (req *Request) cancelOwned(c *NNTPConnection, cause error) {
	req.lifecycleMu.Lock()
	_ = req.noteCauseLocked(cause)
	retire := false
	forceDrain := false
	switch req.phase {
	case requestOwnedWriting:
		retire = true
	case requestFIFOPending:
	case requestResponseActive:
		if req.drainRequired {
			break
		} else if isStreamingCommand(req.Payload) && req.responseProgress && !req.writerActive {
			req.drainRequired = true
			forceDrain = true
		} else {
			retire = true
		}
	}
	req.lifecycleMu.Unlock()
	if forceDrain {
		if c.abandonedDrainTimeout <= 0 ||
			c.rb.forceDrainDeadline(c.conn, time.Now().Add(c.abandonedDrainTimeout)) != nil {
			retire = true
		}
	}
	if retire {
		_ = c.closeTransport()
	}
	req.closePayloadBody()
}

func (req *Request) joinWatch() {
	stop, done := req.watchStop, req.watchDone
	if stop == nil {
		return
	}
	close(stop)
	<-done
}

func (req *Request) recordDeadlineOwner(owner readDeadlineOwner) {
	req.lifecycleMu.Lock()
	req.deadlineOwner = owner
	req.lifecycleMu.Unlock()
}

func (req *Request) deadlineCauseIsCaller() bool {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	if req.deadlineOwner != readDeadlineNone {
		return req.deadlineOwner == readDeadlineCaller
	}
	return req.Ctx != nil && errors.Is(req.Ctx.Err(), context.DeadlineExceeded)
}

func (req *Request) providerDeadlineExpired(cause error) bool {
	req.lifecycleMu.Lock()
	owner := req.deadlineOwner
	req.lifecycleMu.Unlock()
	var networkError net.Error
	return owner == readDeadlineProviderResponse &&
		errors.As(cause, &networkError) && networkError.Timeout()
}

func (req *Request) finalize(cause error) error {
	req.lifecycleMu.Lock()
	if req.phase == requestSettling {
		done := req.settledDone
		req.lifecycleMu.Unlock()
		<-done
		req.lifecycleMu.Lock()
		final := req.finalCause
		req.lifecycleMu.Unlock()
		return final
	}
	if req.phase == requestSettled {
		final := req.finalCause
		req.lifecycleMu.Unlock()
		return final
	}
	providerTimeout := req.providerDeadlineExpiredLocked(cause)
	_ = req.noteCauseLocked(nil)
	switch {
	case req.localWriterError != nil:
		req.finalCause = req.localWriterError
	case providerTimeout && errors.Is(req.lifecycleCause, context.DeadlineExceeded):
		req.finalCause = cause
	case req.lifecycleCause != nil:
		req.finalCause = req.lifecycleCause
	default:
		req.finalCause = cause
	}
	if req.settledDone == nil {
		req.settledDone = make(chan struct{})
	}
	req.phase = requestSettling
	done := req.settledDone
	final := req.finalCause
	req.lifecycleMu.Unlock()
	req.joinWatch()
	req.lifecycleMu.Lock()
	req.phase = requestSettled
	close(done)
	req.lifecycleMu.Unlock()
	return final
}

func (req *Request) providerDeadlineExpiredLocked(cause error) bool {
	var networkError net.Error
	return (req.deadlineOwner == readDeadlineProviderResponse || req.deadlineOwner == readDeadlineProviderStall) &&
		errors.As(cause, &networkError) && networkError.Timeout()
}

func normalizeRequestReadError(req *Request, owner readDeadlineOwner, err error) error {
	if err == nil {
		return nil
	}
	req.recordDeadlineOwner(owner)
	switch owner {
	case readDeadlineCaller:
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			return err
		}
		req.recordCause(context.DeadlineExceeded)
		return context.DeadlineExceeded
	case readDeadlineAbandonedDrain:
		return errAbandonedBodyDrainLimit
	default:
		return err
	}
}

func (req *Request) committed() bool {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	return req.transportOwned && (req.writerCommitted || operationFromPayload(req.Payload) == OperationPost)
}

func (req *Request) stoppedPretransport() bool {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	return !req.transportOwned &&
		(req.phase == requestStoppedBeforeTransport || req.phase == requestSettling || req.phase == requestSettled)
}

func (req *Request) needsDrainClear() bool {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	return req.drainRequired
}

func deliverRequestResponse(req *Request, providerID string, resp Response) {
	req.responseOnce.Do(func() {
		resp.Request = req
		if resp.ProviderID == "" {
			resp.ProviderID = providerID
		}
		defer func() { _ = recover() }()
		select {
		case req.RespCh <- resp:
		default:
		}
		close(req.RespCh)
	})
}

func failRequest(ch chan Response, err error) {
	defer func() { _ = recover() }()
	select {
	case ch <- Response{Err: err}:
	default:
	}
	close(ch)
}

func finishUnadmittedRequest(req *Request, providerID string, cause error) {
	if req.Ctx == nil {
		req.Ctx = context.Background()
	}
	if !req.stoppedPretransport() {
		req.stopBeforeTransport(cause)
	}
	resp := Response{Err: req.finalize(cause), Request: req, ProviderID: providerID}
	if !errors.Is(resp.Err, errFreshTransportRequired) {
		resp.Attempts = []AttemptEvidence{buildAttemptEvidence(req, providerID, resp, time.Now())}
	}
	deliverRequestResponse(req, providerID, resp)
}

func (c *NNTPConnection) failOutstanding() {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	connErr := error(ErrConnectionDied)
	if c.providerName != "" {
		connErr = fmt.Errorf("%s: %w", c.providerName, ErrConnectionDied)
	}
	for {
		select {
		case req := <-c.pending:
			if req == nil {
				continue
			}
			resp := Response{Err: req.finalize(connErr), Request: req, ProviderID: c.providerID}
			resp.Attempts = []AttemptEvidence{buildAttemptEvidence(req, c.providerID, resp, time.Now())}
			c.completeFinalizedRequest(req, resp)
		default:
			return
		}
	}
}

func (c *NNTPConnection) Close() error {
	_ = c.closeTransport()
	<-c.done
	return nil
}

func (c *NNTPConnection) closeTransport() error {
	c.cancel()
	c.connCloseOnce.Do(func() { c.connCloseErr = c.conn.Close() })
	return c.connCloseErr
}

// waitForInflightDrain acquires all semaphore slots, blocking until each
// in-flight response completes. This ensures a clean idle disconnect with
// no lost requests. A 10s timeout prevents hanging if the server stops
// responding mid-response.
func (c *NNTPConnection) waitForInflightDrain() {
	timer := time.NewTimer(inflightDrainTimeout)
	defer timer.Stop()
	for range cap(c.inflightSem) {
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		case <-timer.C:
			return
		}
	}
}

// connGate controls how many connection slots may be connecting/running
// simultaneously within a single provider. When the server returns a
// "max connections" greeting (502/400), throttle() reduces the allowed
// count to the number of currently running connections (min 1) and starts
// a restore timer.
type connGate struct {
	mu           sync.Mutex
	cond         *sync.Cond
	maxSlots     int // original p.Connections
	allowed      int // current limit (reduced during throttle)
	held         int // slots past enter() (connecting + running)
	running      int // slots inside nc.Run()
	restoreTimer *time.Timer
	restoreDur   time.Duration
	available    atomic.Int32 // allowed - held; updated under mu, read lock-free
}

func newConnGate(max int, restoreDur time.Duration) *connGate {
	if restoreDur <= 0 {
		restoreDur = defaultThrottleRestore
	}
	g := &connGate{
		maxSlots:   max,
		allowed:    max,
		restoreDur: restoreDur,
	}
	g.cond = sync.NewCond(&g.mu)
	g.available.Store(int32(max))
	return g
}

// enter blocks until held < allowed or one of the contexts is cancelled.
// Returns true if the slot was granted.
func (g *connGate) enter(slotCtx, reqCtx context.Context) bool {
	// Spin up a goroutine that broadcasts on context cancellation so
	// cond.Wait() can re-check.
	done := make(chan struct{})
	go func() {
		select {
		case <-slotCtx.Done():
		case <-reqCtx.Done():
		case <-done:
		}
		g.cond.Broadcast()
	}()

	g.mu.Lock()
	defer g.mu.Unlock()
	defer close(done)

	for g.held >= g.allowed {
		if slotCtx.Err() != nil || reqCtx.Err() != nil {
			return false
		}
		g.cond.Wait()
	}
	g.held++
	g.available.Store(int32(g.allowed - g.held))
	return true
}

func (g *connGate) exit() {
	g.mu.Lock()
	g.held--
	g.available.Store(int32(g.allowed - g.held))
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) markRunning() {
	g.mu.Lock()
	g.running++
	g.mu.Unlock()
}

func (g *connGate) markNotRunning() {
	g.mu.Lock()
	g.running--
	g.mu.Unlock()
}

// throttle reduces allowed slots to max(1, running) and resets the restore timer.
func (g *connGate) throttle() {
	g.mu.Lock()
	defer g.mu.Unlock()

	newAllowed := max(1, g.running)
	// Only tighten, never loosen during throttle.
	if newAllowed < g.allowed {
		g.allowed = newAllowed
	}

	// Reset (or start) the restore timer.
	if g.restoreTimer != nil {
		g.restoreTimer.Stop()
	}
	g.restoreTimer = time.AfterFunc(g.restoreDur, g.restore)
	g.available.Store(int32(g.allowed - g.held))
}

func (g *connGate) restore() {
	g.mu.Lock()
	g.allowed = g.maxSlots
	g.restoreTimer = nil
	g.available.Store(int32(g.allowed - g.held))
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) stop() {
	g.mu.Lock()
	if g.restoreTimer != nil {
		g.restoreTimer.Stop()
		g.restoreTimer = nil
	}
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) snapshot() (maxSlots, running int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxSlots, g.running
}

// runConnSlot is the slot goroutine that manages the lifecycle of a single
// connection: IDLE → CONNECTING → ACTIVE → (death/idle) → IDLE.
func runConnSlot(ctx context.Context, reqCh <-chan *Request, prioCh <-chan *Request, hotReqCh <-chan *Request, hotPrioCh <-chan *Request, factory ConnFactory, inflight int, statInflight int, backgroundStatInflight int, auth Auth, userAgent string, idleTimeout time.Duration, stallTimeout time.Duration, abandonedDrainBytes int, abandonedDrainTimeout time.Duration, keepaliveInterval time.Duration, keepaliveCommand string, gate *connGate, stats *providerStats, providerName, providerID string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Shared read buffer persists across reconnections to avoid re-growing.
	var sharedBuf readBuffer
	var carriedReq *Request

	for {
		// IDLE: wait for a request (zero TCP resources).
		// Prefer priority requests over normal ones.
		var firstReq *Request
		var secondReq *Request
		var ok bool
		if carriedReq != nil {
			firstReq, ok = carriedReq, true
			carriedReq = nil
			select {
			case priorityReq, priorityOK := <-prioCh:
				if !priorityOK {
					finishUnadmittedRequest(firstReq, providerID, ctx.Err())
					return
				}
				secondReq = firstReq
				firstReq = priorityReq
			default:
			}
		} else {
			select {
			case firstReq, ok = <-prioCh:
				if !ok {
					return
				}
			default:
				select {
				case firstReq, ok = <-prioCh:
					if !ok {
						return
					}
				case firstReq, ok = <-reqCh:
					if !ok {
						return // channel closed, shut down
					}
					select {
					case priorityReq, priorityOK := <-prioCh:
						if !priorityOK {
							finishUnadmittedRequest(firstReq, providerID, ctx.Err())
							return
						}
						secondReq = firstReq
						firstReq = priorityReq
					default:
					}
				case <-ctx.Done():
					return
				}
			}
		}

		// Check if the request is already cancelled.
		select {
		case <-firstReq.Ctx.Done():
			finishUnadmittedRequest(firstReq, providerID, firstReq.Ctx.Err())
			carriedReq = secondReq
			continue
		default:
		}

		// GATE: block if we're at the throttled capacity limit.
		if !gate.enter(ctx, firstReq.Ctx) {
			// Slot or request context cancelled while waiting at the gate.
			cause := error(context.Canceled)
			if firstReq.Ctx.Err() != nil {
				cause = firstReq.Ctx.Err()
			}
			finishUnadmittedRequest(firstReq, providerID, cause)
			carriedReq = secondReq
			continue
		}

		// CONNECTING: dial, greet, authenticate.
		conn, err := factory(ctx)
		if err != nil {
			gate.exit()
			finishUnadmittedRequest(firstReq, providerID, fmt.Errorf("%s: %w", providerName, err))
			carriedReq = secondReq
			// Backoff before retrying to avoid thrashing.
			select {
			case <-time.After(connFailureBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Size the pipeline (inflightSem/pending) to statInflight so bodyless
		// STAT commands can pipeline deep; bodySem is overridden below to the
		// (smaller) Inflight so concurrent bodies stay bounded.
		nc, err := newNNTPConnectionFromConn(ctx, conn, statInflight, reqCh, prioCh, auth, userAgent, &sharedBuf, stats)
		if err != nil {
			_ = conn.Close()
			finishUnadmittedRequest(firstReq, providerID, fmt.Errorf("%s: %w", providerName, err))
			carriedReq = secondReq

			if errors.Is(err, ErrMaxConnections) {
				// Server said "max connections" — throttle and use longer backoff.
				gate.throttle()
				gate.exit()
				select {
				case <-time.After(maxConnsBackoff):
				case <-ctx.Done():
					return
				}
			} else {
				gate.exit()
				select {
				case <-time.After(connFailureBackoff):
				case <-ctx.Done():
					return
				}
			}
			continue
		}

		// ACTIVE: run the connection with the bootstrap request.
		// Bound concurrent bodies to Inflight while the pipeline (inflightSem)
		// allows STAT to reach statInflight. When statInflight == inflight this
		// is identical to the default (both caps equal).
		nc.bodySem = make(chan struct{}, inflight)
		nc.backgroundStatSem = make(chan struct{}, backgroundStatInflight)
		nc.firstReq = firstReq
		nc.secondReq = secondReq
		nc.idleTimeout = idleTimeout
		nc.stallTimeout = stallTimeout
		nc.abandonedDrainBytes = abandonedDrainBytes
		nc.abandonedDrainTimeout = abandonedDrainTimeout
		nc.providerName = providerName
		nc.providerID = providerID
		nc.hotReqCh = hotReqCh
		nc.hotPrioCh = hotPrioCh
		nc.keepaliveInterval = keepaliveInterval
		nc.keepaliveCommand = keepaliveCommand
		gate.markRunning()
		nc.Run() // blocks until death or idle timeout
		gate.markNotRunning()
		gate.exit()

		// Preserve the (possibly grown) read buffer for next connection.
		sharedBuf.buf = nc.rb.buf

		// Loop back to IDLE for automatic reconnection.
	}
}

type streamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

type writerRef struct {
	w io.Writer
}

func (wr *writerRef) Write(p []byte) (int, error) {
	return wr.w.Write(p)
}

type attemptWriter struct {
	req *Request
	w   io.Writer
}

type callerWriterError struct{ cause error }

func (e *callerWriterError) Error() string {
	if e == nil || e.cause == nil {
		return "nntp: caller writer failure"
	}
	return e.cause.Error()
}

func (e *callerWriterError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func markRequestWritten(req *Request) {
	now := time.Now()
	req.writtenAt.Store(now.UnixNano())
	req.lifecycleMu.Lock()
	req.writtenTime = now
	req.lifecycleMu.Unlock()
	req.markPending()
}

func (c *NNTPConnection) acquireBackgroundStat(req *Request) error {
	if !isCheapCommand(req.Payload) || req.Priority {
		return nil
	}
	select {
	case c.backgroundStatSem <- struct{}{}:
		req.heldBackgroundStat = true
		if c.stats != nil {
			c.stats.BackgroundStatInUse.Add(1)
		}
		return nil
	default:
		return errBackgroundStatWindowFull
	}
}

func (c *NNTPConnection) releaseBackgroundStat(req *Request) {
	if !req.heldBackgroundStat {
		return
	}
	req.heldBackgroundStat = false
	<-c.backgroundStatSem
	if c.stats != nil {
		c.stats.BackgroundStatInUse.Add(-1)
	}
	select {
	case c.backgroundStatFreed <- struct{}{}:
	default:
	}
}

func (c *NNTPConnection) reservePipelineOccupancy(req *Request) {
	if req.heldPipeline {
		return
	}
	req.heldPipeline = true
	if c.stats != nil {
		c.stats.PipelineInUse.Add(1)
	}
}

func (c *NNTPConnection) releasePipelineOccupancy(req *Request) {
	c.releaseBackgroundStat(req)
	if req.heldPipeline && c.stats != nil {
		c.stats.PipelineInUse.Add(-1)
	}
	req.heldPipeline = false
}

func (c *NNTPConnection) releaseAdmittedRequest(req *Request) {
	req.capacityOnce.Do(func() {
		c.releasePipelineOccupancy(req)
		if req.heldBody {
			<-c.bodySem
			req.heldBody = false
		}
		if req.heldInflight {
			<-c.inflightSem
			req.heldInflight = false
		}
	})
}

func (c *NNTPConnection) completeFinalizedRequest(req *Request, resp Response) {
	if req.needsDrainClear() {
		if err := c.rb.clearDrainDeadline(c.conn); err != nil {
			_ = c.closeTransport()
			c.failOutstanding()
		}
	}
	req.closePayloadBody()
	c.releaseAdmittedRequest(req)
	deliverRequestResponse(req, c.providerID, resp)
}

func (c *NNTPConnection) waitForInflightSettlement() bool {
	acquired := 0
	defer func() {
		for range acquired {
			<-c.inflightSem
		}
	}()
	for acquired < cap(c.inflightSem) {
		select {
		case c.inflightSem <- struct{}{}:
			acquired++
		case <-c.ctx.Done():
			return false
		}
	}
	return true
}

func (w *attemptWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.req.lifecycleMu.Lock()
	if cause := w.req.noteCauseLocked(nil); cause != nil {
		w.req.lifecycleMu.Unlock()
		return 0, cause
	}
	w.req.writerCommitted = true
	w.req.writerActive = true
	w.req.responseProgress = true
	w.req.attemptState.CompareAndSwap(attemptResponseHead, attemptCommitted)
	w.req.lifecycleMu.Unlock()

	n, err := w.w.Write(p)
	switch {
	case n < 0:
		n = 0
		if err == nil {
			err = errors.New("nntp: caller writer returned a negative byte count")
		}
	case n > len(p):
		n = len(p)
		if err == nil {
			err = errors.New("nntp: caller writer returned an excessive byte count")
		}
	case n < len(p) && err == nil:
		err = io.ErrShortWrite
	}

	w.req.lifecycleMu.Lock()
	w.req.writerActive = false
	if err != nil {
		local := &callerWriterError{cause: err}
		if w.req.localWriterError == nil {
			w.req.localWriterError = local
		}
		w.req.lifecycleMu.Unlock()
		return n, local
	}
	cause := w.req.noteCauseLocked(nil)
	if cause != nil && isStreamingCommand(w.req.Payload) {
		w.req.drainRequired = true
		w.req.lifecycleMu.Unlock()
		return n, nil
	}
	w.req.lifecycleMu.Unlock()
	return n, cause
}

func (req *Request) currentCause() error {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	return req.noteCauseLocked(nil)
}

func (req *Request) timingBoundaries() (written, responseHead time.Time) {
	req.lifecycleMu.Lock()
	defer req.lifecycleMu.Unlock()
	return req.writtenTime, req.responseHeadTime
}

func (req *Request) recordCause(cause error) {
	req.lifecycleMu.Lock()
	_ = req.noteCauseLocked(cause)
	req.lifecycleMu.Unlock()
}

func (c *NNTPConnection) retireBeforeResponse(req *Request, cause error) {
	req.recordCause(cause)
	_ = c.closeTransport()
	req.closePayloadBody()
	resp := Response{Err: req.finalize(cause), Request: req, ProviderID: c.providerID}
	resp.Attempts = []AttemptEvidence{buildAttemptEvidence(req, c.providerID, resp, time.Now())}
	c.completeFinalizedRequest(req, resp)
}

func (c *NNTPConnection) writeAdmitted(bw *bufio.Writer, req *Request) bool {
	if !req.claimTransport() {
		c.releaseAdmittedRequest(req)
		finishUnadmittedRequest(req, c.providerID, req.currentCause())
		return false
	}
	req.startWatch(c)
	c.reservePipelineOccupancy(req)

	if _, err := bw.Write(req.Payload); err != nil {
		c.retireBeforeResponse(req, err)
		return true
	}
	if err := bw.Flush(); err != nil {
		c.retireBeforeResponse(req, err)
		return true
	}
	markRequestWritten(req)
	if req.PostMode {
		req.postReadyCh = make(chan error, 1)
		req.postWriteDone = make(chan struct{})
	}
	c.pending <- req
	if !req.PostMode {
		return false
	}

	var postErr error
	select {
	case postErr = <-req.postReadyCh:
	case <-c.ctx.Done():
		_ = c.closeTransport()
		close(req.postWriteDone)
		return true
	}
	if postErr != nil {
		close(req.postWriteDone)
		return false
	}
	if cause := req.currentCause(); cause != nil {
		_ = c.closeTransport()
		close(req.postWriteDone)
		return true
	}
	if req.PayloadBody != nil {
		if _, err := io.Copy(bw, req.PayloadBody); err != nil {
			req.recordCause(err)
			_ = c.closeTransport()
			close(req.postWriteDone)
			return true
		}
	}
	if err := bw.Flush(); err != nil {
		req.recordCause(err)
		_ = c.closeTransport()
		close(req.postWriteDone)
		return true
	}
	close(req.postWriteDone)
	return false
}

func (c *NNTPConnection) Run() {
	var unsent *Request
	deferredNormal := c.secondReq
	c.secondReq = nil
	readerDone := make(chan struct{})
	defer func() {
		_ = c.closeTransport()
		c.failOutstanding()
		if unsent != nil {
			c.releaseAdmittedRequest(unsent)
			finishUnadmittedRequest(unsent, c.providerID, ErrConnectionDied)
		}
		if deferredNormal != nil {
			finishUnadmittedRequest(deferredNormal, c.providerID, ErrConnectionDied)
		}
		<-readerDone
		c.closeDone()
	}()

	go func() {
		defer close(readerDone)
		c.readerLoop()
		// ensure writer exits too
		c.cancel()
	}()

	// Buffered writer coalesces multiple small BODY commands into fewer
	// write syscalls when inflight > 1. Flushed before any blocking op.
	bw := bufio.NewWriterSize(c.conn, 4096)

	// Cached write deadline state to avoid redundant SetWriteDeadline syscalls.
	var lastWriteDL time.Time
	lastWriteHasDL := false
	writeDLSet := false

	setWriteDeadline := func(dl time.Time, hasDL bool) {
		if writeDLSet && lastWriteHasDL == hasDL && (!hasDL || dl.Equal(lastWriteDL)) {
			return
		}
		if hasDL {
			_ = c.conn.SetWriteDeadline(dl)
		} else {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}
		lastWriteDL = dl
		lastWriteHasDL = hasDL
		writeDLSet = true
	}

	// Process the bootstrap request injected by runConnSlot, if any.
	if c.firstReq != nil {
		req := c.firstReq
		unsent = req
		c.firstReq = nil

		if req.Ctx == nil {
			req.Ctx = context.Background()
		}
		if req.FreshTransport && c.createdAt.Before(req.submittedAt) {
			unsent = nil
			finishUnadmittedRequest(req, c.providerID, errFreshTransportRequired)
			return
		}

		// Check cancellation.
		select {
		case <-req.Ctx.Done():
			unsent = nil
			finishUnadmittedRequest(req, c.providerID, req.Ctx.Err())
			// Connection is still good — fall through to main loop.
			goto mainLoop
		default:
		}

		// Acquire inflight slot.
		select {
		case c.inflightSem <- struct{}{}:
			req.heldInflight = true
		case <-c.ctx.Done():
			return
		}

		// Body-bearing requests additionally take a bodySem slot so concurrent
		// bodies stay bounded by Inflight even when the pipeline (inflightSem)
		// is deeper for STAT. Bodyless STAT skips this and pipelines deep.
		if !isCheapCommand(req.Payload) {
			select {
			case c.bodySem <- struct{}{}:
				req.heldBody = true
			case <-req.Ctx.Done():
				unsent = nil
				c.releaseAdmittedRequest(req)
				finishUnadmittedRequest(req, c.providerID, req.Ctx.Err())
				goto mainLoop // connection still good
			case <-c.ctx.Done():
				return
			}
		}
		if err := c.acquireBackgroundStat(req); err != nil {
			unsent = nil
			c.releaseAdmittedRequest(req)
			finishUnadmittedRequest(req, c.providerID, err)
			goto mainLoop
		}

		dl, hasDL := req.writeDeadline()
		setWriteDeadline(dl, hasDL)

		unsent = nil
		if c.writeAdmitted(bw, req) {
			return
		}
	}

mainLoop:
	// Flush any buffered writes before blocking.
	if bw.Buffered() > 0 {
		if err := bw.Flush(); err != nil {
			return
		}
	}

	// Set up idle timer (nil if no idle timeout configured).
	var idleTimer *time.Timer
	var idleCh <-chan time.Time
	if c.idleTimeout > 0 {
		idleTimer = time.NewTimer(c.idleTimeout)
		idleCh = idleTimer.C
		defer idleTimer.Stop()
	}

	// Set up keepalive timer (nil if no keepalive configured).
	var keepaliveCh <-chan time.Time
	if c.keepaliveInterval > 0 {
		keepaliveCh = time.After(c.keepaliveInterval)
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Flush buffered writes before blocking on semaphore.
		if bw.Buffered() > 0 {
			if err := bw.Flush(); err != nil {
				return
			}
		}

		// wait until we have inflight capacity
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		}

		// Flush buffered writes before blocking on channel read.
		if bw.Buffered() > 0 {
			if err := bw.Flush(); err != nil {
				<-c.inflightSem
				return
			}
		}

		// pull next request (with idle timeout)
		// Hot channels are tried first (non-blocking) so that requests
		// prefer already-connected connections over waking cold slots.
		// When hotReqCh/hotPrioCh are nil (standalone path), receives
		// from nil channels block forever in select and are excluded.
		var req *Request
		var ok bool
		var didKeepalive bool
		if c.prioCh != nil {
			takePriority := func() (*Request, bool, bool) {
				select {
				case priorityReq, priorityOK := <-c.hotPrioCh:
					return priorityReq, priorityOK, true
				default:
				}
				select {
				case priorityReq, priorityOK := <-c.prioCh:
					return priorityReq, priorityOK, true
				default:
					return nil, false, false
				}
			}

			var gotPriority bool
			blockedDeferredStat := deferredNormal != nil &&
				isCheapCommand(deferredNormal.Payload) &&
				!deferredNormal.Priority &&
				len(c.backgroundStatSem) == cap(c.backgroundStatSem)
			req, ok, gotPriority = takePriority()
			if blockedDeferredStat && !gotPriority {
				select {
				case req, ok = <-c.hotPrioCh:
					gotPriority = true
				case req, ok = <-c.prioCh:
					gotPriority = true
				case <-c.backgroundStatFreed:
				case <-c.ctx.Done():
					<-c.inflightSem
					return
				}
			}
			if !gotPriority {
				selectedNormal := false
				if deferredNormal != nil {
					req, ok = deferredNormal, true
					deferredNormal = nil
					selectedNormal = true
				} else {
					select {
					case req, ok = <-c.hotReqCh:
						selectedNormal = true
					default:
						select {
						case req, ok = <-c.reqCh:
							selectedNormal = true
						default:
							select {
							case req, ok = <-c.hotPrioCh:
							case req, ok = <-c.prioCh:
							case req, ok = <-c.hotReqCh:
								selectedNormal = true
							case req, ok = <-c.reqCh:
								selectedNormal = true
							case <-c.ctx.Done():
								<-c.inflightSem
								return
							case <-idleCh:
								<-c.inflightSem
								c.waitForInflightDrain()
								return
							case <-keepaliveCh:
								didKeepalive = true
							}
						}
					}
				}
				if selectedNormal {
					if priorityReq, priorityOK, available := takePriority(); available {
						deferredNormal = req
						req, ok = priorityReq, priorityOK
					}
				}
			}
		} else {
			select {
			case req, ok = <-c.reqCh:
			case <-c.ctx.Done():
				<-c.inflightSem
				return
			case <-idleCh:
				<-c.inflightSem
				c.waitForInflightDrain()
				return
			case <-keepaliveCh:
				didKeepalive = true
			}
		}

		// Keepalive probe: send a lightweight command through the normal pipeline
		// so readerLoop can match the response in FIFO order.
		// inflightSem is already held; readerLoop releases it at line 1008.
		if didKeepalive {
			keepaliveCh = time.After(c.keepaliveInterval) // reset regardless of outcome
			kaCh := make(chan Response, 1)
			kaReq := &Request{
				Payload: []byte(c.keepaliveCommand + "\r\n"),
				RespCh:  kaCh,
				Ctx:     context.Background(),
			}
			kaReq.heldInflight = true
			if c.writeAdmitted(bw, kaReq) {
				return
			}
			select {
			case resp := <-kaCh:
				if resp.Err != nil || resp.StatusCode != keepaliveExpectedCode(c.keepaliveCommand) {
					_ = c.closeTransport()
					c.failOutstanding()
					return
				}
			case <-c.ctx.Done():
				return
			}
			continue
		}
		if !ok {
			<-c.inflightSem
			return
		}
		unsent = req
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}
		req.heldInflight = true

		// Reset idle timer since we got a request.
		if idleTimer != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(c.idleTimeout)
		}

		// Cancel before sending (queued-but-not-sent case)
		select {
		case <-req.Ctx.Done():
			unsent = nil
			c.releaseAdmittedRequest(req)
			finishUnadmittedRequest(req, c.providerID, req.Ctx.Err())
			continue
		default:
		}
		if req.FreshTransport && c.createdAt.Before(req.submittedAt) {
			unsent = nil
			c.releaseAdmittedRequest(req)
			finishUnadmittedRequest(req, c.providerID, errFreshTransportRequired)
			_ = c.waitForInflightSettlement()
			return
		}

		// Body-bearing requests additionally take a bodySem slot so concurrent
		// bodies stay bounded by Inflight even when the pipeline (inflightSem)
		// is deeper for STAT. Bodyless STAT skips this and pipelines deep.
		if !isCheapCommand(req.Payload) {
			select {
			case c.bodySem <- struct{}{}:
				req.heldBody = true
			case <-req.Ctx.Done():
				unsent = nil
				c.releaseAdmittedRequest(req)
				finishUnadmittedRequest(req, c.providerID, req.Ctx.Err())
				continue
			case <-c.ctx.Done():
				return
			}
		}
		if err := c.acquireBackgroundStat(req); err != nil {
			if errors.Is(err, errBackgroundStatWindowFull) {
				<-c.inflightSem
				req.heldInflight = false
				deferredNormal = req
				unsent = nil
				continue
			}
			unsent = nil
			c.releaseAdmittedRequest(req)
			finishUnadmittedRequest(req, c.providerID, err)
			continue
		}

		// per-request write deadline (cached to avoid redundant syscalls)
		dl, hasDL := req.writeDeadline()
		setWriteDeadline(dl, hasDL)

		unsent = nil
		if c.writeAdmitted(bw, req) {
			return
		}
	}
}

func (c *NNTPConnection) readerLoop() {
	for {
		select {
		case <-c.ctx.Done():
			c.failOutstanding()
			return
		default:
		}

		// Match FIFO request
		var req *Request
		select {
		case req = <-c.pending:
		case <-c.ctx.Done():
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}
		deferredCancellation := req.beginResponse()
		responseHeadTime := time.Now()
		req.responseHeadAt.Store(responseHeadTime.UnixNano())
		req.lifecycleMu.Lock()
		req.responseHeadTime = responseHeadTime
		req.lifecycleMu.Unlock()

		resp := Response{
			Request: req,
		}
		decoder := NNTPResponse{
			onMeta:             req.OnMeta,
			strictDecodeErrors: req.ValidateBody,
			decodeFn:           req.decodeFn,
		}

		// If the request is cancelled after send, drain only a bounded prefix of
		// a BODY response, then retire the socket.
		deliver := !deferredCancellation
		var drainDeadlineErr error
		if deferredCancellation && c.abandonedDrainTimeout > 0 {
			drainDeadlineErr = c.rb.forceDrainDeadline(c.conn, time.Now().Add(c.abandonedDrainTimeout))
		}

		out := req.BodyWriter
		if !deliver {
			out = io.Discard
		} else if out == nil {
			out = &resp.Body
		}
		if deliver && isStreamingCommand(req.Payload) {
			out = &attemptWriter{req: req, w: out}
		}

		// Allow us to switch output to io.Discard if the request is cancelled while
		// we are still draining the response.
		outRef := &writerRef{w: out}

		clock := newResponseClock(req.responseTimeout, c.stallTimeout)
		var drainStarted time.Time
		drainStartConsumed := 0
		deadlineOwner, err, terminalOwner, terminalErr := c.rb.feedUntilDoneControlledOwned(c.conn, &decoder, outRef, func(wireBytes, consumedBytes int) (time.Time, bool, readDeadlineOwner, int, error) {
			if drainDeadlineErr != nil {
				return time.Time{}, false, readDeadlineAbandonedDrain, 0, drainDeadlineErr
			}
			if deliver && req.currentCause() != nil {
				deliver = false
				outRef.w = io.Discard
				drainStarted = time.Now()
				drainStartConsumed = consumedBytes
			}
			if !deliver && (deferredCancellation || isStreamingCommand(req.Payload)) {
				if drainStarted.IsZero() {
					drainStarted = time.Now()
					drainStartConsumed = consumedBytes
				}
				if c.abandonedDrainTimeout > 0 && !time.Now().Before(drainStarted.Add(c.abandonedDrainTimeout)) {
					return time.Time{}, false, readDeadlineAbandonedDrain, 0, errAbandonedBodyDrainLimit
				}
				remaining := c.abandonedDrainBytes - (consumedBytes - drainStartConsumed)
				if c.abandonedDrainBytes > 0 && remaining <= 0 {
					return time.Time{}, false, readDeadlineAbandonedDrain, 0, errAbandonedBodyDrainLimit
				}
				if c.abandonedDrainTimeout > 0 {
					return drainStarted.Add(c.abandonedDrainTimeout), true, readDeadlineAbandonedDrain, max(remaining, 0), nil
				}
				return time.Time{}, false, readDeadlineAbandonedDrain, max(remaining, 0), nil
			}
			return clock.control(req.Ctx, wireBytes, consumedBytes)
		})
		retirementOwner, retirementErr := deadlineOwner, err
		if retirementErr == nil && terminalErr != nil {
			retirementOwner, retirementErr = terminalOwner, terminalErr
		}
		if err != nil {
			err = normalizeRequestReadError(req, deadlineOwner, err)
		} else if terminalErr != nil {
			req.recordDeadlineOwner(terminalOwner)
		}
		if err == nil && req.ValidateBody {
			err = decoder.validateBody()
			if err != nil && retirementErr == nil && !errors.Is(err, errBodyEncodingUnknown) {
				retirementOwner, retirementErr = readDeadlineNone, err
			}
		}
		if err != nil {
			if c.providerName != "" {
				resp.Err = fmt.Errorf("%s: %w", c.providerName, err)
			} else {
				resp.Err = err
			}
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder
		resp.ProviderID = c.providerID

		// Two-phase POST: coordinate with writeLoop via postReadyCh.
		if req.PostMode {
			if decoder.StatusCode == 340 && retirementErr == nil {
				if req.postReadyCh != nil {
					req.postReadyCh <- nil
				}
				if req.postWriteDone != nil {
					<-req.postWriteDone
				}
				decoder2 := NNTPResponse{}
				finalClock := newResponseClock(req.responseTimeout, c.stallTimeout)
				owner2, err2, terminalOwner2, terminalErr2 := c.rb.feedUntilDoneControlledOwned(c.conn, &decoder2, io.Discard, func(wireBytes, consumedBytes int) (time.Time, bool, readDeadlineOwner, int, error) {
					return finalClock.control(req.Ctx, wireBytes, consumedBytes)
				})
				retirementOwner, retirementErr = owner2, err2
				if retirementErr == nil && terminalErr2 != nil {
					retirementOwner, retirementErr = terminalOwner2, terminalErr2
					req.recordDeadlineOwner(terminalOwner2)
				}
				if err2 != nil {
					err2 = normalizeRequestReadError(req, owner2, err2)
					if c.providerName != "" {
						resp.Err = fmt.Errorf("%s: %w", c.providerName, err2)
					} else {
						resp.Err = err2
					}
				}
				resp.StatusCode = decoder2.StatusCode
				resp.Status = decoder2.Message
				resp.Meta = decoder2
			} else if req.postReadyCh != nil {
				// A rejection does not consume the one-shot body. A complete 340
				// accompanied by a terminal read error is likewise not safe to use.
				postErr := fmt.Errorf("post rejected: %d %s", decoder.StatusCode, decoder.Message)
				if decoder.StatusCode == 340 && retirementErr != nil {
					postErr = normalizeRequestReadError(req, retirementOwner, retirementErr)
					if c.providerName != "" {
						resp.Err = fmt.Errorf("%s: %w", c.providerName, postErr)
					} else {
						resp.Err = postErr
					}
				}
				req.postReadyCh <- postErr
				if req.postWriteDone != nil {
					<-req.postWriteDone
				}
			}
		}
		resp.Err = req.finalize(resp.Err)

		if c.stats != nil {
			n := int64(decoder.BytesConsumed)
			c.stats.BytesConsumed.Add(n)
			if c.stats.quotaBytes > 0 {
				if c.stats.quotaUsed.Add(n) >= c.stats.quotaBytes {
					c.stats.quotaExceeded.Store(true)
				}
			}
			switch classifyAttemptOutcome(req, resp.StatusCode, resp.Err) {
			case OutcomeCancellation, outcomeLocalFailure:
			case OutcomeSuccess:
				// Successful transfer: feed the TTFB and throughput EWMAs that
				// drive the adaptive attempt timeout and speed-aware dispatch.
				// firstByteAt is unset when the whole response arrived in a
				// single read; fall back to the read start. recordTTFB/Speed
				// ignore non-positive and sub-floor samples respectively.
				fb := clock.firstByte
				if fb.IsZero() {
					fb = clock.started
				}
				_, responseHeadTime := req.timingBoundaries()
				if !responseHeadTime.IsZero() {
					recordTTFB(c.stats, fb.Sub(responseHeadTime))
				} else if headAt := req.responseHeadAt.Load(); headAt != 0 {
					recordTTFB(c.stats, fb.Sub(time.Unix(0, headAt)))
				}
				recordSpeed(c.stats, n, time.Since(fb))
			default:
				if decoder.StatusCode == 430 || decoder.StatusCode == 423 {
					c.stats.Missing.Add(1)
				} else {
					c.stats.Errors.Add(1)
				}
			}
		}
		resp.Attempts = []AttemptEvidence{buildAttemptEvidence(req, c.providerID, resp, time.Now())}
		outcome := classifyAttemptOutcome(req, resp.StatusCode, resp.Err)
		completedDrain := retirementErr == nil && !deliver && req.needsDrainClear() && outcome == OutcomeCancellation
		retire := retirementErr != nil && !completedDrain
		if resp.Err != nil {
			var networkError net.Error
			retire = retire || errors.As(resp.Err, &networkError) && networkError.Timeout() ||
				errors.Is(resp.Err, ErrProtocolDesync)
		}
		retire = retire || resp.StatusCode == 502 ||
			(resp.StatusCode == 451 && (req.PostMode || isArticleOperation(req.Payload))) ||
			(retirementErr != nil && retirementOwner == readDeadlineAbandonedDrain)
		if retire {
			_ = c.closeTransport()
			c.failOutstanding()
		}
		c.completeFinalizedRequest(req, resp)
		if retire {
			return
		}
	}
}

// readOneResponse reads a complete NNTP response from the stream.
// Any unread bytes remain buffered in c.rbuf[c.rstart:c.rend] for subsequent reads.
func (c *NNTPConnection) readOneResponse(out io.Writer) (NNTPResponse, error) {
	resp := NNTPResponse{}
	_, err, _, terminalErr := c.rb.feedUntilDoneControlledOwned(c.conn, &resp, out, func(int, int) (time.Time, bool, readDeadlineOwner, int, error) {
		return time.Time{}, false, readDeadlineNone, 0, nil
	})
	if err != nil {
		return resp, err
	}
	if terminalErr != nil {
		return resp, terminalErr
	}
	return resp, nil
}

// DispatchStrategy controls how the client distributes requests across main providers.
type DispatchStrategy int

const (
	// DispatchRoundRobin distributes requests using dynamic weighted round-robin
	// based on each provider's available capacity. This is the default.
	DispatchRoundRobin DispatchStrategy = iota

	// DispatchFIFO sends all requests to the first provider that has capacity.
	// Overflow cascades to subsequent providers in declaration order.
	DispatchFIFO
)

// ClientOption configures optional Client behavior.
type ClientOption func(*clientConfig)

type clientConfig struct {
	dispatch              DispatchStrategy
	statProbeOff          bool
	speedAwareOff         bool
	circuitBreakerEnabled bool
	circuitBreakerClock   circuitBreakerClock
}

// WithDispatchStrategy sets the request distribution strategy for main providers.
// The default is DispatchRoundRobin.
func WithDispatchStrategy(s DispatchStrategy) ClientOption {
	return func(cfg *clientConfig) { cfg.dispatch = s }
}

// WithStatProbe enables or disables parallel STAT probing on 430 failover.
// When enabled (the default), after the first 430 response the remaining
// providers are probed concurrently with lightweight STAT commands; only the
// first provider that confirms article existence (223) receives the full
// request. This reduces "article missing on N providers" latency from
// sum-of-RTTs to max-of-RTTs.
func WithStatProbe(enabled bool) ClientOption {
	return func(cfg *clientConfig) { cfg.statProbeOff = !enabled }
}

// WithSpeedAwareDispatch enables or disables speed-aware weighting of the
// DispatchRoundRobin strategy. When enabled (the default), each provider's
// round-robin weight is scaled by its observed throughput so faster providers
// receive proportionally more traffic; available connection capacity still
// governs the base weight. Has no effect under DispatchFIFO.
func WithSpeedAwareDispatch(enabled bool) ClientOption {
	return func(cfg *clientConfig) { cfg.speedAwareOff = !enabled }
}

// WithProviderCircuitBreaker enables or disables the bounded in-memory
// provider circuit breaker. It is disabled by default for v4 behavioral
// compatibility. When enabled, three qualifying failures in 30 seconds open a
// provider and use exclusive half-open probes after 10, 20, 40, 80, and then
// 120 second cooldowns.
func WithProviderCircuitBreaker(enabled bool) ClientOption {
	return func(cfg *clientConfig) { cfg.circuitBreakerEnabled = enabled }
}

// withCircuitBreakerClock is an internal deterministic test seam. Production
// clients use the wall clock.
func withCircuitBreakerClock(clock circuitBreakerClock) ClientOption {
	return func(cfg *clientConfig) { cfg.circuitBreakerClock = clock }
}

// Provider describes a single NNTP server with its own credentials and connection count.
type Provider struct {
	// ID is the caller's stable transport identity for result and attempt
	// evidence. When empty, the existing resolved provider name is used.
	ID                     string
	Host                   string
	TLSConfig              *tls.Config
	Auth                   Auth
	Connections            int
	Inflight               int           // 0 defaults to 1; max concurrent BODY (and other body-bearing) commands per connection
	StatInflight           int           // 0 defaults to Inflight; deeper pipeline depth for bodyless STAT commands. Because STAT carries no payload, many can be in flight per connection at negligible memory cost, amortising round-trips. Set higher than Inflight (e.g. 50-100) for STAT-heavy workloads without inflating BODY memory.
	BackgroundStatInflight int           // 0 defaults to StatInflight; max ordinary (non-priority) STAT commands written per connection
	PriorityHeadroom       int           // pipeline slots per connection ordinary STAT cannot consume
	Factory                ConnFactory   // overrides Host/TLSConfig when set
	Backup                 bool          // if true, used only after all eligible main providers fail the request
	SkipPing               bool          // if true, skip the DATE ping on startup (for providers that don't support DATE)
	IdleTimeout            time.Duration // 0 means no idle disconnect
	ThrottleRestore        time.Duration // 0 defaults to 30s
	KeepAlive              time.Duration // TCP keep-alive interval; 0 defaults to 30s; negative disables
	ReconnectDelay         time.Duration // 0 disables auto-reconnect after 502; when set, re-adds provider after this delay

	// AttemptTimeout bounds time-to-first-response-byte starting only when the
	// request becomes response head on its NNTP connection. Pool admission and
	// pipeline wait remain governed by the caller context; body progress after
	// the first byte is governed by StallTimeout. Zero selects an adaptive value.
	AttemptTimeout time.Duration

	// StallTimeout is the rolling progress deadline for a body transfer: once
	// bytes are flowing, the read deadline is extended by StallTimeout on each
	// chunk of progress, so a slow-but-healthy download never times out while a
	// truly stalled one is torn down. 0 defaults to 8s; negative disables it
	// (only the caller's context deadline applies).
	StallTimeout time.Duration

	// AbandonedBodyDrainBytes and AbandonedBodyDrainTimeout bound cleanup after
	// a sent BODY request is canceled. Reaching either bound retires the
	// connection rather than letting obsolete payload block newer work. Zero
	// selects the conservative library default for that bound.
	AbandonedBodyDrainBytes   int
	AbandonedBodyDrainTimeout time.Duration

	// KeepaliveInterval, if non-zero, sends a lightweight NNTP command
	// periodically when the connection is idle, to detect zombie connections
	// before a real request arrives. Recommended: 30s–60s.
	// Disabled when SkipPing is true and KeepaliveCommand is empty.
	KeepaliveInterval time.Duration

	// KeepaliveCommand is the NNTP command sent as a keepalive probe.
	// Defaults to "DATE" (response 111). Use "HELP" (response 100) or
	// "CAPABILITIES" (response 101) for providers that do not support DATE.
	// Ignored when KeepaliveInterval is 0.
	KeepaliveCommand string

	// UserAgent identifies this client to the NNTP server. Empty string disables it.
	UserAgent string

	// QuotaBytes is the maximum number of bytes that may be downloaded from this
	// provider per QuotaPeriod. 0 means unlimited.
	QuotaBytes int64

	// QuotaPeriod is the rolling window after which the quota counter resets.
	// 0 means the quota never resets (lifetime cap).
	// Typical value: 30 * 24 * time.Hour  (≈ monthly)
	QuotaPeriod time.Duration

	// QuotaUsed is the number of bytes already consumed at startup.
	// Set this on restart to restore quota state from a previous run.
	// Read the current value from [ProviderStats.QuotaUsed] before shutting down.
	QuotaUsed int64

	// QuotaResetAt, if non-zero, overrides the quota period reset deadline on startup.
	// Set this on restart to restore the reset deadline from a previous run.
	// Read the current value from [ProviderStats.QuotaResetAt] before shutting down.
	// Ignored when QuotaPeriod is 0 or the time is in the past.
	QuotaResetAt time.Time
}

type providerGroup struct {
	name      string
	id        string
	host      string // raw Provider.Host; empty for Factory-based providers
	maxConns  int
	ctx       context.Context // cancelled on removal/close
	reqCh     chan *Request
	prioCh    chan *Request // priority requests; connections prefer this over reqCh
	hotReqCh  chan *Request // unbuffered; hot (connected) connections read this
	hotPrioCh chan *Request // unbuffered; hot priority connections read this
	gate      *connGate
	breaker   *providerCircuitBreaker
	stats     providerStats
	cancel    context.CancelFunc // cancels this group's slot goroutines
	p         Provider           // original config; used for auto-reconnect

	// Quota period configuration. quotaBytes/quotaUsed/quotaExceeded live in
	// stats so that NNTPConnection can update them via its *providerStats pointer.
	quotaPeriod  time.Duration // 0 = no auto-reset
	quotaResetAt atomic.Int64  // Unix nanoseconds of next reset; 0 = never
}

// attemptTimeout returns the response-head time-to-first-byte bound. An
// explicit Provider.AttemptTimeout wins; otherwise it adapts to the
// provider's observed time-to-first-byte EWMA (seeded from the ping RTT) as
// 4×TTFB, clamped to [minAttemptTimeout, maxAttemptTimeout]. With no sample yet
// it falls back to minAttemptTimeout, preserving the historical 2s behavior.
func (g *providerGroup) attemptTimeout() time.Duration {
	if g.p.AttemptTimeout > 0 {
		return g.p.AttemptTimeout
	}
	ttfb := g.stats.ttfbEWMA.Load()
	if ttfb <= 0 {
		return minAttemptTimeout
	}
	d := time.Duration(ttfb) * 4
	if d < minAttemptTimeout {
		return minAttemptTimeout
	}
	if d > maxAttemptTimeout {
		return maxAttemptTimeout
	}
	return d
}

// isQuotaExceeded reports whether this provider has consumed its download quota
// for the current period.
//
// Fast path (quota not exceeded): single atomic.Bool load (~1 ns).
// Slow path (flag set, period elapsed): resets counters and returns false.
// The time.Now() call is deferred until the cached flag is actually set.
func (g *providerGroup) isQuotaExceeded() bool {
	if g.stats.quotaBytes <= 0 {
		return false // unlimited
	}
	if !g.stats.quotaExceeded.Load() {
		return false // fast path: quota not yet hit
	}
	// Flag is set. If a reset period is configured, check whether it has elapsed.
	if g.quotaPeriod > 0 {
		resetAt := g.quotaResetAt.Load()
		if resetAt > 0 && time.Now().UnixNano() >= resetAt {
			g.stats.quotaUsed.Store(0)
			g.stats.quotaExceeded.Store(false)
			g.quotaResetAt.Store(time.Now().Add(g.quotaPeriod).UnixNano())
			return false
		}
	}
	return true
}

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	registry   atomic.Pointer[providerRegistry]
	registryMu sync.Mutex
	closed     bool

	// Package tests written before the registry correction inspect these
	// mirrors directly. Production code reads registry instead.
	mainGroups   atomic.Pointer[[]*providerGroup]
	backupGroups atomic.Pointer[[]*providerGroup]
	nextIdx      atomic.Uint64 // round-robin counter for mainGroups

	dispatch     DispatchStrategy // set once by NewClient, read-only after
	statProbe    bool             // set once by NewClient; enables parallel STAT probing on 430
	speedAware   bool             // set once by NewClient; weights round-robin dispatch by throughput
	breakerOn    bool
	breakerClock circuitBreakerClock

	nextGenerated           uint64
	nextLifecycleGeneration uint64
	// decodeFn is copied to each request when non-nil. It remains unexported so
	// production callers cannot replace the transport decoder.
	decodeFn func(dst, src []byte, state *rapidyenc.State) (nDst, nSrc int, end rapidyenc.End, err error)

	startTime time.Time
	wg        sync.WaitGroup
}

// parseDateResponse parses an NNTP DATE response message.
// message is the full status line, e.g. "111 20240315120000".
func parseDateResponse(message string) (time.Time, error) {
	// Skip "111 " prefix if present.
	ts := message
	if len(ts) > 4 && ts[3] == ' ' {
		ts = ts[4:]
	}
	if len(ts) < 14 {
		return time.Time{}, fmt.Errorf("nntp: DATE response too short: %q", message)
	}
	return time.Parse("20060102150405", ts[:14])
}

// pingProvider dials a temporary connection, authenticates, sends DATE, and
// measures RTT. The connection is always closed before returning.
func pingProvider(ctx context.Context, factory ConnFactory, auth Auth) PingResult {
	conn, err := factory(ctx)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping dial: %w", err)}
	}
	if conn == nil {
		return PingResult{Err: fmt.Errorf("ping dial: factory returned nil connection")}
	}
	defer func() { _ = conn.Close() }()

	nc := &NNTPConnection{
		conn: conn,
		rb:   readBuffer{buf: make([]byte, defaultReadBufSize)},
	}

	// Read greeting.
	greeting, err := nc.readOneResponse(io.Discard)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping greeting: %w", err)}
	}
	if greeting.StatusCode != 200 && greeting.StatusCode != 201 {
		return PingResult{Err: &greetingError{StatusCode: greeting.StatusCode, Message: greeting.Message}}
	}

	// Auth if needed.
	if auth.Username != "" {
		if err := nc.auth(auth); err != nil {
			return PingResult{Err: fmt.Errorf("ping auth: %w", err)}
		}
	}

	// Send DATE and measure RTT.
	start := time.Now()
	if _, err := conn.Write([]byte("DATE\r\n")); err != nil {
		return PingResult{Err: fmt.Errorf("ping write DATE: %w", err)}
	}
	resp, err := nc.readOneResponse(io.Discard)
	rtt := time.Since(start)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping read DATE: %w", err)}
	}
	if resp.StatusCode != 111 {
		return PingResult{Err: fmt.Errorf("ping DATE unexpected status: %d %s", resp.StatusCode, resp.Message)}
	}

	serverTime, err := parseDateResponse(resp.Message)
	if err != nil {
		return PingResult{RTT: rtt, Err: err}
	}
	return PingResult{RTT: rtt, ServerTime: serverTime}
}

// TestProvider dials the given provider, performs greeting + authentication +
// DATE, and returns the result. It is completely independent of Client/pool.
func TestProvider(ctx context.Context, p Provider) PingResult {
	factory := p.Factory
	if factory == nil {
		host := p.Host
		tlsCfg := p.TLSConfig
		keepAlive := p.KeepAlive
		factory = func(ctx context.Context) (net.Conn, error) {
			return newNetConn(ctx, host, tlsCfg, keepAlive)
		}
	}
	return pingProvider(ctx, factory, p.Auth)
}

// resolveProviderName builds a unique name for a provider based on host and auth.
func resolveProviderName(p Provider, index int) string {
	if p.Host != "" {
		if p.Auth.Username != "" {
			return p.Host + "+" + p.Auth.Username
		}
		return p.Host
	}
	return fmt.Sprintf("provider-%d", index)
}

// pingResolvedProvider performs the optional startup probe without publishing
// a group or touching the Client WaitGroup.
func (c *Client) pingResolvedProvider(ctx context.Context, spec resolvedProvider) PingResult {
	if spec.provider.SkipPing {
		return PingResult{}
	}
	factory := spec.provider.Factory
	if factory == nil {
		host := spec.provider.Host
		tlsCfg := spec.provider.TLSConfig
		keepAlive := spec.provider.KeepAlive
		factory = func(ctx context.Context) (net.Conn, error) {
			return newNetConn(ctx, host, tlsCfg, keepAlive)
		}
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, defaultHandshakeTimeout)
	result := pingProvider(pingCtx, factory, spec.provider.Auth)
	pingCancel()
	return result
}

// startProviderGroup creates a providerGroup and launches its connection slots.
// Any optional startup ping must already have completed.
func (c *Client) startProviderGroup(spec resolvedProvider, ping PingResult) *providerGroup {
	p := spec.provider
	inflight := p.Inflight
	if inflight <= 0 {
		inflight = 1
	}
	// STAT (bodyless) may pipeline deeper than BODY. The overall pipeline cap is
	// max(Inflight, StatInflight); 0 or a smaller value means "same as Inflight"
	// (no separate STAT lane — fully backward compatible).
	statInflight := p.StatInflight
	if statInflight < inflight {
		statInflight = inflight
	}
	priorityHeadroom := max(p.PriorityHeadroom, 0)
	if priorityHeadroom >= statInflight {
		priorityHeadroom = statInflight - 1
	}
	backgroundStatInflight := p.BackgroundStatInflight
	if backgroundStatInflight <= 0 {
		backgroundStatInflight = statInflight
	}
	backgroundStatInflight = min(backgroundStatInflight, statInflight-priorityHeadroom)

	factory := p.Factory
	if factory == nil {
		host := p.Host
		tlsCfg := p.TLSConfig
		keepAlive := p.KeepAlive
		factory = func(ctx context.Context) (net.Conn, error) {
			return newNetConn(ctx, host, tlsCfg, keepAlive)
		}
	}

	name := spec.name
	gate := newConnGate(p.Connections, p.ThrottleRestore)
	gctx, gcancel := context.WithCancel(c.ctx)

	g := &providerGroup{
		name:        name,
		id:          spec.id,
		host:        p.Host,
		maxConns:    p.Connections,
		ctx:         gctx,
		reqCh:       make(chan *Request, p.Connections),
		prioCh:      make(chan *Request, p.Connections),
		hotReqCh:    make(chan *Request),
		hotPrioCh:   make(chan *Request),
		gate:        gate,
		breaker:     newProviderCircuitBreaker(c.breakerOn, c.breakerClock),
		cancel:      gcancel,
		p:           p,
		quotaPeriod: p.QuotaPeriod,
	}
	g.stats.quotaBytes = p.QuotaBytes
	g.stats.pipelineLimit = statInflight * p.Connections
	g.stats.backgroundStatLimit = backgroundStatInflight * p.Connections
	g.stats.priorityHeadroom = priorityHeadroom * p.Connections
	g.stats.Ping = ping
	if ping.Err == nil && ping.RTT > 0 {
		g.stats.ttfbEWMA.Store(int64(ping.RTT))
	}
	if p.QuotaBytes > 0 {
		if p.QuotaUsed > 0 {
			g.stats.quotaUsed.Store(p.QuotaUsed)
			if p.QuotaUsed >= p.QuotaBytes {
				g.stats.quotaExceeded.Store(true)
			}
		}
		if p.QuotaPeriod > 0 {
			if !p.QuotaResetAt.IsZero() && p.QuotaResetAt.After(time.Now()) {
				g.quotaResetAt.Store(p.QuotaResetAt.UnixNano())
			} else {
				g.quotaResetAt.Store(time.Now().Add(p.QuotaPeriod).UnixNano())
			}
		}
	}

	// Resolve the rolling stall timeout: 0 => default, negative => disabled.
	stall := p.StallTimeout
	if stall == 0 {
		stall = defaultStallTimeout
	} else if stall < 0 {
		stall = 0
	}
	drainBytes := p.AbandonedBodyDrainBytes
	if drainBytes <= 0 {
		drainBytes = defaultAbandonedBodyDrainBytes
	}
	drainTimeout := p.AbandonedBodyDrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultAbandonedBodyDrainTimeout
	}

	// Resolve keepalive settings. If SkipPing is true and no explicit command
	// is set, keepalive is disabled (we don't know which command the server supports).
	kaInterval := p.KeepaliveInterval
	kaCmd := p.KeepaliveCommand
	if kaInterval > 0 {
		if kaCmd == "" {
			if p.SkipPing {
				kaInterval = 0 // disable: no safe probe command known
			} else {
				kaCmd = "DATE"
			}
		}
	}

	for range p.Connections {
		c.wg.Add(1)
		go runConnSlot(gctx, g.reqCh, g.prioCh, g.hotReqCh, g.hotPrioCh, factory, inflight, statInflight, backgroundStatInflight, p.Auth, p.UserAgent, p.IdleTimeout, stall, drainBytes, drainTimeout, kaInterval, kaCmd, gate, &g.stats, safeIdentityText(name), g.id, &c.wg)
	}

	return g
}

func NewClient(ctx context.Context, providers []Provider, opts ...ClientOption) (*Client, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("nntp: at least one provider is required")
	}

	// Require at least one main (non-backup) provider.
	hasMain := false
	for _, p := range providers {
		if !p.Backup {
			hasMain = true
			break
		}
	}
	if !hasMain {
		return nil, fmt.Errorf("nntp: at least one non-backup provider is required")
	}

	// Resolve and reserve the complete identity namespace before any provider
	// factory can dial.
	resolved, owners, err := resolveInitialProviders(providers)
	if err != nil {
		return nil, err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	var cfg clientConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c := &Client{
		ctx:           ctx,
		cancel:        cancel,
		dispatch:      cfg.dispatch,
		statProbe:     !cfg.statProbeOff,
		speedAware:    !cfg.speedAwareOff,
		breakerOn:     cfg.circuitBreakerEnabled,
		breakerClock:  cfg.circuitBreakerClock,
		nextGenerated: 1,
		startTime:     time.Now(),
	}
	registry := newProviderRegistry()
	registry.ownerByToken = owners

	for _, spec := range resolved {
		c.registryMu.Lock()
		generation := c.nextProviderGenerationLocked()
		c.registryMu.Unlock()
		group := c.startProviderGroup(spec, c.pingResolvedProvider(c.ctx, spec))
		registry.byID[spec.id] = providerRegistration{
			spec:       spec,
			group:      group,
			generation: generation,
		}
		registry.orderedIDs = append(registry.orderedIDs, spec.id)
	}
	c.registryMu.Lock()
	c.publishRegistryLocked(registry)
	c.registryMu.Unlock()

	return c, nil
}

// Close cancels the client, stops all provider gates, and waits for all
// connection slots to stop. Slots manage their own TCP connection cleanup.
// Context cancellation (c.cancel) cascades to all group contexts, so closing
// reqCh is unnecessary and avoids a race with stale-snapshot senders.
func (c *Client) Close() error {
	c.cancel()
	registry := c.closeRegistry()
	for _, groups := range [...][]*providerGroup{registry.mains, registry.backups} {
		for _, group := range groups {
			group.gate.stop()
		}
	}
	c.wg.Wait()
	return nil
}

func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}

	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}

	go c.sendWithRetry(ctx, payload, bodyWriter, metaFn, respCh)
	return respCh
}

// SendPriority is like Send but enqueues the request on the priority channel,
// so idle connections will pick it up before normal requests.
func (c *Client) SendPriority(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}

	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}

	go c.doSendWithRetry(ctx, payload, bodyWriter, metaFn, respCh, true, false)
	return respCh
}

func (c *Client) sendValidatedBody(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), priority bool) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	go c.doSendWithRetry(ctx, payload, bodyWriter, onMeta, respCh, priority, true)
	return respCh
}

func (c *Client) requestCancellation(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return c.ctx.Err()
}

// extractProbeMsgID returns the "<id@host>" message-ID from a BODY, HEAD, or
// ARTICLE payload, or nil when the payload has no message-ID (GROUP, DATE, …)
// or when the command is already STAT or POST (probing would be redundant or
// inapplicable).
func extractProbeMsgID(payload []byte) []byte {
	switch operationFromPayload(payload) {
	case OperationBody, OperationHead, operationArticle:
	default:
		return nil
	}
	open := bytes.IndexByte(payload, '<')
	if open < 0 {
		return nil
	}
	close := bytes.IndexByte(payload[open:], '>')
	if close < 0 {
		return nil
	}
	return payload[open : open+close+1]
}

// probeResult carries the outcome of one parallel STAT probe.
type probeResult struct {
	index     int
	g         *providerGroup
	resp      Response
	ok        bool
	cancelled bool
}

// raceCandidates probes candidates in parallel with STAT on the priority lane,
// then sends the real payload to the first provider that confirms 223.
// All-miss latency is max-of-RTTs instead of sum-of-RTTs.
//
// Note: when all probes miss, respCh is NOT written; the caller must deliver
// the saved 430 response from the first provider that triggered the race.
func (c *Client) raceCandidates(
	ctx context.Context,
	candidates []*providerGroup,
	statPayload, payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	validateBody bool,
	attempts *[]AttemptEvidence,
	respCh chan<- Response,
) (delivered, cancelled bool, lastErr error) {
	if err := c.requestCancellation(ctx); err != nil {
		return false, true, err
	}
	// Filter to live candidates. Every configured provider remains independently
	// eligible even when multiple accounts share one endpoint: co-location is
	// not evidence that retention, authorization, or article availability agree.
	live := make([]*providerGroup, 0, len(candidates))
	for _, g := range candidates {
		if err := c.requestCancellation(ctx); err != nil {
			return false, true, err
		}
		if g.isQuotaExceeded() {
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrQuotaExceeded)
			*attempts = append(*attempts, buildEligibilityEvidence(payload, g.id, lastErr, validateBody))
			continue
		}
		live = append(live, g)
	}

	if len(live) == 0 {
		return false, false, lastErr
	}

	// Single live candidate: skip the probe RTT and send the real payload directly.
	if len(live) == 1 {
		g := live[0]
		resp, ok, done := c.tryGroupResilient(ctx, g, payload, bodyWriter, onMeta, true, validateBody, false)
		*attempts = append(*attempts, resp.Attempts...)
		if done {
			return false, true, lastErr
		}
		if !ok {
			return false, false, lastErr
		}
		if resp.Err != nil {
			if bodyWriter != nil && attemptCommittedResp(resp) {
				resp.Attempts = cloneAttempts(*attempts)
				respCh <- resp
				return true, false, nil
			}
			return false, false, resp.Err
		}
		if resp.StatusCode == 502 {
			c.retireUnavailableProvider(g)
			return false, false, fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrServiceUnavailable)
		}
		if resp.StatusCode == 430 || resp.StatusCode == 423 {
			c.nextIdx.Add(1)
			return false, false, lastErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			return false, false, fmt.Errorf("%s: %w", safeIdentityText(g.name), toError(resp.StatusCode, resp.Status))
		}
		resp.Attempts = cloneAttempts(*attempts)
		respCh <- resp
		return true, false, lastErr
	}

	// ≥2 candidates: probe all in parallel.
	results := make(chan probeResult, len(live))
	for index, g := range live {
		go func(index int, g *providerGroup) {
			resp, ok, done := c.tryGroupResilient(ctx, g, statPayload, nil, nil, true, false, false)
			results <- probeResult{index: index, g: g, resp: resp, ok: ok, cancelled: done}
		}(index, g)
	}

	// Collect ALL probe results before acting on the winner, so that side
	// effects like 502 provider removal are applied regardless of order.
	ordered := make([]probeResult, len(live))
	for range live {
		pr := <-results
		ordered[pr.index] = pr
	}
	var winners []*providerGroup
	for _, pr := range ordered {
		*attempts = append(*attempts, pr.resp.Attempts...)
		if pr.cancelled {
			cancelled = true
			continue
		}
		if !pr.ok {
			continue
		}
		if pr.resp.Err != nil {
			lastErr = pr.resp.Err
			continue
		}
		g := pr.g
		switch pr.resp.StatusCode {
		case 502:
			c.retireUnavailableProvider(g)
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrServiceUnavailable)
		case 430, 423:
			c.nextIdx.Add(1)
		case 223:
			winners = append(winners, g)
		default:
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), toError(pr.resp.StatusCode, pr.resp.Status))
		}
	}

	if cancelled {
		return false, true, lastErr
	}

	if len(winners) == 0 {
		return false, false, lastErr
	}

	// Try STAT-positive providers in configured order. A faster later probe may
	// not override the preferred provider, and corrupt/expired winners advance.
	for _, winner := range winners {
		resp, ok, done := c.tryGroupResilient(ctx, winner, payload, bodyWriter, onMeta, true, validateBody, false)
		*attempts = append(*attempts, resp.Attempts...)
		if done {
			return false, true, lastErr
		}
		if !ok {
			continue
		}
		if resp.Err != nil {
			if bodyWriter != nil && attemptCommittedResp(resp) {
				resp.Attempts = cloneAttempts(*attempts)
				respCh <- resp
				return true, false, nil
			}
			lastErr = resp.Err
			continue
		}
		if resp.StatusCode == 430 || resp.StatusCode == 423 {
			c.nextIdx.Add(1)
			continue
		}
		if resp.StatusCode == 502 {
			c.retireUnavailableProvider(winner)
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(winner.name), ErrServiceUnavailable)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(winner.name), toError(resp.StatusCode, resp.Status))
			continue
		}
		resp.Attempts = cloneAttempts(*attempts)
		respCh <- resp
		return true, false, lastErr
	}
	return false, false, lastErr
}

type responseClock struct {
	started, firstByte, responseDeadline, stallDeadline time.Time
	stall                                               time.Duration
	wire, consumed                                      int
}

func newResponseClock(responseTimeout, stall time.Duration) responseClock {
	now := time.Now()
	clock := responseClock{started: now, stall: stall}
	if responseTimeout > 0 {
		clock.responseDeadline = now.Add(responseTimeout)
	}
	return clock
}

func (clock *responseClock) control(ctx context.Context, wire, consumed int) (time.Time, bool, readDeadlineOwner, int, error) {
	if wire > clock.wire || consumed > clock.consumed {
		now := time.Now()
		if clock.firstByte.IsZero() {
			clock.firstByte = now
		}
		clock.wire, clock.consumed = wire, consumed
		if clock.stall > 0 {
			deadline := now.Add(clock.stall)
			quantum := min(stallDeadlineQuantum, clock.stall/2)
			if quantum <= 0 || clock.stallDeadline.IsZero() ||
				!clock.stallDeadline.After(now) || deadline.Sub(clock.stallDeadline) >= quantum {
				clock.stallDeadline = deadline
			}
		}
	}
	caller, hasCaller := ctx.Deadline()
	deadline, owner := clock.responseDeadline, readDeadlineProviderResponse
	if wire > 0 || consumed > 0 {
		deadline, owner = clock.stallDeadline, readDeadlineProviderStall
	}
	if deadline.IsZero() {
		if hasCaller {
			return caller, true, readDeadlineCaller, 0, nil
		}
		return time.Time{}, false, readDeadlineNone, 0, nil
	}
	if hasCaller && caller.Before(deadline) {
		return caller, true, readDeadlineCaller, 0, nil
	}
	return deadline, true, owner, 0, nil
}

// writeDeadline keeps the caller's end-to-end deadline active during writes.
// Provider response timing begins later, at response-head admission.
func (req *Request) writeDeadline() (time.Time, bool) {
	return req.Ctx.Deadline()
}

// tryGroup dispatches a single request to a provider group and waits for the
// response. priority=true routes through the priority channels.
//
// The caller context governs pool admission and pipeline wait. The separate
// provider response timeout begins in readerLoop only at FIFO response-head
// admission, so local queueing cannot be classified as provider latency.
func (c *Client) tryGroup(
	ctx context.Context,
	g *providerGroup,
	payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	priority bool,
	validateBody bool,
	freshTransport bool,
) (resp Response, ok bool, done bool) {
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()

	innerCh := make(chan Response, 1)
	req := &Request{
		Ctx:             reqCtx,
		Payload:         payload,
		RespCh:          innerCh,
		BodyWriter:      bodyWriter,
		ValidateBody:    validateBody,
		FreshTransport:  freshTransport,
		Priority:        priority,
		OnMeta:          onMeta,
		decodeFn:        c.decodeFn,
		submittedAt:     time.Now(),
		responseTimeout: g.attemptTimeout(),
		transportCause:  func() error { return c.groupCause(g) },
	}

	var hotCh chan *Request
	var coldCh chan *Request
	if priority {
		hotCh = g.hotPrioCh
		coldCh = g.prioCh
	} else {
		hotCh = g.hotReqCh
		coldCh = g.reqCh
	}

	select {
	case hotCh <- req:
	default:
		select {
		case <-c.ctx.Done():
			req.stopBeforeTransport(c.ctx.Err())
			return failedAttempt(req, g.id, c.ctx.Err()), false, true
		case <-reqCtx.Done():
			req.stopBeforeTransport(reqCtx.Err())
			return failedAttempt(req, g.id, reqCtx.Err()), false, ctx.Err() != nil
		case <-g.ctx.Done():
			req.stopBeforeTransport(ErrConnectionDied)
			return failedAttempt(req, g.id, ErrConnectionDied), false, false
		case coldCh <- req:
		}
	}

	resp, ok, cause, done := c.awaitAttempt(reqCtx, g, req, innerCh)
	if cause != nil {
		return failedAttempt(req, g.id, cause), false, done
	}
	if ok && len(resp.Attempts) == 0 && !errors.Is(resp.Err, errFreshTransportRequired) {
		resp.Request = req
		resp.ProviderID = g.id
		resp.Attempts = []AttemptEvidence{buildAttemptEvidence(req, g.id, resp, time.Now())}
	}
	return resp, ok, false
}

// attemptCommittedResp reports whether the response came from an attempt that
// had already started streaming bytes (the reader committed). Such an attempt
// must not be retried or failed over when a caller-supplied writer is in use.
func attemptCommittedResp(resp Response) bool {
	return resp.Request != nil && resp.Request.committed()
}

type setupResponseCoder interface {
	setupResponseCode() int
}

func attemptPhaseDurations(submitted, written, responseHead, completed time.Time) (poolQueue, headWait, service time.Duration) {
	if !submitted.IsZero() {
		if completed.Before(submitted) {
			completed = submitted
		}
		if written.IsZero() {
			return completed.Sub(submitted), 0, 0
		}
		if written.Before(submitted) {
			written = submitted
		} else if written.After(completed) {
			written = completed
		}
		poolQueue = written.Sub(submitted)
		if responseHead.IsZero() {
			return poolQueue, completed.Sub(written), 0
		}
		if responseHead.Before(written) {
			responseHead = written
		} else if responseHead.After(completed) {
			responseHead = completed
		}
		return poolQueue, responseHead.Sub(written), completed.Sub(responseHead)
	}

	if !written.IsZero() {
		if responseHead.IsZero() {
			return 0, max(completed.Sub(written), 0), 0
		}
		headWait = max(responseHead.Sub(written), 0)
	}
	if !responseHead.IsZero() {
		service = max(completed.Sub(responseHead), 0)
	}
	return 0, headWait, service
}

func buildAttemptEvidence(req *Request, providerID string, resp Response, completedAt time.Time) AttemptEvidence {
	cause := resp.Err
	if cause == nil {
		cause = toError(resp.StatusCode, resp.Status)
	}
	responseCode := resp.StatusCode
	if responseCode == 0 && cause != nil {
		var setupErr setupResponseCoder
		if errors.As(cause, &setupErr) {
			responseCode = setupErr.setupResponseCode()
		}
	}
	outcome := classifyAttemptOutcome(req, responseCode, cause)
	validation := BodyValidationNotApplicable
	if operationFromPayload(req.Payload) == OperationBody {
		if !req.ValidateBody {
			validation = BodyValidationNotRequested
		} else {
			switch outcome {
			case OutcomeSuccess:
				validation = BodyValidationValid
			case OutcomeCorruptBody:
				validation = BodyValidationInvalid
			default:
				if resp.StatusCode == nntpBody || resp.Err != nil {
					validation = BodyValidationIncomplete
				}
			}
		}
	}

	writtenTime, responseHeadTime := req.timingBoundaries()
	writtenAt := req.writtenAt.Load()
	headAt := req.responseHeadAt.Load()
	if writtenTime.IsZero() && writtenAt > 0 {
		writtenTime = time.Unix(0, writtenAt)
	}
	if responseHeadTime.IsZero() && headAt > 0 {
		responseHeadTime = time.Unix(0, headAt)
	}
	poolQueue, headWait, service := attemptPhaseDurations(req.submittedAt, writtenTime, responseHeadTime, completedAt)

	providerResponseTimeout := headAt > 0 && req.providerDeadlineExpired(resp.Err)
	return AttemptEvidence{
		ProviderID:               providerID,
		Operation:                operationFromPayload(req.Payload),
		Outcome:                  outcome,
		ResponseCode:             responseCode,
		BodyValidation:           validation,
		Cause:                    cause,
		ProviderResponseTimeout:  providerResponseTimeout,
		PoolQueueDuration:        max(poolQueue, 0),
		PipelineHeadWaitDuration: max(headWait, 0),
		ResponseServiceDuration:  max(service, 0),
	}
}

func buildEligibilityEvidence(payload []byte, providerID string, cause error, validateBody bool) AttemptEvidence {
	req := &Request{Payload: payload, ValidateBody: validateBody}
	return buildAttemptEvidence(req, providerID, Response{Err: cause}, time.Now())
}

// maxSpeedScore is the highest multiplier speed-aware dispatch applies to a
// provider's base (capacity) weight.
const maxSpeedScore = 4

// dispatchWeights computes cumulative round-robin weights for the given main
// providers. The base weight is each provider's available connection capacity
// (min 1 when live); quota-exceeded providers get weight 0. When speedAware is
// true the base weight is scaled by speedScore so faster providers receive
// proportionally more traffic. With no throughput samples this reduces to pure
// capacity weighting (the historical behavior).
func dispatchWeights(mains []*providerGroup, speedAware bool) (cum []int, total int) {
	cum = make([]int, len(mains))
	var maxSpeed float64
	if speedAware {
		for _, g := range mains {
			if s := speedEWMABytesPerSec(&g.stats); s > maxSpeed {
				maxSpeed = s
			}
		}
	}
	for i, g := range mains {
		w := 0
		if !g.isQuotaExceeded() {
			w = max(1, int(g.gate.available.Load()))
			if speedAware && maxSpeed > 0 {
				w *= speedScore(speedEWMABytesPerSec(&g.stats), maxSpeed)
			}
		}
		total += w
		cum[i] = total
	}
	return cum, total
}

// speedScore maps a provider's throughput to an integer multiplier in
// [1, maxSpeedScore] relative to the fastest provider. An unmeasured provider
// (speed 0) scores the maximum so it is not starved before it has a sample.
func speedScore(speed, maxSpeed float64) int {
	if speed <= 0 {
		return maxSpeedScore
	}
	s := int(float64(maxSpeedScore)*speed/maxSpeed + 0.5)
	if s < 1 {
		return 1
	}
	if s > maxSpeedScore {
		return maxSpeedScore
	}
	return s
}

func (c *Client) sendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response) {
	c.doSendWithRetry(ctx, payload, bodyWriter, onMeta, respCh, false, false)
}

// tryGroupResilient retries a single provider on a fresh connection when a
// pooled connection dies mid-request (stale socket the server already
// closed). Without this, a single-provider pool fails immediately with
// "all providers exhausted: ... connection died" because there is no next
// provider to fall back to. Bounded so a genuinely-down server still fails
// fast. Only transport-level connection death is retried (see
// isConnectionDeathError); 430/502/quota and provider removal (!ok) keep
// their existing behavior.
func (c *Client) tryGroupResilient(
	ctx context.Context,
	g *providerGroup,
	payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	priority bool,
	validateBody bool,
	freshTransport bool,
) (resp Response, ok bool, cancelled bool) {
	lease, breakerErr := g.breaker.acquire(g.id)
	if breakerErr != nil {
		// Preserve PR1 cancellation precedence when cancellation races with
		// breaker eligibility. An open provider must not hide caller shutdown.
		cancellationResp := func(err error) Response {
			return Response{
				Err:        err,
				ProviderID: g.id,
				Attempts:   []AttemptEvidence{buildEligibilityEvidence(payload, g.id, err, validateBody)},
			}
		}
		if err := ctx.Err(); err != nil {
			return cancellationResp(err), false, true
		}
		if err := c.ctx.Err(); err != nil {
			return cancellationResp(err), false, true
		}
		resp = Response{
			Err:        breakerErr,
			ProviderID: g.id,
			Attempts:   []AttemptEvidence{buildEligibilityEvidence(payload, g.id, breakerErr, validateBody)},
		}
		return resp, true, false
	}
	if lease.probe {
		// Half-open is a transport recovery probe, not merely another request
		// on a socket that predates the breaker cooldown.
		freshTransport = true
	}
	defer func() {
		g.breaker.complete(lease, classifyCircuitBreakerCompletion(resp, ok, cancelled))
	}()

	var attempts []AttemptEvidence
	connRetries := 0
	temporaryRetried := false
	for {
		resp, ok, cancelled = c.tryGroup(ctx, g, payload, bodyWriter, onMeta, priority, validateBody, freshTransport)
		attempts = append(attempts, resp.Attempts...)
		if cancelled || !ok {
			resp.Attempts = cloneAttempts(attempts)
			return
		}
		// If the attempt already streamed bytes into the caller's writer, never
		// retry: partial data was delivered and re-streaming would corrupt it.
		// Buffered requests (bodyWriter == nil) keep their per-attempt buffer,
		// so retrying them stays safe.
		if bodyWriter != nil && attemptCommittedResp(resp) {
			resp.Attempts = cloneAttempts(attempts)
			return
		}
		if errors.Is(resp.Err, errFreshTransportRequired) {
			continue
		}
		if !temporaryRetried && resp.Err == nil && resp.StatusCode == 451 && isArticleOperation(payload) {
			temporaryRetried = true
			delay := temporaryRetryMinDelay + time.Duration(rand.Int64N(int64(temporaryRetryJitter)+1))
			select {
			case <-time.After(delay):
				// The response connection was retired by readerLoop. Reject every
				// other transport that predates this retry as well, so a multi-
				// connection provider cannot satisfy the retry from another hot
				// socket.
				freshTransport = true
				continue
			case <-ctx.Done():
				resp.Attempts = cloneAttempts(attempts)
				return resp, true, true
			}
		}
		if connRetries < maxConnDiedRetries && isConnectionDeathError(resp.Err) {
			connRetries++
			continue // dead connection drained; retry fresh on same provider
		}
		resp.Attempts = cloneAttempts(attempts)
		return
	}
}

func (c *Client) doSendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response, priority bool, validateBody bool) {
	defer close(respCh)

	// Precompute for STAT probe: extract message-ID once.
	msgID := extractProbeMsgID(payload)
	raceable := c.statProbe && msgID != nil
	var statPayload []byte
	if raceable {
		statPayload = append(append([]byte("STAT "), msgID...), "\r\n"...)
	}

	var lastResp Response
	hasResp := false
	var lastErr error
	var attempts []AttemptEvidence
	post430 := false

	// 1. Try all main providers.
	registry := c.registry.Load()
	mains := registry.mains
	n := len(mains)
	if n == 0 {
		respCh <- Response{Err: errors.New("nntp: no main providers")}
		return
	}

	// Pick start index based on dispatch strategy.
	var start int
	switch c.dispatch {
	case DispatchFIFO:
		// Priority order: first provider with available capacity and within quota,
		// falling back to provider 0 if all are saturated or exceeded.
		for i, g := range mains {
			if g.gate.available.Load() > 0 && !g.isQuotaExceeded() {
				start = i
				break
			}
		}
	default: // DispatchRoundRobin
		// Dynamic weighted round-robin. Quota-exceeded providers get weight 0
		// so they are never selected during normal dispatch.
		cumWeights, totalW := dispatchWeights(mains, c.speedAware)
		if totalW == 0 {
			// All providers are quota-exceeded; start at 0 and let the main
			// loop below return ErrQuotaExceeded for each.
			start = 0
		} else {
			slot := int(c.nextIdx.Add(1) % uint64(totalW))
			start = sort.SearchInts(cumWeights, slot+1)
		}
	}

	for attempt := range n {
		idx := (start + attempt) % n
		g := mains[idx]
		if g.isQuotaExceeded() {
			if err := c.requestCancellation(ctx); err != nil {
				attempts = append(attempts, buildEligibilityEvidence(payload, g.id, err, validateBody))
				respCh <- cancellationResponse(attempts, err)
				return
			}
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrQuotaExceeded)
			attempts = append(attempts, buildEligibilityEvidence(payload, g.id, lastErr, validateBody))
			continue
		}
		resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, bodyWriter, onMeta, priority || post430, validateBody, false)
		attempts = append(attempts, resp.Attempts...)
		if cancelled {
			err := ctx.Err()
			if err == nil {
				err = c.ctx.Err()
			}
			respCh <- cancellationResponse(attempts, err)
			return
		}
		if !ok {
			// Connection died — try next provider.
			continue
		}
		if resp.Err != nil {
			// A committed attempt with a caller writer already streamed partial
			// bytes; deliver the error rather than re-streaming into the same
			// writer on another provider.
			if bodyWriter != nil && attemptCommittedResp(resp) {
				resp.Attempts = cloneAttempts(attempts)
				respCh <- resp
				return
			}
			lastErr = resp.Err
			continue
		}
		if resp.StatusCode == 502 {
			// Provider returned "service unavailable" — remove it from the
			// pool immediately so no further requests are routed to it.
			c.retireUnavailableProvider(g)
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrServiceUnavailable)
			continue
		}
		if resp.StatusCode == 430 || resp.StatusCode == 423 {
			c.nextIdx.Add(1) // bias next request away from this provider
			lastResp = resp
			hasResp = true
			post430 = true

			if raceable {
				// Build remaining mains and race them in parallel via STAT.
				rest := make([]*providerGroup, 0, n-attempt-1)
				for a := attempt + 1; a < n; a++ {
					rest = append(rest, mains[(start+a)%n])
				}
				delivered, cancelled, raceErr := c.raceCandidates(
					ctx, rest, statPayload, payload, bodyWriter, onMeta,
					validateBody, &attempts, respCh,
				)
				if cancelled {
					err := ctx.Err()
					if err == nil {
						err = c.ctx.Err()
					}
					respCh <- cancellationResponse(attempts, err)
					return
				}
				if delivered {
					return
				}
				if raceErr != nil {
					lastErr = raceErr
				}
				break // all remaining mains were probed in the race
			}
			continue
		}
		if resp.StatusCode == 451 && !isArticleOperation(payload) {
			resp.Attempts = cloneAttempts(attempts)
			respCh <- resp
			return
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), toError(resp.StatusCode, resp.Status))
			continue
		}
		// Success.
		resp.Attempts = cloneAttempts(attempts)
		respCh <- resp
		return
	}

	// 2. All main providers returned 430 (or died) — try backup providers.
	backups := registry.backups
	if raceable && post430 {
		delivered, cancelled, raceErr := c.raceCandidates(
			ctx, backups, statPayload, payload, bodyWriter, onMeta,
			validateBody, &attempts, respCh,
		)
		if cancelled {
			err := ctx.Err()
			if err == nil {
				err = c.ctx.Err()
			}
			respCh <- cancellationResponse(attempts, err)
			return
		}
		if delivered {
			return
		}
		if raceErr != nil {
			lastErr = raceErr
		}
	} else {
		for i := range backups {
			if err := c.requestCancellation(ctx); err != nil {
				respCh <- cancellationResponse(attempts, err)
				return
			}
			g := backups[i]
			if g.isQuotaExceeded() {
				if err := c.requestCancellation(ctx); err != nil {
					attempts = append(attempts, buildEligibilityEvidence(payload, g.id, err, validateBody))
					respCh <- cancellationResponse(attempts, err)
					return
				}
				lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrQuotaExceeded)
				attempts = append(attempts, buildEligibilityEvidence(payload, g.id, lastErr, validateBody))
				continue
			}
			resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, bodyWriter, onMeta, priority || post430, validateBody, false)
			attempts = append(attempts, resp.Attempts...)
			if cancelled {
				err := ctx.Err()
				if err == nil {
					err = c.ctx.Err()
				}
				respCh <- cancellationResponse(attempts, err)
				return
			}
			if !ok {
				continue
			}
			if resp.Err != nil {
				// A committed attempt with a caller writer already streamed
				// partial bytes; deliver the error rather than re-streaming into
				// the same writer on another provider.
				if bodyWriter != nil && attemptCommittedResp(resp) {
					resp.Attempts = cloneAttempts(attempts)
					respCh <- resp
					return
				}
				lastErr = resp.Err
				continue
			}
			if resp.StatusCode == 502 {
				c.retireUnavailableProvider(g)
				lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), ErrServiceUnavailable)
				continue
			}
			resp.Attempts = cloneAttempts(attempts)
			if resp.StatusCode == 451 && !isArticleOperation(payload) {
				respCh <- resp
				return
			}
			if resp.StatusCode == 430 || resp.StatusCode == 423 {
				lastResp = resp
				hasResp = true
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 400 {
				lastErr = fmt.Errorf("%s: %w", safeIdentityText(g.name), toError(resp.StatusCode, resp.Status))
				continue
			}
			respCh <- resp
			return
		}
	}

	// 3. All providers exhausted — deliver the last 430, the last error, or a fallback.
	if err := c.requestCancellation(ctx); err != nil {
		respCh <- cancellationResponse(attempts, err)
		return
	}
	if lastErr != nil {
		respCh <- Response{
			Err:      newTransportError(attempts, lastErr),
			Attempts: cloneAttempts(attempts),
		}
	} else if hasResp {
		lastResp.Attempts = cloneAttempts(attempts)
		respCh <- lastResp
	} else {
		respCh <- Response{Err: errors.New("nntp: all providers exhausted")}
	}
}

// NumProviders returns the number of configured providers (main + backup).
func (c *Client) NumProviders() int {
	registry := c.registry.Load()
	return len(registry.mains) + len(registry.backups)
}

// Stats returns a snapshot of per-provider and aggregate metrics.
func (c *Client) Stats() ClientStats {
	elapsed := time.Since(c.startTime)
	secs := elapsed.Seconds()
	var cs ClientStats
	cs.Elapsed = elapsed
	var totalBytes int64
	registry := c.registry.Load()
	for _, groups := range [...][]*providerGroup{registry.mains, registry.backups} {
		for _, g := range groups {
			consumed := g.stats.BytesConsumed.Load()
			totalBytes += consumed
			maxSlots, running := g.gate.snapshot()
			quotaUsed := g.stats.quotaUsed.Load()
			ps := ProviderStats{
				Name:                g.name,
				ProviderID:          g.id,
				SpeedEWMA:           speedEWMABytesPerSec(&g.stats),
				BytesConsumed:       consumed,
				Missing:             g.stats.Missing.Load(),
				Errors:              g.stats.Errors.Load(),
				ActiveConnections:   running,
				MaxConnections:      maxSlots,
				AvailableSlots:      int(g.gate.available.Load()),
				TTFB:                time.Duration(g.stats.ttfbEWMA.Load()),
				PipelineInUse:       int(g.stats.PipelineInUse.Load()),
				PipelineLimit:       g.stats.pipelineLimit,
				BackgroundStatInUse: int(g.stats.BackgroundStatInUse.Load()),
				BackgroundStatLimit: g.stats.backgroundStatLimit,
				PriorityHeadroom:    g.stats.priorityHeadroom,
				CircuitBreaker:      g.breaker.snapshot(),
				Ping:                g.stats.Ping,
				QuotaBytes:          g.stats.quotaBytes,
				QuotaUsed:           quotaUsed,
				QuotaExceeded:       g.stats.quotaBytes > 0 && quotaUsed >= g.stats.quotaBytes,
			}
			if g.stats.quotaBytes > 0 && g.quotaPeriod > 0 {
				resetAt := g.quotaResetAt.Load()
				if resetAt > 0 {
					ps.QuotaResetAt = time.Unix(0, resetAt)
				}
			}
			if secs > 0 {
				ps.AvgSpeed = float64(consumed) / secs
			}
			cs.Providers = append(cs.Providers, ps)
		}
	}
	cs.BytesConsumed = totalBytes
	if secs > 0 {
		cs.AvgSpeed = float64(totalBytes) / secs
	}
	return cs
}

// AddProvider validates, pings, and registers a new provider at runtime.
// Ping failures are recorded in the group's stats but do not cause an error return.
func (c *Client) AddProvider(p Provider) error {
	return c.addProvider(p)
}

// RemoveProvider stops and removes a provider by stable ID or operational name.
// Goroutines wind down asynchronously; Client.Close still waits for all via c.wg.
func (c *Client) RemoveProvider(name string) error {
	group, exists := c.removeProvider(name)
	if !exists {
		return fmt.Errorf("nntp: provider %q not found", name)
	}
	if group != nil {
		group.cancel()
		group.gate.stop()
	}
	return nil
}

// ResetProviderQuota resets the download quota for the named provider without
// removing and re-adding it. The consumed-bytes counter and exceeded flag are
// cleared atomically, and a fresh reset deadline is scheduled when the provider
// has a non-zero quota period.
//
// Returns an error if no provider with that name is registered.
func (c *Client) ResetProviderQuota(name string) error {
	registration, exists := c.registrationForToken(name)
	if !exists || registration.group == nil {
		return fmt.Errorf("nntp: provider %q not found", name)
	}
	group := registration.group
	group.stats.quotaUsed.Store(0)
	group.stats.quotaExceeded.Store(false)
	if group.quotaPeriod > 0 {
		group.quotaResetAt.Store(time.Now().Add(group.quotaPeriod).UnixNano())
	} else {
		group.quotaResetAt.Store(0)
	}
	return nil
}
