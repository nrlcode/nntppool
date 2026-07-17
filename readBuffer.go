package nntppool

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	defaultReadBufSize = 128 * 1024
	maxReadBufSize     = 8 * 1024 * 1024
)

type readDeadlineOwner uint8

const (
	readDeadlineNone readDeadlineOwner = iota
	readDeadlineCaller
	readDeadlineProviderResponse
	readDeadlineProviderStall
	readDeadlineAbandonedDrain
)

type readBuffer struct {
	buf        []byte
	start, end int

	// deadlineMu serializes every socket read-deadline mutation. A forced drain
	// remains authoritative until its checked clear succeeds.
	deadlineMu     sync.Mutex
	forcedDrain    bool
	forcedDeadline time.Time

	// Cached deadline to avoid redundant SetReadDeadline syscalls. The cache is
	// updated only after the syscall succeeds.
	lastDeadline    time.Time
	lastHasDeadline bool
	deadlineSet     bool // true after first SetReadDeadline call

	// A Read may return final bytes together with a terminal transport error.
	// Keep that fact until the current response either proves complete or needs
	// more input; buffered bytes must never make the dead socket look reusable.
	deferredTerminalErr   error
	deferredTerminalOwner readDeadlineOwner
}

func (rb *readBuffer) init() {
	if len(rb.buf) == 0 {
		rb.buf = make([]byte, defaultReadBufSize)
	}
}

func (rb *readBuffer) window() []byte {
	return rb.buf[rb.start:rb.end]
}

func (rb *readBuffer) advance(consumed int) {
	if consumed <= 0 {
		return
	}
	rb.start += consumed
	if rb.start >= rb.end {
		rb.start, rb.end = 0, 0
	}
}

func (rb *readBuffer) compact() {
	if rb.start == 0 || rb.start == rb.end {
		return
	}
	copy(rb.buf, rb.buf[rb.start:rb.end])
	rb.end -= rb.start
	rb.start = 0
}

func (rb *readBuffer) ensureWriteSpace() error {
	if rb.end < len(rb.buf) {
		return nil
	}
	if rb.start > 0 {
		rb.compact()
		if rb.end < len(rb.buf) {
			return nil
		}
	}

	// No space and cannot compact: grow.
	cur := len(rb.buf)
	if cur == 0 {
		cur = defaultReadBufSize
	}
	newLen := cur * 2
	if newLen > maxReadBufSize {
		newLen = maxReadBufSize
	}
	if newLen <= len(rb.buf) {
		return fmt.Errorf("nntp read buffer exceeded %d bytes", maxReadBufSize)
	}

	nb := make([]byte, newLen)
	copy(nb, rb.window())
	rb.end = rb.end - rb.start
	rb.start = 0
	rb.buf = nb
	return nil
}

func (rb *readBuffer) setDeadlineLocked(conn net.Conn, deadline time.Time, hasDeadline bool) error {
	if rb.deadlineSet && rb.lastHasDeadline == hasDeadline &&
		(!hasDeadline || deadline.Equal(rb.lastDeadline)) {
		return nil
	}
	if !hasDeadline {
		deadline = time.Time{}
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	rb.lastDeadline = deadline
	rb.lastHasDeadline = hasDeadline
	rb.deadlineSet = true
	return nil
}

func (rb *readBuffer) forceDrainDeadline(conn net.Conn, deadline time.Time) error {
	rb.deadlineMu.Lock()
	defer rb.deadlineMu.Unlock()
	rb.forcedDrain = true
	rb.forcedDeadline = deadline
	return rb.setDeadlineLocked(conn, deadline, true)
}

func (rb *readBuffer) clearDrainDeadline(conn net.Conn) error {
	rb.deadlineMu.Lock()
	defer rb.deadlineMu.Unlock()
	if !rb.forcedDrain {
		return nil
	}
	if err := rb.setDeadlineLocked(conn, time.Time{}, false); err != nil {
		return err
	}
	rb.forcedDrain = false
	rb.forcedDeadline = time.Time{}
	return nil
}

func (rb *readBuffer) readMore(conn net.Conn, deadline time.Time, hasDeadline bool) (int, error) {
	n, _, err := rb.readMoreOwned(conn, deadline, hasDeadline, readDeadlineNone)
	return n, err
}

func (rb *readBuffer) readMoreOwned(conn net.Conn, deadline time.Time, hasDeadline bool, owner readDeadlineOwner) (int, readDeadlineOwner, error) {
	if err := rb.ensureWriteSpace(); err != nil {
		return 0, owner, err
	}
	rb.deadlineMu.Lock()
	if rb.forcedDrain {
		deadline, hasDeadline, owner = rb.forcedDeadline, true, readDeadlineAbandonedDrain
	}
	err := rb.setDeadlineLocked(conn, deadline, hasDeadline)
	rb.deadlineMu.Unlock()
	if err != nil {
		return 0, owner, err
	}
	n, err := conn.Read(rb.buf[rb.end:])
	if n > 0 {
		rb.end += n
	}
	return n, owner, err
}

func (rb *readBuffer) deferTerminal(owner readDeadlineOwner, err error) {
	if err != nil && rb.deferredTerminalErr == nil {
		rb.deferredTerminalOwner = owner
		rb.deferredTerminalErr = err
	}
}

func (rb *readBuffer) takeDeferredTerminal() (readDeadlineOwner, error) {
	owner, err := rb.deferredTerminalOwner, rb.deferredTerminalErr
	rb.deferredTerminalOwner = readDeadlineNone
	rb.deferredTerminalErr = nil
	return owner, err
}

// feedUntilDone streams a response off the wire into out, decoding via feeder.
//
// deadline is consulted before every read; it receives wireBytes, the
// cumulative number of bytes already read from conn during this call. This
// lets callers distinguish "still waiting for the first response byte" (a
// time-to-first-byte bound) from "bytes are flowing" (a rolling progress/stall
// bound), and to detect progress even when the decoder consumes 0 bytes from a
// partial line.
func (rb *readBuffer) feedUntilDone(conn net.Conn, feeder streamFeeder, out io.Writer, deadline func(wireBytes int) (time.Time, bool)) error {
	return rb.feedUntilDoneControlled(
		conn,
		feeder,
		out,
		func(wireBytes, _ int) (time.Time, bool, int, error) {
			dl, ok := deadline(wireBytes)
			return dl, ok, 0, nil
		},
	)
}

// feedUntilDoneControlled is feedUntilDone with an additional response-byte
// budget. control receives both bytes read from conn during this response and
// total bytes consumed by the feeder, including bytes that were already in the
// shared read buffer. maxFeed limits the next feeder call; zero means
// unlimited. This lets abandoned BODY responses drain a bounded wire prefix
// without accidentally ignoring a large prebuffered tail.
func (rb *readBuffer) feedUntilDoneControlled(
	conn net.Conn,
	feeder streamFeeder,
	out io.Writer,
	control func(wireBytes, consumedBytes int) (deadline time.Time, hasDeadline bool, maxFeed int, err error),
) error {
	_, err, _, terminalErr := rb.feedUntilDoneControlledOwned(
		conn,
		feeder,
		out,
		func(wireBytes, consumedBytes int) (time.Time, bool, readDeadlineOwner, int, error) {
			deadline, hasDeadline, maxFeed, controlErr := control(wireBytes, consumedBytes)
			return deadline, hasDeadline, readDeadlineNone, maxFeed, controlErr
		},
	)
	if err == nil && terminalErr != nil && rb.start != rb.end {
		return terminalErr
	}
	return err
}

func (rb *readBuffer) feedUntilDoneControlledOwned(
	conn net.Conn,
	feeder streamFeeder,
	out io.Writer,
	control func(wireBytes, consumedBytes int) (deadline time.Time, hasDeadline bool, owner readDeadlineOwner, maxFeed int, err error),
) (readDeadlineOwner, error, readDeadlineOwner, error) {
	rb.init()
	wireBytes := 0
	consumedBytes := 0

	for {
		// Ensure we have some bytes to feed.
		if rb.start == rb.end {
			if rb.deferredTerminalErr != nil {
				owner, err := rb.takeDeferredTerminal()
				return owner, err, readDeadlineNone, nil
			}
			rb.start, rb.end = 0, 0
			dl, ok, owner, _, controlErr := control(wireBytes, consumedBytes)
			if controlErr != nil {
				return owner, controlErr, readDeadlineNone, nil
			}
			n, selectedOwner, err := rb.readMoreOwned(conn, dl, ok, owner)
			wireBytes += n
			if err != nil {
				if n == 0 {
					return selectedOwner, err, readDeadlineNone, nil
				}
				rb.deferTerminal(selectedOwner, err)
			}
		}

		_, _, owner, maxFeed, controlErr := control(wireBytes, consumedBytes)
		if controlErr != nil {
			return owner, controlErr, readDeadlineNone, nil
		}
		window := rb.window()
		if maxFeed > 0 && len(window) > maxFeed {
			window = window[:maxFeed]
		}
		consumed, done, err := feeder.Feed(window, out)
		if consumed > 0 {
			rb.advance(consumed)
			consumedBytes += consumed
		}
		if err != nil {
			return readDeadlineNone, err, readDeadlineNone, nil
		}
		// Re-check control even when this buffered feed completed the response.
		// Time and byte bounds are lifecycle limits, not merely socket-read
		// deadlines, so locally buffered completion cannot bypass them.
		if _, _, owner, _, controlErr = control(wireBytes, consumedBytes); controlErr != nil {
			return owner, controlErr, readDeadlineNone, nil
		}
		if done {
			terminalOwner, terminalErr := rb.takeDeferredTerminal()
			return readDeadlineNone, nil, terminalOwner, terminalErr
		}
		if rb.deferredTerminalErr != nil {
			if consumed == 0 || rb.start == rb.end {
				owner, err := rb.takeDeferredTerminal()
				return owner, err, readDeadlineNone, nil
			}
			continue
		}
		if consumed > 0 && rb.start != rb.end {
			continue
		}

		// Need more data.
		// If decoder couldn't consume anything but we have buffered bytes,
		// compact them to the start so the next read appends contiguously.
		if consumed == 0 && (rb.end-rb.start) > 0 {
			rb.compact()
		}

		dl, ok, owner, _, controlErr := control(wireBytes, consumedBytes)
		if controlErr != nil {
			return owner, controlErr, readDeadlineNone, nil
		}
		n, selectedOwner, err := rb.readMoreOwned(conn, dl, ok, owner)
		wireBytes += n
		if err != nil {
			if n == 0 {
				return selectedOwner, err, readDeadlineNone, nil
			}
			rb.deferTerminal(selectedOwner, err)
		}
	}
}
