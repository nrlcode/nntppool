package nntppool

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/mnightingale/rapidyenc"
)

// YEncMeta groups provisional yEnc metadata from =ybegin and =ypart. It is
// available before decoding completes and is not final integrity evidence.
type YEncMeta struct {
	FileName  string
	FileSize  int64
	Part      int64
	PartBegin int64
	PartSize  int64
	Total     int64
}

type NNTPResponse struct {
	BytesDecoded  int
	BytesConsumed int
	Lines         []string
	Format        rapidyenc.Format
	YEnc          YEncMeta
	EndSize       int64
	ExpectedCRC   uint32
	Message       string
	State         rapidyenc.State
	StatusCode    int
	CRC           uint32

	eof            bool
	body           bool
	hasPart        bool
	hasBegin       bool
	hasEnd         bool
	hasCrc         bool
	hasPartCRC     bool
	hasFileCRC     bool
	hasEndPart     bool
	hasEmptyline   bool // for article requests has the empty line separating headers and body been seen
	partCRC        uint32
	fileCRC        uint32
	endPart        int64
	uuHasBegin     bool
	uuSeenData     bool
	uuShortSeen    bool
	uuNeedEnd      bool
	uuComplete     bool
	sawYEncControl bool
	onMeta         func(YEncMeta)
	headerErr      error
	// Raw Send retains v4's decoder-error behavior. Strict high-level BODY
	// requests set this flag and propagate native decoder failures as corruption.
	strictDecodeErrors bool
	decodeFn           func(dst, src []byte, state *rapidyenc.State) (nDst, nSrc int, end rapidyenc.End, err error)
}

const nntpBody = 222
const nntpArtiicle = 220
const nntpHead = 221
const nntpCapabilities = 101

// Feed consumes raw NNTP protocol bytes from buf, writing any decoded payload bytes to out.
// It returns (bytesConsumedFromBuf, done, error).
func (r *NNTPResponse) Feed(buf []byte, out io.Writer) (consumed int, done bool, err error) {
	if out == nil {
		out = io.Discard
	}

	n, err := r.decode(buf, out)
	r.BytesConsumed += n
	if err != nil {
		return n, false, err
	}
	if r.eof {
		return n, true, nil
	}
	return n, false, nil
}

func (r *NNTPResponse) decode(buf []byte, out io.Writer) (read int, err error) {
	if r.body && r.Format == rapidyenc.FormatYenc {
		n, err := r.decodeYenc(buf, out)
		if err != nil {
			return int(n), err
		}
		read += int(n)
		buf = buf[n:]
		if r.body {
			return int(n), err
		}
	}

	// Line by line processing
	if !r.body {
		var line []byte
		var found bool
		for {
			if line, buf, found = bytes.Cut(buf, []byte("\r\n")); !found {
				break
			}
			read += len(line) + 2

			if bytes.Equal(line, []byte(".")) {
				r.eof = true
				break
			}

			if r.Format == rapidyenc.FormatUnknown {
				if r.StatusCode == 0 && len(line) >= 3 {
					r.Message = string(line)
					r.StatusCode, err = strconv.Atoi(string(line[:3]))
					if err != nil {
						return read, ErrProtocolDesync
					}
					if !isMultiline(r.StatusCode) {
						r.eof = true
						break
					}
					continue
				}
				r.detectFormat(line)
			}

			switch r.Format {
			case rapidyenc.FormatUnknown:
				if !r.strictDecodeErrors {
					r.Lines = append(r.Lines, string(line))
				}
			case rapidyenc.FormatYenc:
				r.processYencHeader(line)
				if r.body {
					n, err := r.decodeYenc(buf, out)
					read += int(n)
					buf = buf[n:]
					if err != nil {
						return read, err
					}
					if r.body {
						// Still decoding, need more data
						return read, nil
					}
					// =ypart was encountered, switch to body decoding
				}
			case rapidyenc.FormatUU:
				if r.strictDecodeErrors {
					if err := r.processUULine(line, out); err != nil {
						return read, err
					}
				}
			}
		}
	}

	return read, nil
}

func (r *NNTPResponse) detectFormat(line []byte) {
	if r.StatusCode != nntpBody && r.StatusCode != nntpArtiicle {
		return
	}

	if len(line) == 0 {
		r.hasEmptyline = true
		return
	}

	// YEnc detection
	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		r.Format = rapidyenc.FormatYenc
		return
	}
	if r.strictDecodeErrors && (bytes.HasPrefix(line, []byte("=ypart ")) || bytes.HasPrefix(line, []byte("=yend "))) {
		r.sawYEncControl = true
		return
	}

	uuLine := unstuffUULine(line)
	if r.strictDecodeErrors {
		if bytes.HasPrefix(uuLine, []byte("begin ")) || isStrictUUDataLine(uuLine) {
			r.Format = rapidyenc.FormatUU
		}
		return
	}

	// UUEncode detection: 60 or 61 chars, starts with 'M'
	if (len(line) == 60 || len(line) == 61) && line[0] == 'M' {
		r.Format = rapidyenc.FormatUU
		return
	}

	// UUEncode alternative header form: "begin "
	if bytes.HasPrefix(line, []byte("begin ")) {
		// Skip leading spaces
		line = bytes.TrimLeft(line[6:], " ")

		// Extract the next token (permission part)
		perms, found := bytes.CutPrefix(line, []byte(" "))
		if !found {
			return
		}

		// Check all characters are between '0' and '7'
		valid := true
		for _, c := range perms {
			if c < '0' || c > '7' {
				valid = false
				break
			}
		}

		if valid {
			r.Format = rapidyenc.FormatUU
		}
		return
	}

	// Remove dot stuffing
	if bytes.HasPrefix(line, []byte("..")) {
		line = line[1:]
	}

	// Multipart UU with a short final part
	if len(line) <= 1 {
		return
	}

	// For Article responses only consider after the headers
	if r.StatusCode != nntpBody && (r.StatusCode != nntpArtiicle || !r.hasEmptyline) {
		return
	}

	first := line[0]
	n := len(line)

	for _, length := range []int{
		decodeUUCharWorkaround(first),
		decodeUUChar(first),
	} {
		if n < length || length <= 0 {
			continue
		}

		body := line[1:length]
		padding := line[length:]

		if !allInASCIIRange(body, 32, 96) || !onlySpaceOrBacktick(padding) {
			continue
		}

		// Probably UU
		r.Format = rapidyenc.FormatUU
		return
	}
}

func unstuffUULine(line []byte) []byte {
	if bytes.HasPrefix(line, []byte("..")) {
		return line[1:]
	}
	return line
}

func validUUBegin(line []byte) bool {
	fields := bytes.Fields(line)
	if len(fields) < 3 || !bytes.Equal(fields[0], []byte("begin")) || (len(fields[1]) != 3 && len(fields[1]) != 4) {
		return false
	}
	for _, char := range fields[1] {
		if char < '0' || char > '7' {
			return false
		}
	}
	return len(fields[2]) > 0
}

func uuEncodedLengths(decoded int) (minimum, canonical int) {
	return (4*decoded + 2) / 3, 4 * ((decoded + 2) / 3)
}

func isUUAlphabet(data []byte) bool {
	for _, char := range data {
		if (char < ' ' || char > '_') && char != '`' {
			return false
		}
	}
	return true
}

func strictUUDataShape(line []byte) (decoded, encodedLength int, ok bool) {
	if len(line) < 2 {
		return 0, 0, false
	}
	if line[0] < '!' || line[0] > 'M' {
		return 0, 0, false
	}
	decoded = decodeUUChar(line[0])
	minimum, canonical := uuEncodedLengths(decoded)
	encodedLength = len(line) - 1
	if encodedLength < minimum || encodedLength > canonical || !isUUAlphabet(line[1:]) {
		return 0, 0, false
	}
	return decoded, encodedLength, true
}

func isStrictUUDataLine(line []byte) bool {
	_, _, ok := strictUUDataShape(line)
	return ok
}

func (r *NNTPResponse) processUULine(wireLine []byte, out io.Writer) error {
	line := unstuffUULine(wireLine)
	if bytes.HasPrefix(line, []byte("begin ")) {
		if r.uuHasBegin || r.uuSeenData || r.uuNeedEnd || r.uuComplete || !validUUBegin(line) {
			return fmt.Errorf("%w: invalid UU begin line", ErrBodyCorrupt)
		}
		r.uuHasBegin = true
		return nil
	}
	if bytes.Equal(line, []byte("end")) {
		if !r.uuHasBegin || !r.uuNeedEnd || r.uuComplete {
			return fmt.Errorf("%w: invalid UU end line", ErrBodyCorrupt)
		}
		r.uuNeedEnd = false
		r.uuComplete = true
		return nil
	}
	if len(line) == 1 && (line[0] == ' ' || line[0] == '`') {
		if !r.uuHasBegin || r.uuNeedEnd || r.uuComplete {
			return fmt.Errorf("%w: invalid UU zero line", ErrBodyCorrupt)
		}
		r.uuNeedEnd = true
		return nil
	}
	if r.uuNeedEnd || r.uuComplete || r.uuShortSeen {
		return fmt.Errorf("%w: invalid UU line order", ErrBodyCorrupt)
	}

	decodedLength, encodedLength, ok := strictUUDataShape(line)
	if !ok {
		return fmt.Errorf("%w: malformed UU data line", ErrBodyCorrupt)
	}
	var encoded [60]byte
	copy(encoded[:], line[1:1+encodedLength])
	var decoded [45]byte
	decodedOffset := 0
	for encodedOffset := 0; encodedOffset < encodedLength; encodedOffset += 4 {
		var values [4]byte
		for i := range values {
			if encodedOffset+i < encodedLength {
				values[i] = byte(decodeUUChar(encoded[encodedOffset+i]))
			}
		}
		triplet := [3]byte{
			values[0]<<2 | values[1]>>4,
			values[1]<<4 | values[2]>>2,
			values[2]<<6 | values[3],
		}
		decodedOffset += copy(decoded[decodedOffset:], triplet[:min(3, decodedLength-decodedOffset)])
	}
	if decodedOffset != decodedLength {
		return fmt.Errorf("%w: truncated UU data line", ErrBodyCorrupt)
	}
	written, err := out.Write(decoded[:decodedLength])
	if err != nil {
		return err
	}
	if written != decodedLength {
		return io.ErrShortWrite
	}
	r.BytesDecoded += decodedLength
	r.uuSeenData = true
	if decodedLength < 45 {
		r.uuShortSeen = true
	}
	return nil
}

func allInASCIIRange(b []byte, lo, hi byte) bool {
	for _, c := range b {
		if c < lo || c > hi {
			return false
		}
	}
	return true
}

func onlySpaceOrBacktick(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '`' {
			return false
		}
	}
	return true
}

func decodeUUCharWorkaround(c byte) int {
	return int(((int(c)-32)&63)*4+5) / 3
}

func decodeUUChar(c byte) int {
	if c == '`' {
		return 0
	}
	return int((c - ' ') & 0x3F)
}

func isMultiline(code int) bool {
	return code == nntpBody || code == nntpArtiicle || code == nntpHead || code == nntpCapabilities
}

func (r *NNTPResponse) decodeYenc(buf []byte, out io.Writer) (n int64, err error) {
	if len(buf) == 0 {
		return 0, nil
	}

	var produced, consumed int
	var end rapidyenc.End

	decodeFn := r.decodeFn
	if decodeFn == nil {
		decodeFn = rapidyenc.DecodeIncremental
	}
	produced, consumed, end, err = decodeFn(buf, buf, &r.State)
	if err != nil && r.strictDecodeErrors {
		return 0, fmt.Errorf("%w: yEnc decode: %w", ErrBodyCorrupt, err)
	}

	if produced > 0 {
		r.CRC = crc32.Update(r.CRC, crc32.IEEETable, buf[:produced])
		r.BytesDecoded += produced
		if _, werr := out.Write(buf[:produced]); werr != nil {
			return n, werr
		}
	}
	n += int64(consumed)

	switch end {
	case rapidyenc.EndNone:
		if r.State == rapidyenc.StateCRLFEQ {
			// Special case: found "\r\n=" but no more data - might be start of =yend
			r.State = rapidyenc.StateCRLF
			n -= 1 // Back up to allow =yend detection
		}
	case rapidyenc.EndControl:
		// Found "\r\n=y" - likely =yend line, exit body mode
		r.body = false
		n -= 2 // Back up to include "=y" for header processing
	case rapidyenc.EndArticle:
		// Found ".\r\n" - NNTP article terminator, exit body mode
		r.body = false
		n -= 3 // Back up to include ".\r\n" for terminator detection
	}

	return n, nil
}

func (r *NNTPResponse) processYencHeader(line []byte) {
	var err error
	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		r.hasBegin = true
		line = line[len("=ybegin"):]
		if r.YEnc.FileSize, err = extractInt(line, []byte(" size=")); err != nil {
			r.headerErr = fmt.Errorf("invalid =ybegin size: %w", err)
		}
		if r.YEnc.FileName, err = extractString(line, []byte(" name=")); err != nil {
			r.headerErr = fmt.Errorf("invalid =ybegin name: %w", err)
		}
		if r.YEnc.Part, err = extractInt(line, []byte(" part=")); err != nil {
			if bytes.Contains(line, []byte(" part=")) {
				r.headerErr = fmt.Errorf("invalid =ybegin part: %w", err)
			}
			// Not multi-part, so body starts immediately after =ybegin
			r.body = true
			r.YEnc.PartSize = r.YEnc.FileSize
			if r.onMeta != nil {
				r.onMeta(r.YEnc)
				r.onMeta = nil
			}
		}
		if r.YEnc.Total, err = extractInt(line, []byte(" total=")); err != nil && bytes.Contains(line, []byte(" total=")) {
			r.headerErr = fmt.Errorf("invalid =ybegin total: %w", err)
		}
	} else if bytes.HasPrefix(line, []byte("=ypart ")) {
		// =ypart signals start of body data in multi-part files
		r.hasPart = true
		r.body = true
		line = line[len("=ypart"):]
		var begin int64
		// Convert from 1-based to 0-based indexing
		if begin, err = extractInt(line, []byte(" begin=")); err != nil || begin <= 0 {
			r.headerErr = fmt.Errorf("invalid =ypart begin")
		} else {
			r.YEnc.PartBegin = begin - 1
		}
		if end, endErr := extractInt(line, []byte(" end=")); endErr == nil && end >= begin && begin > 0 {
			r.YEnc.PartSize = end - r.YEnc.PartBegin
		} else {
			r.headerErr = fmt.Errorf("invalid =ypart end")
		}
		if r.onMeta != nil {
			r.onMeta(r.YEnc)
			r.onMeta = nil
		}
	} else if bytes.HasPrefix(line, []byte("=yend ")) {
		r.hasEnd = true
		line = line[len("=yend"):]
		if bytes.Contains(line, []byte(" part=")) {
			if r.endPart, err = extractInt(line, []byte(" part=")); err != nil || r.endPart <= 0 {
				r.headerErr = fmt.Errorf("invalid =yend part")
			} else {
				r.hasEndPart = true
			}
		}
		if bytes.Contains(line, []byte(" pcrc32=")) {
			crc, crcErr := extractCRC(line, []byte(" pcrc32="))
			if crcErr != nil {
				r.headerErr = fmt.Errorf("invalid =yend pcrc32: %w", crcErr)
			} else {
				r.partCRC = crc
				r.hasPartCRC = true
			}
		}
		if bytes.Contains(line, []byte(" crc32=")) {
			crc, crcErr := extractCRC(line, []byte(" crc32="))
			if crcErr != nil {
				r.headerErr = fmt.Errorf("invalid =yend crc32: %w", crcErr)
			} else {
				r.fileCRC = crc
				r.hasFileCRC = true
			}
		}
		// Preserve the existing single expected-CRC view for callers. Final
		// applicability is resolved by validateBody once part coverage is known.
		switch {
		case r.hasPartCRC:
			r.ExpectedCRC = r.partCRC
			r.hasCrc = true
		case r.hasFileCRC:
			r.ExpectedCRC = r.fileCRC
			r.hasCrc = true
		}
		if r.EndSize, err = extractInt(line, []byte(" size=")); err != nil {
			r.headerErr = fmt.Errorf("invalid =yend size: %w", err)
		}
	}
}

// validateBody verifies the framing and integrity facts that are knowable at
// the transport boundary. It is called before a buffered BODY attempt may be
// accepted or selected as a provider fallback winner.
func (r *NNTPResponse) validateBody() error {
	if r.StatusCode != nntpBody {
		if r.StatusCode >= 200 && r.StatusCode < 400 {
			return fmt.Errorf("%w: BODY returned status %d", ErrBodyCorrupt, r.StatusCode)
		}
		return nil
	}
	switch r.Format {
	case rapidyenc.FormatUnknown:
		if r.sawYEncControl {
			return fmt.Errorf("%w: yEnc control line without =ybegin", ErrBodyCorrupt)
		}
		return fmt.Errorf("%w: content is not a recognized encoding", errBodyEncodingUnknown)
	case rapidyenc.FormatUU:
		if r.uuHasBegin {
			if !r.uuComplete || r.uuNeedEnd {
				return fmt.Errorf("%w: truncated UU wrapper", ErrBodyCorrupt)
			}
			return nil
		}
		if !r.uuSeenData {
			return fmt.Errorf("%w: missing UU data", ErrBodyCorrupt)
		}
		return nil
	case rapidyenc.FormatYenc:
	default:
		return fmt.Errorf("%w: unsupported body format", errBodyEncodingUnknown)
	}
	if !r.hasBegin {
		return fmt.Errorf("%w: missing valid =ybegin", ErrBodyCorrupt)
	}
	if r.headerErr != nil {
		return fmt.Errorf("%w: %v", ErrBodyCorrupt, r.headerErr)
	}
	if !r.hasEnd {
		return fmt.Errorf("%w: missing =yend", ErrBodyCorrupt)
	}
	if r.YEnc.FileSize < 0 || r.YEnc.PartSize < 0 || r.YEnc.PartBegin < 0 {
		return fmt.Errorf("%w: negative yEnc metadata", ErrBodyCorrupt)
	}
	if r.YEnc.FileName == "" {
		return fmt.Errorf("%w: missing yEnc name", ErrBodyCorrupt)
	}
	if r.hasPart && r.YEnc.Part <= 0 {
		return fmt.Errorf("%w: =ypart without valid part number", ErrBodyCorrupt)
	}
	if r.YEnc.Part > 0 {
		if !r.hasPart {
			return fmt.Errorf("%w: multipart body missing =ypart", ErrBodyCorrupt)
		}
		if !r.hasEndPart || r.endPart != r.YEnc.Part {
			return fmt.Errorf("%w: multipart trailer part does not match", ErrBodyCorrupt)
		}
		if r.YEnc.Total <= 0 || r.YEnc.Part > r.YEnc.Total {
			return fmt.Errorf("%w: part exceeds total", ErrBodyCorrupt)
		}
		partEnd := r.YEnc.PartBegin + r.YEnc.PartSize
		if r.YEnc.PartSize != int64(r.BytesDecoded) || partEnd > r.YEnc.FileSize ||
			(r.YEnc.Part == r.YEnc.Total && partEnd != r.YEnc.FileSize) ||
			(r.YEnc.Part < r.YEnc.Total && partEnd >= r.YEnc.FileSize) {
			return fmt.Errorf("%w: incoherent multipart size", ErrBodyCorrupt)
		}
	} else if r.YEnc.FileSize != int64(r.BytesDecoded) {
		return fmt.Errorf("%w: decoded size %d does not match file size %d", ErrBodyCorrupt, r.BytesDecoded, r.YEnc.FileSize)
	}
	if r.EndSize != int64(r.BytesDecoded) {
		return fmt.Errorf("%w: trailer size %d does not match decoded size %d", ErrBodyCorrupt, r.EndSize, r.BytesDecoded)
	}
	completeFile := !r.hasPart || (r.YEnc.PartBegin == 0 && r.YEnc.PartSize == r.YEnc.FileSize)
	if r.hasPartCRC && r.CRC != r.partCRC {
		return fmt.Errorf("%w: %w", ErrBodyCorrupt, ErrCRCMismatch)
	}
	// crc32 covers the complete file. It is verifiable for a single-part/full-
	// coverage BODY, but only syntax-checkable for one partial multipart BODY.
	// pcrc32 remains the integrity checksum for that partial part.
	if r.hasFileCRC && completeFile && r.CRC != r.fileCRC {
		return fmt.Errorf("%w: %w", ErrBodyCorrupt, ErrCRCMismatch)
	}
	// ExpectedCRC/hasCrc describe the checksum that was actually applicable to
	// these decoded bytes, avoiding a false mismatch for an unverifiable whole-
	// file CRC on a partial multipart response.
	r.hasCrc = false
	r.ExpectedCRC = 0
	switch {
	case r.hasPartCRC:
		r.hasCrc = true
		r.ExpectedCRC = r.partCRC
	case r.hasFileCRC && completeFile:
		r.hasCrc = true
		r.ExpectedCRC = r.fileCRC
	}
	return nil
}

func extractString(data, substr []byte) (string, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return "", fmt.Errorf("substr not found: %s", substr)
	}

	data = data[start+len(substr):]
	if end := bytes.IndexAny(data, "\x00\r\n"); end != -1 {
		return string(data[:end]), nil
	}

	return string(data), nil
}

func extractInt(data, substr []byte) (int64, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return 0, fmt.Errorf("substr not found: %s", substr)
	}

	data = data[start+len(substr):]
	if end := bytes.IndexAny(data, "\x00\x20\r\n"); end != -1 {
		return strconv.ParseInt(string(data[:end]), 10, 64)
	}

	return strconv.ParseInt(string(data), 10, 64)
}

var (
	errCrcNotfound = errors.New("crc not found")
)

// extractCRC converts a hexadecimal representation of a crc32 hash
func extractCRC(data, substr []byte) (uint32, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return 0, errCrcNotfound
	}

	data = data[start+len(substr):]
	end := bytes.IndexAny(data, "\x00\x20\r\n")
	if end != -1 {
		data = data[:end]
	}

	if len(data) != 8 {
		return 0, fmt.Errorf("CRC must contain exactly 8 hexadecimal digits")
	}
	parsed, err := strconv.ParseUint(string(data), 16, 32)
	return uint32(parsed), err
}
