package proto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"

	"github.com/redis/go-redis/v9/internal/util"
)

// DefaultBufferSize is the default size for read/write buffers (32 KiB).
const DefaultBufferSize = 32 * 1024

// redis resp protocol data type.
const (
	RespStatus    = '+' // +<string>\r\n
	RespError     = '-' // -<string>\r\n
	RespString    = '$' // $<length>\r\n<bytes>\r\n
	RespInt       = ':' // :<number>\r\n
	RespNil       = '_' // _\r\n
	RespFloat     = ',' // ,<floating-point-number>\r\n (golang float)
	RespBool      = '#' // true: #t\r\n false: #f\r\n
	RespBlobError = '!' // !<length>\r\n<bytes>\r\n
	RespVerbatim  = '=' // =<length>\r\nFORMAT:<bytes>\r\n
	RespBigInt    = '(' // (<big number>\r\n
	RespArray     = '*' // *<len>\r\n... (same as resp2)
	RespMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	RespSet       = '~' // ~<len>\r\n... (same as Array)
	RespAttr      = '|' // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	RespPush      = '>' // ><len>\r\n... (same as Array)
)

// Not used temporarily.
// Redis has not used these two data types for the time being, and will implement them later.
// Streamed           = "EOF:"
// StreamedAggregated = '?'

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil") // nolint:errname

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

func ParseErrorReply(line []byte) error {
	msg := string(line[1:])
	return parseTypedRedisError(msg)
}

//------------------------------------------------------------------------------

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReaderSize(rd, DefaultBufferSize),
	}
}

func NewReaderSize(rd io.Reader, size int) *Reader {
	return &Reader{
		rd: bufio.NewReaderSize(rd, size),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

// Size returns the size of the underlying buffer in bytes.
func (r *Reader) Size() int {
	return r.rd.Size()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// PeekReplyType returns the data type of the next response without advancing the Reader,
// and discard the attribute type.
func (r *Reader) PeekReplyType() (byte, error) {
	b, err := r.rd.Peek(1)
	if err != nil {
		return 0, err
	}
	if b[0] == RespAttr {
		if err = r.DiscardNext(); err != nil {
			return 0, err
		}
		return r.PeekReplyType()
	}
	return b[0], nil
}

// pushHeaderMaxPeek bounds the read-ahead when peeking a push-notification name
// (generous for real names; also caps a corrupt/long frame). Further capped by
// the reader's buffer size so Peek never overflows it.
const pushHeaderMaxPeek = 512

// MinRESP3ReadBufferSize is the smallest read buffer that keeps any
// client-reserved push header within the peek window, so such a frame is never
// consumed by generic push processing (see PeekPushNotificationName).
const MinRESP3ReadBufferSize = pushHeaderMaxPeek

// ErrPushNotificationNameTooLong is returned when the header doesn't fit the peek
// window/buffer, so the name can't be read without consuming the frame; callers
// should ReadReply rather than leave it at the buffer head.
var ErrPushNotificationNameTooLong = errors.New("redis: push notification name exceeds peek window")

func (r *Reader) PeekPushNotificationName() (string, error) {
	// "prime" the buffer and confirm the next reply is a push frame.
	c, err := r.Peek(1)
	if err != nil {
		return "", err
	}
	if c[0] != RespPush {
		return "", fmt.Errorf("redis: can't peek push notification name, next reply is not a push notification")
	}

	// Cap by buffer size so Peek never returns bufio.ErrBufferFull.
	maxPeek := pushHeaderMaxPeek
	if sz := r.Size(); sz > 0 && sz < maxPeek {
		maxPeek = sz
	}

	// Grow the window (refilling from the socket, bounded by the caller's read
	// deadline) so a header split across reads completes instead of stalling
	// repeated drain passes on the same partial bytes.
	toPeek := r.Buffered()
	if toPeek < 1 {
		toPeek = 1
	}
	for {
		if toPeek > maxPeek {
			toPeek = maxPeek
		}
		buf, peekErr := r.rd.Peek(toPeek)
		name, complete, malformed := parsePushNotificationName(buf)
		if malformed != nil {
			return "", malformed
		}
		if complete {
			return name, nil
		}
		if peekErr != nil {
			return "", peekErr // can't get more yet (deadline/EOF); caller retries
		}
		if len(buf) >= maxPeek {
			return "", ErrPushNotificationNameTooLong // doesn't fit; caller consumes
		}
		toPeek = len(buf) + 1 // force one more fill on the next Peek
	}
}

// parsePushNotificationName reads the notification name from the head of a RESP3
// push frame. complete=true returns the name; complete=false with a nil err means
// buf lacks the full name line yet (caller supplies more); a non-nil err means
// buf is not a valid push head.
func parsePushNotificationName(buf []byte) (name string, complete bool, err error) {
	if len(buf) < 1 {
		return "", false, nil
	}
	if buf[0] != RespPush {
		return "", false, fmt.Errorf("redis: can't parse push notification: %q", buf)
	}
	// Skip the push type byte, then the "<count>\r\n" array-length line.
	rest, ok, perr := pushSkipUintLine(buf[1:])
	if perr != nil {
		return "", false, perr
	}
	if !ok {
		return "", false, nil
	}
	// The first array element is the name: bulk "$<len>\r\n<name>\r\n" or simple
	// "+<name>\r\n".
	if len(rest) < 1 {
		return "", false, nil
	}
	switch rest[0] {
	case RespString:
		rest, ok, perr = pushSkipUintLine(rest[1:])
		if perr != nil {
			return "", false, perr
		}
		if !ok {
			return "", false, nil
		}
	case RespStatus:
		rest = rest[1:]
	default:
		return "", false, fmt.Errorf("redis: can't parse push notification name: %q", buf)
	}
	// The name runs up to the next CRLF.
	nameBytes, ok := pushLineUpToCRLF(rest)
	if !ok {
		return "", false, nil
	}
	return util.BytesToString(nameBytes), true, nil
}

// pushSkipUintLine consumes a "<digits>\r\n" line. ok=false (nil err) means it's
// not fully present yet; a non-nil err means a non-digit appeared before the CRLF.
func pushSkipUintLine(buf []byte) (rest []byte, ok bool, err error) {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[i+2:], true, nil
		}
		if buf[i] < '0' || buf[i] > '9' {
			return nil, false, fmt.Errorf("redis: can't parse push notification: %q", buf)
		}
	}
	return nil, false, nil
}

// pushLineUpToCRLF returns the bytes preceding the next CRLF. ok=false means no
// CRLF is present in buf yet.
func pushLineUpToCRLF(buf []byte) (line []byte, ok bool) {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[:i], true
		}
	}
	return nil, false
}

// ReadLine Return a valid reply, it will check the protocol or redis error,
// and discard the attribute type.
func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case RespError:
		return nil, ParseErrorReply(line)
	case RespNil:
		return nil, Nil
	case RespBlobError:
		var blobErr string
		blobErr, err = r.readStringReply(line)
		if err == nil {
			err = parseTypedRedisError(blobErr)
		}
		return nil, err
	case RespAttr:
		if err = r.Discard(line); err != nil {
			return nil, err
		}
		return r.ReadLine()
	}

	// Compatible with RESP2
	if IsNilReply(line) {
		return nil, Nil
	}

	return line, nil
}

// readLine returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b[:len(b)-2], nil
}

func (r *Reader) ReadReply() (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case RespStatus:
		return string(line[1:]), nil
	case RespInt:
		return util.ParseInt(line[1:], 10, 64)
	case RespFloat:
		return r.readFloat(line)
	case RespBool:
		return r.readBool(line)
	case RespBigInt:
		return r.readBigInt(line)

	case RespString:
		return r.readStringReply(line)
	case RespVerbatim:
		return r.readVerb(line)

	case RespArray, RespSet, RespPush:
		return r.readSlice(line)
	case RespMap:
		return r.readMap(line)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

func (r *Reader) readFloat(line []byte) (float64, error) {
	v := string(line[1:])
	switch string(line[1:]) {
	case "inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	case "nan", "-nan":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(v, 64)
}

func (r *Reader) readBool(line []byte) (bool, error) {
	switch string(line[1:]) {
	case "t":
		return true, nil
	case "f":
		return false, nil
	}
	return false, fmt.Errorf("redis: can't parse bool reply: %q", line)
}

func (r *Reader) readBigInt(line []byte) (*big.Int, error) {
	i := new(big.Int)
	if i, ok := i.SetString(string(line[1:]), 10); ok {
		return i, nil
	}
	return nil, fmt.Errorf("redis: can't parse bigInt reply: %q", line)
}

func (r *Reader) readStringReply(line []byte) (string, error) {
	n, err := replyLen(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return util.BytesToString(b[:n]), nil
}

func (r *Reader) readVerb(line []byte) (string, error) {
	s, err := r.readStringReply(line)
	if err != nil {
		return "", err
	}
	if len(s) < 4 || s[3] != ':' {
		return "", fmt.Errorf("redis: can't parse verbatim string reply: %q", line)
	}
	return s[4:], nil
}

func (r *Reader) readSlice(line []byte) ([]interface{}, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}

	val := make([]interface{}, n)
	for i := 0; i < len(val); i++ {
		v, err := r.ReadReply()
		if err != nil {
			if err == Nil {
				val[i] = nil
				continue
			}
			if err, ok := err.(RedisError); ok {
				val[i] = err
				continue
			}
			return nil, err
		}
		val[i] = v
	}
	return val, nil
}

func (r *Reader) readMap(line []byte) (map[interface{}]interface{}, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}
	m := make(map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		k, err := r.ReadReply()
		if err != nil {
			return nil, err
		}
		v, err := r.ReadReply()
		if err != nil {
			if err == Nil {
				m[k] = nil
				continue
			}
			if err, ok := err.(RedisError); ok {
				m[k] = err
				continue
			}
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// -------------------------------

func (r *Reader) ReadInt() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespInt, RespStatus:
		return util.ParseInt(line[1:], 10, 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return util.ParseInt([]byte(s), 10, 64)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return 0, err
		}
		if !b.IsInt64() {
			return 0, fmt.Errorf("bigInt(%s) value out of range", b.String())
		}
		return b.Int64(), nil
	}
	return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
}

func (r *Reader) ReadUint() (uint64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespInt, RespStatus:
		return util.ParseUint(line[1:], 10, 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return util.ParseUint([]byte(s), 10, 64)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return 0, err
		}
		if !b.IsUint64() {
			return 0, fmt.Errorf("bigInt(%s) value out of range", b.String())
		}
		return b.Uint64(), nil
	}
	return 0, fmt.Errorf("redis: can't parse uint reply: %.100q", line)
}

func (r *Reader) ReadFloat() (float64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespFloat:
		return r.readFloat(line)
	case RespStatus:
		return strconv.ParseFloat(string(line[1:]), 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}
	return 0, fmt.Errorf("redis: can't parse float reply: %.100q", line)
}

func (r *Reader) ReadString() (string, error) {
	line, err := r.ReadLine()
	if err != nil {
		return "", err
	}

	switch line[0] {
	case RespStatus, RespInt, RespFloat:
		return string(line[1:]), nil
	case RespString:
		return r.readStringReply(line)
	case RespBool:
		b, err := r.readBool(line)
		return strconv.FormatBool(b), err
	case RespVerbatim:
		return r.readVerb(line)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return "", err
		}
		return b.String(), nil
	}
	return "", fmt.Errorf("redis: can't parse reply=%.100q reading string", line)
}

func (r *Reader) ReadBool() (bool, error) {
	s, err := r.ReadString()
	if err != nil {
		return false, err
	}
	return s == "OK" || s == "1" || s == "true", nil
}

func (r *Reader) ReadSlice() ([]interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	return r.readSlice(line)
}

// ReadFixedArrayLen read fixed array length.
func (r *Reader) ReadFixedArrayLen(fixedLen int) error {
	n, err := r.ReadArrayLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements in the array, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadArrayLen Read and return the length of the array.
func (r *Reader) ReadArrayLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespArray, RespSet, RespPush:
		return replyLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array/set/push reply: %.100q", line)
	}
}

// ReadFixedMapLen reads fixed map length.
func (r *Reader) ReadFixedMapLen(fixedLen int) error {
	n, err := r.ReadMapLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements in the map, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadMapLen reads the length of the map type.
// If responding to the array type (RespArray/RespSet/RespPush),
// it must be a multiple of 2 and return n/2.
// Other types will return an error.
func (r *Reader) ReadMapLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespMap:
		return replyLen(line)
	case RespArray, RespSet, RespPush:
		// Some commands and RESP2 protocol may respond to array types.
		n, err := replyLen(line)
		if err != nil {
			return 0, err
		}
		if n%2 != 0 {
			return 0, fmt.Errorf("redis: the length of the array must be a multiple of 2, got: %d", n)
		}
		return n / 2, nil
	default:
		return 0, fmt.Errorf("redis: can't parse map reply: %.100q", line)
	}
}

// DiscardNext read and discard the data represented by the next line.
func (r *Reader) DiscardNext() error {
	line, err := r.readLine()
	if err != nil {
		return err
	}
	return r.Discard(line)
}

// Discard the data represented by line.
func (r *Reader) Discard(line []byte) (err error) {
	if len(line) == 0 {
		return errors.New("redis: invalid line")
	}
	switch line[0] {
	case RespStatus, RespError, RespInt, RespNil, RespFloat, RespBool, RespBigInt:
		return nil
	}

	n, err := replyLen(line)
	if err != nil && err != Nil {
		return err
	}

	switch line[0] {
	case RespBlobError, RespString, RespVerbatim:
		// +\r\n
		_, err = r.rd.Discard(n + 2)
		return err
	case RespArray, RespSet, RespPush:
		for i := 0; i < n; i++ {
			if err = r.DiscardNext(); err != nil {
				return err
			}
		}
		return nil
	case RespMap, RespAttr:
		// Read key & value.
		for i := 0; i < n*2; i++ {
			if err = r.DiscardNext(); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("redis: can't parse %.100q", line)
}

func replyLen(line []byte) (n int, err error) {
	n, err = util.Atoi(line[1:])
	if err != nil {
		return 0, err
	}

	if n < -1 {
		return 0, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case RespString, RespVerbatim, RespBlobError,
		RespArray, RespSet, RespPush, RespMap, RespAttr:
		if n == -1 {
			return 0, Nil
		}
	}
	return n, nil
}

// IsNilReply detects redis.Nil of RESP2.
func IsNilReply(line []byte) bool {
	return len(line) == 3 &&
		(line[0] == RespString || line[0] == RespArray) &&
		line[1] == '-' && line[2] == '1'
}

// ReadRawReply reads the next RESP reply and returns it as raw bytes without parsing.
func (r *Reader) ReadRawReply() ([]byte, error) {
	return r.readRawReplyBuf(nil)
}

func (r *Reader) readRawReplyBuf(buf []byte) ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return buf, err
	}

	buf = append(buf, line...)
	buf = append(buf, '\r', '\n')

	switch line[0] {
	case RespStatus, RespError, RespInt, RespNil, RespFloat, RespBool, RespBigInt:
		return buf, nil

	case RespString, RespVerbatim, RespBlobError:
		n, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return buf, nil
			}
			return buf, err
		}
		curLen := len(buf)
		buf = append(buf, make([]byte, n+2)...)
		_, err = io.ReadFull(r.rd, buf[curLen:])
		return buf, err

	case RespArray, RespSet, RespPush:
		n, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return buf, nil
			}
			return buf, err
		}
		for i := 0; i < n; i++ {
			buf, err = r.readRawReplyBuf(buf)
			if err != nil {
				return buf, err
			}
		}
		return buf, nil

	case RespMap:
		n, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return buf, nil
			}
			return buf, err
		}
		for i := 0; i < n*2; i++ {
			buf, err = r.readRawReplyBuf(buf)
			if err != nil {
				return buf, err
			}
		}
		return buf, nil

	case RespAttr:
		// Per RESP3 spec, an attribute is always followed by the actual command reply.
		// We need to read the attribute's key-value pairs AND the following reply.
		n, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return buf, nil
			}
			return buf, err
		}
		// Read the attribute key-value pairs
		for i := 0; i < n*2; i++ {
			buf, err = r.readRawReplyBuf(buf)
			if err != nil {
				return buf, err
			}
		}
		// Read the command reply that follows the attribute
		return r.readRawReplyBuf(buf)
	}

	return buf, fmt.Errorf("redis: can't read raw reply: %.100q", line)
}

var crlf = []byte{'\r', '\n'}

// ReadRawReplyWriteTo streams the next RESP reply directly to w without intermediate allocations.
// Returns the number of bytes written and any error encountered.
func (r *Reader) ReadRawReplyWriteTo(w io.Writer) (int64, error) {
	return r.readRawReplyWriteTo(w)
}

func (r *Reader) readRawReplyWriteTo(w io.Writer) (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	var written int64
	n, err := w.Write(line)
	written += int64(n)
	if err != nil {
		return written, err
	}
	n, err = w.Write(crlf)
	written += int64(n)
	if err != nil {
		return written, err
	}

	switch line[0] {
	case RespStatus, RespError, RespInt, RespNil, RespFloat, RespBool, RespBigInt:
		return written, nil

	case RespString, RespVerbatim, RespBlobError:
		dataLen, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return written, nil
			}
			return written, err
		}
		copied, err := io.CopyN(w, r.rd, int64(dataLen)+2)
		written += copied
		return written, err

	case RespArray, RespSet, RespPush:
		count, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return written, nil
			}
			return written, err
		}
		for i := 0; i < count; i++ {
			n, err := r.readRawReplyWriteTo(w)
			written += n
			if err != nil {
				return written, err
			}
		}
		return written, nil

	case RespMap:
		count, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return written, nil
			}
			return written, err
		}
		for i := 0; i < count*2; i++ {
			n, err := r.readRawReplyWriteTo(w)
			written += n
			if err != nil {
				return written, err
			}
		}
		return written, nil

	case RespAttr:
		// Per RESP3 spec, an attribute is always followed by the actual command reply.
		// We need to read the attribute's key-value pairs AND the following reply.
		count, err := replyLen(line)
		if err != nil {
			if err == Nil {
				return written, nil
			}
			return written, err
		}
		// Read the attribute key-value pairs
		for i := 0; i < count*2; i++ {
			n, err := r.readRawReplyWriteTo(w)
			written += n
			if err != nil {
				return written, err
			}
		}
		// Read the command reply that follows the attribute
		n, err := r.readRawReplyWriteTo(w)
		written += n
		return written, err
	}

	return written, fmt.Errorf("redis: can't read raw reply: %.100q", line)
}
