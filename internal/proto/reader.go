package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

	"gopkg.in/redis.v5/internal"
)

const bytesAllocLimit = 1024 * 1024 // 1mb

const errEmptyReply = internal.RedisError("redis: reply is empty")

type MultiBulkParse func(*Reader, int64) (interface{}, error)

type Reader struct {
	src *bufio.Reader
	buf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		src: bufio.NewReader(rd),
		buf: make([]byte, 0, defaultBufSize),
	}
}

func (p *Reader) PeekBuffered() []byte {
	if n := p.src.Buffered(); n != 0 {
		b, _ := p.src.Peek(n)
		return b
	}
	return nil
}

func (p *Reader) ReadN(n int) ([]byte, error) {
	return readN(p.src, p.buf, n)
}

func (p *Reader) ReadLine() ([]byte, error) {
	line, isPrefix, err := p.src.ReadLine()
	if err != nil {
		return nil, err
	}
	if isPrefix {
		return nil, bufio.ErrBufferFull
	}
	if len(line) == 0 {
		return nil, errEmptyReply
	}
	if isNilReply(line) {
		return nil, internal.Nil
	}
	return line, nil
}

func (p *Reader) ReadReply(m MultiBulkParse) (interface{}, error) {
	line, err := p.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case ErrorReply:
		return nil, parseErrorValue(line)
	case StatusReply:
		return parseStatusValue(line)
	case IntReply:
		return parseIntValue(line)
	case StringReply:
		return p.readBytesValue(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		return m(p, n)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

func (p *Reader) ReadIntReply() (int64, error) {
	line, err := p.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, parseErrorValue(line)
	case IntReply:
		return parseIntValue(line)
	default:
		return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
	}
}

func (p *Reader) ReadBytesReply() ([]byte, error) {
	line, err := p.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, parseErrorValue(line)
	case StringReply:
		return p.readBytesValue(line)
	case StatusReply:
		return parseStatusValue(line)
	default:
		return nil, fmt.Errorf("redis: can't parse string reply: %.100q", line)
	}
}

func (p *Reader) ReadStringReply() (string, error) {
	b, err := p.ReadBytesReply()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Reader) ReadFloatReply() (float64, error) {
	s, err := p.ReadStringReply()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(s, 64)
}

func (p *Reader) ReadArrayReply(m MultiBulkParse) (interface{}, error) {
	line, err := p.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, parseErrorValue(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		return m(p, n)
	default:
		return nil, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (p *Reader) ReadArrayLen() (int64, error) {
	line, err := p.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, parseErrorValue(line)
	case ArrayReply:
		return parseArrayLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (p *Reader) ReadScanReply() ([]string, uint64, error) {
	n, err := p.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}
	if n != 2 {
		return nil, 0, fmt.Errorf("redis: got %d elements in scan reply, expected 2", n)
	}

	s, err := p.ReadStringReply()
	if err != nil {
		return nil, 0, err
	}

	cursor, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, 0, err
	}

	n, err = p.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}

	keys := make([]string, n)
	for i := int64(0); i < n; i++ {
		key, err := p.ReadStringReply()
		if err != nil {
			return nil, 0, err
		}
		keys[i] = key
	}

	return keys, cursor, err
}

func (p *Reader) readBytesValue(line []byte) ([]byte, error) {
	if isNilReply(line) {
		return nil, internal.Nil
	}

	replyLen, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}

	b, err := p.ReadN(replyLen + 2)
	if err != nil {
		return nil, err
	}
	return b[:replyLen], nil
}

// --------------------------------------------------------------------

func readN(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 && b == nil {
		return make([]byte, 0), nil
	}

	if cap(b) >= n {
		b = b[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	b = b[:cap(b)]

	pos := 0
	for pos < n {
		diff := n - len(b)
		if diff > bytesAllocLimit {
			diff = bytesAllocLimit
		}
		b = append(b, make([]byte, diff)...)

		nn, err := io.ReadFull(r, b[pos:])
		if err != nil {
			return nil, err
		}
		pos += nn
	}

	return b, nil
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func formatUint(u uint64) string {
	return strconv.FormatUint(u, 10)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func isNilReply(b []byte) bool {
	return len(b) == 3 &&
		(b[0] == StringReply || b[0] == ArrayReply) &&
		b[1] == '-' && b[2] == '1'
}

func parseErrorValue(line []byte) error {
	return internal.RedisError(string(line[1:]))
}

func parseStatusValue(line []byte) ([]byte, error) {
	return line[1:], nil
}

func parseIntValue(line []byte) (int64, error) {
	return strconv.ParseInt(string(line[1:]), 10, 64)
}

func parseArrayLen(line []byte) (int64, error) {
	if isNilReply(line) {
		return 0, internal.Nil
	}
	return parseIntValue(line)
}
