package proto

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"

	"github.com/go-redis/redis/v8/internal/util"
)

const (
	redisStatus    = '+' // +<string>\r\n
	redisError     = '-' // -<string>\r\n
	redisString    = '$' // $<length>\r\n<bytes>\r\n
	redisInteger   = ':' // :<number>\r\n
	redisNil       = '_' // _\r\n
	redisFloat     = ',' // ,<floating-point-number>\r\n (golang float)
	redisBool      = '#' // true: #t\r\n false: #f\r\n
	redisBlobError = '!' // !<length>\r\n<bytes>\r\n
	redisVerb      = '=' // =<length>\r\nFORMAT:<bytes>\r\n
	redisBigInt    = '(' // (<big number>\r\n
	redisArray     = '*' // *<len>\r\n... (same as resp2)
	redisMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	redisSet       = '~'
	redisAttr      = '|'
	redisPush      = '>'
)

// Streamed           = "EOF:"
// StreamedAggregated = '?'

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

func ParseErrorReply(line []byte) error {
	return RedisError(string(line[1:]))
}

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(rd),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

func (r *Reader) ReadReply() (*Value, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	v := new(Value)
	v.Typ = line[0]

	switch line[0] {
	case redisStatus:
		v.Str = string(line[1:])
	case redisError:
		v.RedisError = RedisError(line[1:])
	case redisInteger:
		v.Integer, err = util.ParseInt(line[1:], 10, 64)
	case redisNil:
		v.RedisError = Nil
	case redisFloat:
		v.Float, err = r.readFloat(line)
	case redisBool:
		v.Boolean, err = r.readBool(line)
	case redisBigInt:
		v.BigInt, err = r.readBigInt(line)

	case redisBlobError:
		var blobErr string
		blobErr, err = r.readString(line)
		if err == nil {
			v.RedisError = RedisError(blobErr)
		}
	case redisString:
		v.Str, err = r.readString(line)
	case redisVerb:
		var s string
		s, err = r.readString(line)
		if err == nil {
			if len(s) < 4 || s[3] != ':' {
				err = fmt.Errorf("redis: can't parse verbatim string reply: %q", line)
			} else {
				v.Str = s[4:]
				v.StrFmt = s[:3]
			}
		}

	case redisArray, redisSet, redisPush:
		v.Slice, err = r.readArraySetPush(line)
	case redisMap:
		v.Map, err = r.readMap(line)
	case redisAttr:
		var (
			attr map[*Value]*Value
			val  *Value
		)
		attr, err = r.readMap(line)
		if err != nil && err != Nil {
			return nil, err
		}

		val, err = r.ReadReply()
		if err != nil {
			return nil, err
		}

		v.Attribute = &AttributeType{
			Attr:  attr,
			Value: val,
		}
	default:
		err = fmt.Errorf("redis: invalid reply: %q", line)
	}

	if err == Nil {
		if v.RedisError == nil {
			v.RedisError = Nil
		}
		err = nil
	}

	return v, err
}

func (r *Reader) readMap(line []byte) (map[*Value]*Value, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}

	// Maps can have any other type as field and value,
	// however Redis will use only a subset of the available possibilities.
	// For instance it is very unlikely that Redis commands would return
	// an Array as a key, however Lua scripts and modules will likely be able to do so.
	m := make(map[*Value]*Value)
	for i := 0; i < n; i++ {
		k, err := r.ReadReply()
		if err != nil {
			return nil, err
		}

		v, err := r.ReadReply()
		if err != nil {
			return nil, err
		}

		m[k] = v
	}
	return m, nil
}

func (r *Reader) readArraySetPush(line []byte) ([]*Value, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}

	vs := make([]*Value, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadReply()
		if err != nil {
			return nil, err
		}
		vs[i] = v
	}
	return vs, nil
}

func (r *Reader) readBigInt(line []byte) (*big.Int, error) {
	i := new(big.Int)
	if i, ok := i.SetString(string(line[1:]), 10); ok {
		return i, nil
	}
	return nil, fmt.Errorf("redis: can't parse bigInt reply: %q", line)
}

func (r *Reader) readBool(line []byte) (bool, error) {
	switch string(line[1:]) {
	case "t":
		return true, nil
	case "f":
		return false, nil
	default:
		return false, fmt.Errorf("redis: can't parse bool reply: %q", line)
	}
}

var (
	uvinf    = math.Inf(1)
	uvneginf = math.Inf(-1)
)

func (r *Reader) readFloat(line []byte) (float64, error) {
	v := string(line[1:])
	switch v {
	case "inf":
		return uvinf, nil
	case "-inf":
		return uvneginf, nil
	default:
		return strconv.ParseFloat(v, 64)
	}
}

func (r *Reader) readString(line []byte) (string, error) {
	n, err := replyLen(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return string(b[:n]), nil
}

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

func replyLen(line []byte) (n int, err error) {
	n, err = util.Atoi(line[1:])
	if err != nil {
		return 0, err
	}

	if n < -1 {
		return 0, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case redisString, redisVerb, redisBlobError,
		redisArray, redisSet, redisPush, redisMap, redisAttr:
		if n == -1 {
			return 0, Nil
		}
	}
	return n, nil
}
