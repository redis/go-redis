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
	RespStatus    = '+' // +<string>


	RespError     = '-' // -<string>


	RespInt       = ':' // :<number>


	RespString    = '$' // $<length>

<data>


	RespArray     = '*' // *<len>

<element_1>...<element_N>
	RespNil       = '_' // _


	RespBool      = '#' // #t

 or #f


	RespFloat     = ',' // ,<float>


	RespBigInt    = '(' // (<bigint>


	RespVerbatim  = '=' // =<length>

<format>:<data>


	RespMap       = '%' // %<len>

<key_1><val_1>...<key_N><val_N>
	RespSet       = '~' // ~<len>

<element_1>...<element_N>
	RespAttr      = '|' // |<len>

<key_1><val_1>...<key_N><val_N>
	RespPush      = '>' // ><len>

<element_1>...<element_N>
	RespConst     = '+' // RESP3 constants like OK, PONG, etc.
	RespStream    = '?' // RESP3 streaming data.
	RespChunk     = ';' // RESP3 chunked data.
)

type MultiBulkParse func(*Reader, int) (interface{}, error)

type Reader struct {
	rd *bufio.Reader

	_numBuf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:      bufio.NewReaderSize(rd, DefaultBufferSize),
		_numBuf: make([]byte, 64),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Read(p []byte) (int, error) {
	return r.rd.Read(p)
}

func (r *Reader) ReadByte() (byte, error) {
	return r.rd.ReadByte()
}

func (r *Reader) UnreadByte() error {
	return r.rd.UnreadByte()
}

func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.rd.ReadSlice('
')
	if err != nil {
		if err == bufio.ErrBufferFull {
			return nil, errors.New("redis: line too long")
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '
' {
		return nil, fmt.Errorf("redis: invalid line: %q", line)
	}
	return line[:len(line)-2], nil
}

func (r *Reader) ReadReply() (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case RespStatus:
		return util.BytesToString(line[1:]), nil
	case RespError:
		return util.BytesToString(line[1:]), nil
	case RespInt:
		return r.readInt(line[1:])
	case RespString:
		return r.readString(line[1:])
	case RespArray:
		return r.readArray(line[1:])
	case RespNil:
		return nil, nil
	case RespBool:
		return r.readBool(line[1:])
	case RespFloat:
		return r.readFloat(line[1:])
	case RespBigInt:
		return r.readBigInt(line[1:])
	case RespVerbatim:
		return r.readVerbatim(line[1:])
	case RespMap:
		return r.readMap(line[1:])
	case RespSet:
		return r.readSet(line[1:])
	case RespAttr:
		return r.readAttr(line[1:])
	case RespPush:
		return r.readPush(line[1:])
	}
	return nil, fmt.Errorf("redis: unknown reply: %q", line)
}

func (r *Reader) readInt(line []byte) (int64, error) {
	return strconv.ParseInt(util.BytesToString(line), 10, 64)
}

func (r *Reader) readString(line []byte) (string, error) {
	n, err := r.readInt(line)
	if err != nil {
		return "", err
	}
	if n == -1 {
		return "", nil
	}

	b := make([]byte, n+2)
	if _, err := io.ReadFull(r.rd, b); err != nil {
		return "", err
	}
	if b[n] != '
' || b[n+1] != '
' {
		return "", fmt.Errorf("redis: invalid line: %q", b)
	}
	return util.BytesToString(b[:n]), nil
}

func (r *Reader) readArray(line []byte) (interface{}, error) {
	n, err := r.readInt(line)
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}

	elements := make([]interface{}, n)
	for i := 0; i < int(n); i++ {
		v, err := r.ReadReply()
		if err != nil {
			return nil, err
		}
		elements[i] = v
	}
	return elements, nil
}

func (r *Reader) readBool(line []byte) (bool, error) {
	if len(line) != 1 {
		return false, fmt.Errorf("redis: invalid bool: %q", line)
	}
	switch line[0] {
	case 't':
		return true, nil
	case 'f':
		return false, nil
	}
	return false, fmt.Errorf("redis: invalid bool: %q", line)
}

func (r *Reader) readFloat(line []byte) (float64, error) {
	return strconv.ParseFloat(util.BytesToString(line), 64)
}

func (r *Reader) readBigInt(line []byte) (*big.Int, error) {
	i := new(big.Int)
	if _, ok := i.SetString(util.BytesToString(line), 10); !ok {
		return nil, fmt.Errorf("redis: invalid big int: %q", line)
	}
	return i, nil
}

func (r *Reader) readVerbatim(line []byte) (string, error) {
	n, err := r.readInt(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+2)
	if _, err := io.ReadFull(r.rd, b); err != nil {
		return "", err
	}
	if b[n] != '
' || b[n+1] != '
' {
		return "", fmt.Errorf("redis: invalid line: %q", b)
	}
	return util.BytesToString(b[4:n]), nil
}

func (r *Reader) readMap(line []byte) (map[interface{}]interface{}, error) {
	n, err := r.readInt(line)
	if err != nil {
		return nil, err
	}

	m := make(map[interface{}]interface{}, n)
	for i := 0; i < int(n); i++ {
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

func (r *Reader) readSet(line []byte) ([]interface{}, error) {
	return r.readArray(line)
}

func (r *Reader) readAttr(line []byte) (map[interface{}]interface{}, error) {
	return r.readMap(line)
}

func (r *Reader) readPush(line []byte) (interface{}, error) {
	return r.readArray(line)
}

func (r *Reader) ReadRawReply() ([]byte, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case RespStatus, RespError, RespInt, RespNil, RespBool, RespFloat, RespBigInt:
		return r.appendRawLine(nil, line)
	case RespString, RespVerbatim:
		return r.readRawString(line)
	case RespArray, RespSet, RespPush:
		return r.readRawArray(line)
	case RespMap, RespAttr:
		return r.readRawMap(line)
	}
	return nil, fmt.Errorf("redis: unknown reply: %q", line)
}

func (r *Reader) appendRawLine(b []byte, line []byte) ([]byte, error) {
	b = append(b, line...)
	b = append(b, '
', '
')
	return b, nil
}

func (r *Reader) readRawString(line []byte) ([]byte, error) {
	n, err := r.readInt(line[1:])
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return r.appendRawLine(nil, line)
	}

	b := make([]byte, len(line)+2+int(n)+2)
	copy(b, line)
	b[len(line)] = '
'
	b[len(line)+1] = '
'
	if _, err := io.ReadFull(r.rd, b[len(line)+2:]); err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Reader) readRawArray(line []byte) ([]byte, error) {
	n, err := r.readInt(line[1:])
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return r.appendRawLine(nil, line)
	}

	b, err := r.appendRawLine(nil, line)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(n); i++ {
		reply, err := r.ReadRawReply()
		if err != nil {
			return nil, err
		}
		b = append(b, reply...)
	}
	return b, nil
}

func (r *Reader) readRawMap(line []byte) ([]byte, error) {
	n, err := r.readInt(line[1:])
	if err != nil {
		return nil, err
	}

	b, err := r.appendRawLine(nil, line)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(n)*2; i++ {
		reply, err := r.ReadRawReply()
		if err != nil {
			return nil, err
		}
		b = append(b, reply...)
	}
	return b, nil
}

