package redis

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/vmihailenco/bufio"
)

type replyType int

const (
	ifaceSlice replyType = iota
	stringSlice
	boolSlice
	stringStringMap
	stringFloatMap
)

// Represents Redis nil reply.
var Nil = errors.New("(nil)")

var (
	errReaderTooSmall = errors.New("redis: reader is too small")
	errValNotSet      = errors.New("redis: value is not set")
	errInvalidType    = errors.New("redis: invalid reply type")
)

//------------------------------------------------------------------------------

type parserError struct {
	err error
}

func (e *parserError) Error() string {
	return e.err.Error()
}

//------------------------------------------------------------------------------

func appendReq(buf []byte, args []string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendUint(buf, uint64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendUint(buf, uint64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

//------------------------------------------------------------------------------

type reader interface {
	ReadLine() ([]byte, bool, error)
	Read([]byte) (int, error)
	ReadN(n int) ([]byte, error)
	Buffered() int
}

func readLine(rd reader) ([]byte, error) {
	line, isPrefix, err := rd.ReadLine()
	if err != nil {
		return line, err
	}
	if isPrefix {
		return line, errReaderTooSmall
	}
	return line, nil
}

func readN(rd reader, n int) ([]byte, error) {
	buf, err := rd.ReadN(n)
	if err == bufio.ErrBufferFull {
		newBuf := make([]byte, n)
		r := copy(newBuf, buf)
		buf = newBuf

		for {
			nn, err := rd.Read(buf[r:])
			r += nn
			if r >= n {
				// Ignore error if we read enough.
				break
			}
			if err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	return buf, nil
}

//------------------------------------------------------------------------------

func ParseReq(rd reader) ([]string, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] != '*' {
		return []string{string(line)}, nil
	}
	numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, numReplies)
	for i := int64(0); i < numReplies; i++ {
		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("redis: expected '$', but got %q", line)
		}

		argLen, err := strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return nil, err
		}

		arg, err := readN(rd, int(argLen)+2)
		if err != nil {
			return nil, err
		}
		args = append(args, string(arg[:argLen]))
	}
	return args, nil
}

//------------------------------------------------------------------------------

func parseReply(rd reader) (interface{}, error) {
	return _parseReply(rd, ifaceSlice)
}

func parseStringSliceReply(rd reader) (interface{}, error) {
	return _parseReply(rd, stringSlice)
}

func parseBoolSliceReply(rd reader) (interface{}, error) {
	return _parseReply(rd, boolSlice)
}

func parseStringStringMapReply(rd reader) (interface{}, error) {
	return _parseReply(rd, stringStringMap)
}

func parseStringFloatMapReply(rd reader) (interface{}, error) {
	return _parseReply(rd, stringFloatMap)
}

func _parseReply(rd reader, typ replyType) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return 0, &parserError{err}
	}

	switch line[0] {
	case '-':
		return nil, errors.New(string(line[1:]))
	case '+':
		return string(line[1:]), nil
	case ':':
		v, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return 0, &parserError{err}
		}
		return v, nil
	case '$':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return "", Nil
		}

		replyLenInt32, err := strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return "", &parserError{err}
		}
		replyLen := int(replyLenInt32) + 2

		line, err = readN(rd, replyLen)
		if err != nil {
			return "", &parserError{err}
		}
		return string(line[:len(line)-2]), nil
	case '*':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		repliesNum, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, &parserError{err}
		}

		switch typ {
		case stringSlice:
			vals := make([]string, 0, repliesNum)
			for i := int64(0); i < repliesNum; i++ {
				vi, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				if v, ok := vi.(string); ok {
					vals = append(vals, v)
				} else {
					return nil, errInvalidType
				}
			}
			return vals, nil
		case boolSlice:
			vals := make([]bool, 0, repliesNum)
			for i := int64(0); i < repliesNum; i++ {
				vi, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				if v, ok := vi.(int64); ok {
					vals = append(vals, v == 1)
				} else {
					return nil, errInvalidType
				}
			}
			return vals, nil
		case stringStringMap: // TODO: Consider handling Nil values.
			m := make(map[string]string, repliesNum/2)
			for i := int64(0); i < repliesNum; i += 2 {
				keyI, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				key, ok := keyI.(string)
				if !ok {
					return nil, errInvalidType
				}

				valueI, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				value, ok := valueI.(string)
				if !ok {
					return nil, errInvalidType
				}

				m[key] = value
			}
			return m, nil
		case stringFloatMap: // TODO: Consider handling Nil values.
			m := make(map[string]float64, repliesNum/2)
			for i := int64(0); i < repliesNum; i += 2 {
				keyI, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				key, ok := keyI.(string)
				if !ok {
					return nil, errInvalidType
				}

				valueI, err := parseReply(rd)
				if err != nil {
					return nil, err
				}
				valueS, ok := valueI.(string)
				if !ok {
					return nil, errInvalidType
				}
				value, err := strconv.ParseFloat(valueS, 64)
				if err != nil {
					return nil, &parserError{err}
				}

				m[key] = value
			}
			return m, nil
		default:
			vals := make([]interface{}, 0, repliesNum)
			for i := int64(0); i < repliesNum; i++ {
				v, err := parseReply(rd)
				if err == Nil {
					vals = append(vals, nil)
				} else if err != nil {
					return nil, err
				} else {
					vals = append(vals, v)
				}
			}
			return vals, nil
		}
	default:
		return nil, &parserError{fmt.Errorf("redis: can't parse %q", line)}
	}
	panic("not reachable")
}
