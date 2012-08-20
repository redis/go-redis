package redis

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/vmihailenco/bufio"
)

//------------------------------------------------------------------------------

var Nil = errors.New("(nil)")

//------------------------------------------------------------------------------

var (
	errReaderTooSmall = errors.New("redis: reader is too small")
	errValNotSet      = errors.New("redis: value is not set")
)

//------------------------------------------------------------------------------

func appendReq(buf []byte, args []string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendUint(buf, uint64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendUint(buf, uint64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, []byte(arg)...)
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

//------------------------------------------------------------------------------

func parseReply(rd reader) (interface{}, error) {
	return _parseReply(rd, false)
}

func parseIfaceSliceReply(rd reader) (interface{}, error) {
	return _parseReply(rd, true)
}

func _parseReply(rd reader, useIfaceSlice bool) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return 0, err
	}

	switch line[0] {
	case '-':
		return nil, errors.New(string(line[1:]))
	case '+':
		return string(line[1:]), nil
	case ':':
		return strconv.ParseInt(string(line[1:]), 10, 64)
	case '$':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return "", Nil
		}

		replyLenInt32, err := strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return "", err
		}
		replyLen := int(replyLenInt32) + 2

		line, err = rd.ReadN(replyLen)
		if err == bufio.ErrBufferFull {
			buf := make([]byte, replyLen)
			r := copy(buf, line)

			for r < replyLen {
				n, err := rd.Read(buf[r:])
				if err != nil {
					return "", err
				}
				r += n
			}

			line = buf
		} else if err != nil {
			return "", err
		}

		return string(line[:len(line)-2]), nil
	case '*':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		if useIfaceSlice {
			numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return nil, err
			}

			vals := make([]interface{}, 0, numReplies)
			for i := int64(0); i < numReplies; i++ {
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
		} else {
			numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return nil, err
			}

			vals := make([]string, 0, numReplies)
			for i := int64(0); i < numReplies; i++ {
				v, err := parseReply(rd)
				if err != nil {
					return nil, err
				} else {
					vals = append(vals, v.(string))
				}
			}

			return vals, nil
		}
	default:
		return nil, fmt.Errorf("redis: can't parse %q", line)
	}
	panic("not reachable")
}
