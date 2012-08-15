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

func AppendReq(buf []byte, args []string) []byte {
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

type ReadLiner interface {
	ReadLine() ([]byte, bool, error)
	Read([]byte) (int, error)
	ReadN(n int) ([]byte, error)
}

func readLine(rd ReadLiner) ([]byte, error) {
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

func ParseReq(rd ReadLiner) ([]string, error) {
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

	args := make([]string, 0)
	for i := int64(0); i < numReplies; i++ {
		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("Expected '$', but got %q", line)
		}

		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		args = append(args, string(line))
	}
	return args, nil
}

//------------------------------------------------------------------------------

func ParseReply(rd ReadLiner) (interface{}, error) {
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

		val := make([]interface{}, 0)
		if len(line) == 2 && line[1] == '0' {
			return val, nil
		}

		numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}

		for i := int64(0); i < numReplies; i++ {
			v, err := ParseReply(rd)
			if err == Nil {
				val = append(val, nil)
			} else if err != nil {
				return nil, err
			} else {
				val = append(val, v)
			}
		}

		return val, nil
	default:
		return nil, fmt.Errorf("redis: can't parse %q", line)
	}
	panic("not reachable")
}
