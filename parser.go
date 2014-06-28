package redis

import (
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/bufio.v1"
)

type multiBulkParser func(rd *bufio.Reader, n int64) (interface{}, error)

var (
	errReaderTooSmall   = errors.New("redis: reader is too small")
	errInvalidReplyType = errors.New("redis: invalid reply type")
)

//------------------------------------------------------------------------------

func appendArgs(buf []byte, args []string) []byte {
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

func readLine(rd *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := rd.ReadLine()
	if err != nil {
		return line, err
	}
	if isPrefix {
		return line, errReaderTooSmall
	}
	return line, nil
}

func readN(rd *bufio.Reader, n int) ([]byte, error) {
	b, err := rd.ReadN(n)
	if err == bufio.ErrBufferFull {
		tmp := make([]byte, n)
		r := copy(tmp, b)
		b = tmp

		for {
			nn, err := rd.Read(b[r:])
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
	return b, nil
}

//------------------------------------------------------------------------------

func parseReq(rd *bufio.Reader) ([]string, error) {
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

func parseReply(rd *bufio.Reader, p multiBulkParser) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case '-':
		return nil, errorf(string(line[1:]))
	case '+':
		return string(line[1:]), nil
	case ':':
		v, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case '$':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		replyLenInt32, err := strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return nil, err
		}
		replyLen := int(replyLenInt32) + 2

		line, err = readN(rd, replyLen)
		if err != nil {
			return nil, err
		}
		return string(line[:len(line)-2]), nil
	case '*':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		repliesNum, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}

		return p(rd, repliesNum)
	}
	return nil, fmt.Errorf("redis: can't parse %q", line)
}

func parseSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	vals := make([]interface{}, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := parseReply(rd, parseSlice)
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

func parseStringSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	vals := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		vi, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		if v, ok := vi.(string); ok {
			vals = append(vals, v)
		} else {
			return nil, errInvalidReplyType
		}
	}
	return vals, nil
}

func parseBoolSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	vals := make([]bool, 0, n)
	for i := int64(0); i < n; i++ {
		vi, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		if v, ok := vi.(int64); ok {
			vals = append(vals, v == 1)
		} else {
			return nil, errInvalidReplyType
		}
	}
	return vals, nil
}

func parseStringStringMap(rd *bufio.Reader, n int64) (interface{}, error) {
	m := make(map[string]string, n/2)
	for i := int64(0); i < n; i += 2 {
		keyI, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		key, ok := keyI.(string)
		if !ok {
			return nil, errInvalidReplyType
		}

		valueI, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		value, ok := valueI.(string)
		if !ok {
			return nil, errInvalidReplyType
		}

		m[key] = value
	}
	return m, nil
}

func parseStringFloatMap(rd *bufio.Reader, n int64) (interface{}, error) {
	m := make(map[string]float64, n/2)
	for i := int64(0); i < n; i += 2 {
		keyI, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		key, ok := keyI.(string)
		if !ok {
			return nil, errInvalidReplyType
		}

		valueI, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		valueS, ok := valueI.(string)
		if !ok {
			return nil, errInvalidReplyType
		}
		value, err := strconv.ParseFloat(valueS, 64)
		if err != nil {
			return nil, err
		}

		m[key] = value
	}
	return m, nil
}
