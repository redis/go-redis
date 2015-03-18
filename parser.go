package redis

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"gopkg.in/bufio.v1"
)

type multiBulkParser func(rd *bufio.Reader, n int64) (interface{}, error)

var (
	errReaderTooSmall = errors.New("redis: reader is too small")
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

		replyLen, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}

		b, err := readN(rd, replyLen+2)
		if err != nil {
			return nil, err
		}
		return string(b[:replyLen]), nil
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
		viface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		v, ok := viface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", viface)
		}
		vals = append(vals, v)
	}
	return vals, nil
}

func parseBoolSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	vals := make([]bool, 0, n)
	for i := int64(0); i < n; i++ {
		viface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		v, ok := viface.(int64)
		if !ok {
			return nil, fmt.Errorf("got %T, expected int64", viface)
		}
		vals = append(vals, v == 1)
	}
	return vals, nil
}

func parseStringStringMap(rd *bufio.Reader, n int64) (interface{}, error) {
	m := make(map[string]string, n/2)
	for i := int64(0); i < n; i += 2 {
		keyiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		key, ok := keyiface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", keyiface)
		}

		valueiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		value, ok := valueiface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", valueiface)
		}

		m[key] = value
	}
	return m, nil
}

func parseStringIntMap(rd *bufio.Reader, n int64) (interface{}, error) {
	m := make(map[string]int64, n/2)
	for i := int64(0); i < n; i += 2 {
		keyiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		key, ok := keyiface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", keyiface)
		}

		valueiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		switch value := valueiface.(type) {
		case int64:
			m[key] = value
		case string:
			m[key], err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("got %v, expected number", value)
			}
		default:
			return nil, fmt.Errorf("got %T, expected number or string", valueiface)

		}
	}
	return m, nil
}

func parseZSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	zz := make([]Z, n/2)
	for i := int64(0); i < n; i += 2 {
		z := &zz[i/2]

		memberiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		member, ok := memberiface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", memberiface)
		}
		z.Member = member

		scoreiface, err := parseReply(rd, nil)
		if err != nil {
			return nil, err
		}
		scorestr, ok := scoreiface.(string)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", scoreiface)
		}
		score, err := strconv.ParseFloat(scorestr, 64)
		if err != nil {
			return nil, err
		}
		z.Score = score
	}
	return zz, nil
}

func parseClusterSlotInfoSlice(rd *bufio.Reader, n int64) (interface{}, error) {
	infos := make([]ClusterSlotInfo, 0, n)
	for i := int64(0); i < n; i++ {
		viface, err := parseReply(rd, parseSlice)
		if err != nil {
			return nil, err
		}

		item, ok := viface.([]interface{})
		if !ok {
			return nil, fmt.Errorf("got %T, expected []interface{}", viface)
		} else if len(item) < 3 {
			return nil, fmt.Errorf("got %v, expected {int64, int64, string...}", item)
		}

		start, ok := item[0].(int64)
		if !ok || start < 0 || start > hashSlots {
			return nil, fmt.Errorf("got %v, expected {int64, int64, string...}", item)
		}
		end, ok := item[1].(int64)
		if !ok || end < 0 || end > hashSlots {
			return nil, fmt.Errorf("got %v, expected {int64, int64, string...}", item)
		}

		info := ClusterSlotInfo{int(start), int(end), make([]string, len(item)-2)}
		for n, ipair := range item[2:] {
			pair, ok := ipair.([]interface{})
			if !ok || len(pair) != 2 {
				return nil, fmt.Errorf("got %v, expected []interface{host, port}", viface)
			}

			ip, ok := pair[0].(string)
			if !ok || len(ip) < 1 {
				return nil, fmt.Errorf("got %v, expected IP PORT pair", pair)
			}
			port, ok := pair[1].(int64)
			if !ok || port < 1 {
				return nil, fmt.Errorf("got %v, expected IP PORT pair", pair)
			}

			info.Addrs[n] = net.JoinHostPort(ip, strconv.FormatInt(port, 10))
		}
		infos = append(infos, info)
	}
	return infos, nil
}
