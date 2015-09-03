package redis

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

type multiBulkParser func(cn *conn, n int64) (interface{}, error)

var (
	errReaderTooSmall = errors.New("redis: reader is too small")
)

//------------------------------------------------------------------------------

// Copy of encoding.BinaryMarshaler.
type binaryMarshaler interface {
	MarshalBinary() (data []byte, err error)
}

// Copy of encoding.BinaryUnmarshaler.
type binaryUnmarshaler interface {
	UnmarshalBinary(data []byte) error
}

func appendString(b []byte, s string) []byte {
	b = append(b, '$')
	b = strconv.AppendUint(b, uint64(len(s)), 10)
	b = append(b, '\r', '\n')
	b = append(b, s...)
	b = append(b, '\r', '\n')
	return b
}

func appendBytes(b, bb []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendUint(b, uint64(len(bb)), 10)
	b = append(b, '\r', '\n')
	b = append(b, bb...)
	b = append(b, '\r', '\n')
	return b
}

func appendArg(b []byte, val interface{}) ([]byte, error) {
	switch v := val.(type) {
	case nil:
		b = appendString(b, "")
	case string:
		b = appendString(b, v)
	case []byte:
		b = appendBytes(b, v)
	case int:
		b = appendString(b, formatInt(int64(v)))
	case int8:
		b = appendString(b, formatInt(int64(v)))
	case int16:
		b = appendString(b, formatInt(int64(v)))
	case int32:
		b = appendString(b, formatInt(int64(v)))
	case int64:
		b = appendString(b, formatInt(v))
	case uint:
		b = appendString(b, formatUint(uint64(v)))
	case uint8:
		b = appendString(b, formatUint(uint64(v)))
	case uint16:
		b = appendString(b, formatUint(uint64(v)))
	case uint32:
		b = appendString(b, formatUint(uint64(v)))
	case uint64:
		b = appendString(b, formatUint(v))
	case float32:
		b = appendString(b, formatFloat(float64(v)))
	case float64:
		b = appendString(b, formatFloat(v))
	case bool:
		if v {
			b = appendString(b, "1")
		} else {
			b = appendString(b, "0")
		}
	default:
		if bm, ok := val.(binaryMarshaler); ok {
			bb, err := bm.MarshalBinary()
			if err != nil {
				return nil, err
			}
			b = appendBytes(b, bb)
		} else {
			err := fmt.Errorf(
				"redis: can't marshal %T (consider implementing BinaryMarshaler)", val)
			return nil, err
		}
	}
	return b, nil
}

func appendArgs(b []byte, args []interface{}) ([]byte, error) {
	b = append(b, '*')
	b = strconv.AppendUint(b, uint64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, arg := range args {
		var err error
		b, err = appendArg(b, arg)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func scan(b []byte, val interface{}) error {
	switch v := val.(type) {
	case nil:
		return errorf("redis: Scan(nil)")
	case *string:
		*v = bytesToString(b)
		return nil
	case *[]byte:
		*v = b
		return nil
	case *int:
		var err error
		*v, err = strconv.Atoi(bytesToString(b))
		return err
	case *int8:
		n, err := strconv.ParseInt(bytesToString(b), 10, 8)
		if err != nil {
			return err
		}
		*v = int8(n)
		return nil
	case *int16:
		n, err := strconv.ParseInt(bytesToString(b), 10, 16)
		if err != nil {
			return err
		}
		*v = int16(n)
		return nil
	case *int32:
		n, err := strconv.ParseInt(bytesToString(b), 10, 16)
		if err != nil {
			return err
		}
		*v = int32(n)
		return nil
	case *int64:
		n, err := strconv.ParseInt(bytesToString(b), 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *uint:
		n, err := strconv.ParseUint(bytesToString(b), 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
		return nil
	case *uint8:
		n, err := strconv.ParseUint(bytesToString(b), 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
		return nil
	case *uint16:
		n, err := strconv.ParseUint(bytesToString(b), 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
		return nil
	case *uint32:
		n, err := strconv.ParseUint(bytesToString(b), 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	case *uint64:
		n, err := strconv.ParseUint(bytesToString(b), 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *float32:
		n, err := strconv.ParseFloat(bytesToString(b), 32)
		if err != nil {
			return err
		}
		*v = float32(n)
		return err
	case *float64:
		var err error
		*v, err = strconv.ParseFloat(bytesToString(b), 64)
		return err
	case *bool:
		*v = len(b) == 1 && b[0] == '1'
		return nil
	default:
		if bu, ok := val.(binaryUnmarshaler); ok {
			return bu.UnmarshalBinary(b)
		}
		err := fmt.Errorf(
			"redis: can't unmarshal %T (consider implementing BinaryUnmarshaler)", val)
		return err
	}
}

//------------------------------------------------------------------------------

func readLine(cn *conn) ([]byte, error) {
	line, isPrefix, err := cn.rd.ReadLine()
	if err != nil {
		return line, err
	}
	if isPrefix {
		return line, errReaderTooSmall
	}
	return line, nil
}

func readN(cn *conn, n int) ([]byte, error) {
	var b []byte
	if cap(cn.buf) < n {
		b = make([]byte, n)
	} else {
		b = cn.buf[:n]
	}
	_, err := io.ReadFull(cn.rd, b)
	return b, err
}

//------------------------------------------------------------------------------

func parseReply(cn *conn, p multiBulkParser) (interface{}, error) {
	line, err := readLine(cn)
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case '-':
		return nil, errorf(string(line[1:]))
	case '+':
		return line[1:], nil
	case ':':
		v, err := strconv.ParseInt(bytesToString(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case '$':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		replyLen, err := strconv.Atoi(bytesToString(line[1:]))
		if err != nil {
			return nil, err
		}

		b, err := readN(cn, replyLen+2)
		if err != nil {
			return nil, err
		}
		return b[:replyLen], nil
	case '*':
		if len(line) == 3 && line[1] == '-' && line[2] == '1' {
			return nil, Nil
		}

		repliesNum, err := strconv.ParseInt(bytesToString(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}

		return p(cn, repliesNum)
	}
	return nil, fmt.Errorf("redis: can't parse %q", line)
}

func parseSlice(cn *conn, n int64) (interface{}, error) {
	vals := make([]interface{}, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := parseReply(cn, parseSlice)
		if err == Nil {
			vals = append(vals, nil)
		} else if err != nil {
			return nil, err
		} else {
			switch vv := v.(type) {
			case []byte:
				vals = append(vals, string(vv))
			default:
				vals = append(vals, v)
			}
		}
	}
	return vals, nil
}

func parseStringSlice(cn *conn, n int64) (interface{}, error) {
	vals := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		viface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		v, ok := viface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", viface)
		}
		vals = append(vals, string(v))
	}
	return vals, nil
}

func parseBoolSlice(cn *conn, n int64) (interface{}, error) {
	vals := make([]bool, 0, n)
	for i := int64(0); i < n; i++ {
		viface, err := parseReply(cn, nil)
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

func parseStringStringMap(cn *conn, n int64) (interface{}, error) {
	m := make(map[string]string, n/2)
	for i := int64(0); i < n; i += 2 {
		keyIface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		keyBytes, ok := keyIface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected []byte", keyIface)
		}
		key := string(keyBytes)

		valueIface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		valueBytes, ok := valueIface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected []byte", valueIface)
		}

		m[key] = string(valueBytes)
	}
	return m, nil
}

func parseStringIntMap(cn *conn, n int64) (interface{}, error) {
	m := make(map[string]int64, n/2)
	for i := int64(0); i < n; i += 2 {
		keyiface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		key, ok := keyiface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", keyiface)
		}

		valueiface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		switch value := valueiface.(type) {
		case int64:
			m[string(key)] = value
		case string:
			m[string(key)], err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("got %v, expected number", value)
			}
		default:
			return nil, fmt.Errorf("got %T, expected number or string", valueiface)
		}
	}
	return m, nil
}

func parseZSlice(cn *conn, n int64) (interface{}, error) {
	zz := make([]Z, n/2)
	for i := int64(0); i < n; i += 2 {
		z := &zz[i/2]

		memberiface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		member, ok := memberiface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", memberiface)
		}
		z.Member = string(member)

		scoreiface, err := parseReply(cn, nil)
		if err != nil {
			return nil, err
		}
		scoreb, ok := scoreiface.([]byte)
		if !ok {
			return nil, fmt.Errorf("got %T, expected string", scoreiface)
		}
		score, err := strconv.ParseFloat(bytesToString(scoreb), 64)
		if err != nil {
			return nil, err
		}
		z.Score = score
	}
	return zz, nil
}

func parseClusterSlotInfoSlice(cn *conn, n int64) (interface{}, error) {
	infos := make([]ClusterSlotInfo, 0, n)
	for i := int64(0); i < n; i++ {
		viface, err := parseReply(cn, parseSlice)
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
