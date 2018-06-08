package redis

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/internal/proto"
)

// Implements proto.MultiBulkParse
func sliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	vals := make([]interface{}, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := rd.ReadReply(sliceParser)
		if err != nil {
			if err == Nil {
				vals = append(vals, nil)
				continue
			}
			if err, ok := err.(proto.RedisError); ok {
				vals = append(vals, err)
				continue
			}
			return nil, err
		}

		switch v := v.(type) {
		case []byte:
			vals = append(vals, string(v))
		default:
			vals = append(vals, v)
		}
	}
	return vals, nil
}

// Implements proto.MultiBulkParse
func boolSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	bools := make([]bool, 0, n)
	for i := int64(0); i < n; i++ {
		n, err := rd.ReadIntReply()
		if err != nil {
			return nil, err
		}
		bools = append(bools, n == 1)
	}
	return bools, nil
}

// Implements proto.MultiBulkParse
func stringSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	ss := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		s, err := rd.ReadStringReply()
		if err == Nil {
			ss = append(ss, "")
		} else if err != nil {
			return nil, err
		} else {
			ss = append(ss, s)
		}
	}
	return ss, nil
}

// Implements proto.MultiBulkParse
func stringStringMapParser(rd *proto.Reader, n int64) (interface{}, error) {
	m := make(map[string]string, n/2)
	for i := int64(0); i < n; i += 2 {
		key, err := rd.ReadStringReply()
		if err != nil {
			return nil, err
		}

		value, err := rd.ReadStringReply()
		if err != nil {
			return nil, err
		}

		m[key] = value
	}
	return m, nil
}

func streamEntrySliceMapParser(rd *proto.Reader, n int64) (interface{}, error) {
	ret := map[string][]StreamEntry{}
	for i := int64(0); i < n; i++ {
		entry, err := rd.ReadArrayReply(streamEntrySliceWithKeyParser)
		if err != nil {
			return nil, err
		}
		for k, v := range entry.(map[string][]StreamEntry) {
			ret[k] = v
		}
	}
	return ret, nil
}

func streamEntrySliceWithKeyParser(rd *proto.Reader, n int64) (interface{}, error) {
	var key string
	key, err := rd.ReadStringReply()
	if err != nil {
		return nil, err
	}
	entry_arr, err := rd.ReadArrayReply(streamEntrySliceParser)
	if err != nil {
		return nil, err
	}
	return map[string][]StreamEntry{
		key: entry_arr.([]StreamEntry),
	}, nil
}

func streamEntrySliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	var ret []StreamEntry
	for i := int64(0); i < n; i++ {
		entry, err := rd.ReadArrayReply(streamEntryParser)
		if err != nil {
			return nil, err
		}
		ret = append(ret, entry.(StreamEntry))
	}
	return ret, nil
}

func streamEntryParser(rd *proto.Reader, n int64) (interface{}, error) {
	si, err := rd.ReadTmpBytesReply()
	if err != nil {
		return nil, err
	}
	m, err := rd.ReadArrayReply(stringStringMapParser)
	if err != nil {
		return nil, err
	}
	return StreamEntry{
		Id:     string(si),
		Fields: m.(map[string]string),
	}, nil
}

func xPendingInfoParser(rd *proto.Reader, n int64) (interface{}, error) {
	count, err := rd.ReadIntReply()
	if err != nil {
		return nil, err
	}
	lower, err := rd.ReadStringReply()
	if err != nil && err != Nil {
		return nil, err
	}
	higher, err := rd.ReadStringReply()
	if err != nil && err != Nil {
		return nil, err
	}
	ret := XPendingInfo{
		Count:  count,
		Lower:  lower,
		Higher: higher,
	}
	info, err := rd.ReadArrayReply(func(rd *proto.Reader, n int64) (interface{}, error) {
		ret := make(map[string]string)
		for i := int64(0); i < n; i++ {
			info, err := rd.ReadArrayReply(stringStringMapParser)
			if err != nil {
				return nil, err
			}
			for k, v := range info.(map[string]string) {
				ret[k] = v
			}
		}
		return ret, nil
	})
	if err != nil && err != Nil {
		return nil, err
	} else if err == nil {
		ret.ConsumerInfo = info.(map[string]string)
	}
	return ret, nil
}

func xPendingConsumerInfoParser(rd *proto.Reader, n int64) (interface{}, error) {
	id, err := rd.ReadStringReply()
	if err != nil {
		return nil, err
	}
	consumer, err := rd.ReadStringReply()
	if err != nil && err != Nil {
		return nil, err
	}
	idle, err := rd.ReadIntReply()
	if err != nil && err != Nil {
		return nil, err
	}
	retry_count, err := rd.ReadIntReply()
	if err != nil && err != Nil {
		return nil, err
	}
	return XPendingConsumerInfo{
		Id:         id,
		Consumer:   consumer,
		Idle:       idle,
		RetryCount: retry_count,
	}, nil
}

func xPendingConsumerInfoSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	var ret []XPendingConsumerInfo
	for i := int64(0); i < n; i++ {
		entry, err := rd.ReadArrayReply(xPendingConsumerInfoParser)
		if err != nil {
			return nil, err
		}
		ret = append(ret, entry.(XPendingConsumerInfo))
	}
	return ret, nil
}

// Implements proto.MultiBulkParse
func stringIntMapParser(rd *proto.Reader, n int64) (interface{}, error) {
	m := make(map[string]int64, n/2)
	for i := int64(0); i < n; i += 2 {
		key, err := rd.ReadStringReply()
		if err != nil {
			return nil, err
		}

		n, err := rd.ReadIntReply()
		if err != nil {
			return nil, err
		}

		m[key] = n
	}
	return m, nil
}

// Implements proto.MultiBulkParse
func stringStructMapParser(rd *proto.Reader, n int64) (interface{}, error) {
	m := make(map[string]struct{}, n)
	for i := int64(0); i < n; i++ {
		key, err := rd.ReadStringReply()
		if err != nil {
			return nil, err
		}

		m[key] = struct{}{}
	}
	return m, nil
}

// Implements proto.MultiBulkParse
func zSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	zz := make([]Z, n/2)
	for i := int64(0); i < n; i += 2 {
		var err error

		z := &zz[i/2]

		z.Member, err = rd.ReadStringReply()
		if err != nil {
			return nil, err
		}

		z.Score, err = rd.ReadFloatReply()
		if err != nil {
			return nil, err
		}
	}
	return zz, nil
}

// Implements proto.MultiBulkParse
func clusterSlotsParser(rd *proto.Reader, n int64) (interface{}, error) {
	slots := make([]ClusterSlot, n)
	for i := 0; i < len(slots); i++ {
		n, err := rd.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		if n < 2 {
			err := fmt.Errorf("redis: got %d elements in cluster info, expected at least 2", n)
			return nil, err
		}

		start, err := rd.ReadIntReply()
		if err != nil {
			return nil, err
		}

		end, err := rd.ReadIntReply()
		if err != nil {
			return nil, err
		}

		nodes := make([]ClusterNode, n-2)
		for j := 0; j < len(nodes); j++ {
			n, err := rd.ReadArrayLen()
			if err != nil {
				return nil, err
			}
			if n != 2 && n != 3 {
				err := fmt.Errorf("got %d elements in cluster info address, expected 2 or 3", n)
				return nil, err
			}

			ip, err := rd.ReadStringReply()
			if err != nil {
				return nil, err
			}

			port, err := rd.ReadIntReply()
			if err != nil {
				return nil, err
			}
			nodes[j].Addr = net.JoinHostPort(ip, strconv.FormatInt(port, 10))

			if n == 3 {
				id, err := rd.ReadStringReply()
				if err != nil {
					return nil, err
				}
				nodes[j].Id = id
			}
		}

		slots[i] = ClusterSlot{
			Start: int(start),
			End:   int(end),
			Nodes: nodes,
		}
	}
	return slots, nil
}

func newGeoLocationParser(q *GeoRadiusQuery) proto.MultiBulkParse {
	return func(rd *proto.Reader, n int64) (interface{}, error) {
		var loc GeoLocation
		var err error

		loc.Name, err = rd.ReadStringReply()
		if err != nil {
			return nil, err
		}
		if q.WithDist {
			loc.Dist, err = rd.ReadFloatReply()
			if err != nil {
				return nil, err
			}
		}
		if q.WithGeoHash {
			loc.GeoHash, err = rd.ReadIntReply()
			if err != nil {
				return nil, err
			}
		}
		if q.WithCoord {
			n, err := rd.ReadArrayLen()
			if err != nil {
				return nil, err
			}
			if n != 2 {
				return nil, fmt.Errorf("got %d coordinates, expected 2", n)
			}

			loc.Longitude, err = rd.ReadFloatReply()
			if err != nil {
				return nil, err
			}
			loc.Latitude, err = rd.ReadFloatReply()
			if err != nil {
				return nil, err
			}
		}

		return &loc, nil
	}
}

func newGeoLocationSliceParser(q *GeoRadiusQuery) proto.MultiBulkParse {
	return func(rd *proto.Reader, n int64) (interface{}, error) {
		locs := make([]GeoLocation, 0, n)
		for i := int64(0); i < n; i++ {
			v, err := rd.ReadReply(newGeoLocationParser(q))
			if err != nil {
				return nil, err
			}
			switch vv := v.(type) {
			case []byte:
				locs = append(locs, GeoLocation{
					Name: string(vv),
				})
			case *GeoLocation:
				locs = append(locs, *vv)
			default:
				return nil, fmt.Errorf("got %T, expected string or *GeoLocation", v)
			}
		}
		return locs, nil
	}
}

func geoPosParser(rd *proto.Reader, n int64) (interface{}, error) {
	var pos GeoPos
	var err error

	pos.Longitude, err = rd.ReadFloatReply()
	if err != nil {
		return nil, err
	}

	pos.Latitude, err = rd.ReadFloatReply()
	if err != nil {
		return nil, err
	}

	return &pos, nil
}

func geoPosSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	positions := make([]*GeoPos, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := rd.ReadReply(geoPosParser)
		if err != nil {
			if err == Nil {
				positions = append(positions, nil)
				continue
			}
			return nil, err
		}
		switch v := v.(type) {
		case *GeoPos:
			positions = append(positions, v)
		default:
			return nil, fmt.Errorf("got %T, expected *GeoPos", v)
		}
	}
	return positions, nil
}

func commandInfoParser(rd *proto.Reader, n int64) (interface{}, error) {
	var cmd CommandInfo
	var err error

	if n != 6 {
		return nil, fmt.Errorf("redis: got %d elements in COMMAND reply, wanted 6", n)
	}

	cmd.Name, err = rd.ReadStringReply()
	if err != nil {
		return nil, err
	}

	arity, err := rd.ReadIntReply()
	if err != nil {
		return nil, err
	}
	cmd.Arity = int8(arity)

	flags, err := rd.ReadReply(stringSliceParser)
	if err != nil {
		return nil, err
	}
	cmd.Flags = flags.([]string)

	firstKeyPos, err := rd.ReadIntReply()
	if err != nil {
		return nil, err
	}
	cmd.FirstKeyPos = int8(firstKeyPos)

	lastKeyPos, err := rd.ReadIntReply()
	if err != nil {
		return nil, err
	}
	cmd.LastKeyPos = int8(lastKeyPos)

	stepCount, err := rd.ReadIntReply()
	if err != nil {
		return nil, err
	}
	cmd.StepCount = int8(stepCount)

	for _, flag := range cmd.Flags {
		if flag == "readonly" {
			cmd.ReadOnly = true
			break
		}
	}

	return &cmd, nil
}

// Implements proto.MultiBulkParse
func commandInfoSliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	m := make(map[string]*CommandInfo, n)
	for i := int64(0); i < n; i++ {
		v, err := rd.ReadReply(commandInfoParser)
		if err != nil {
			return nil, err
		}
		vv := v.(*CommandInfo)
		m[vv.Name] = vv

	}
	return m, nil
}

// Implements proto.MultiBulkParse
func timeParser(rd *proto.Reader, n int64) (interface{}, error) {
	if n != 2 {
		return nil, fmt.Errorf("got %d elements, expected 2", n)
	}

	sec, err := rd.ReadInt()
	if err != nil {
		return nil, err
	}

	microsec, err := rd.ReadInt()
	if err != nil {
		return nil, err
	}

	return time.Unix(sec, microsec*1000), nil
}
