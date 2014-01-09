package redis

import (
	"strconv"
	"time"
)

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func readTimeout(sec int64) time.Duration {
	if sec == 0 {
		return 0
	}
	return time.Duration(sec+1) * time.Second
}

//------------------------------------------------------------------------------

func (c *Client) Auth(password string) *StatusCmd {
	req := NewStatusCmd("AUTH", password)
	c.Process(req)
	return req
}

func (c *Client) Echo(message string) *StringCmd {
	req := NewStringCmd("ECHO", message)
	c.Process(req)
	return req
}

func (c *Client) Ping() *StatusCmd {
	req := NewStatusCmd("PING")
	c.Process(req)
	return req
}

func (c *Client) Quit() *StatusCmd {
	panic("not implemented")
}

func (c *Client) Select(index int64) *StatusCmd {
	req := NewStatusCmd("SELECT", strconv.FormatInt(index, 10))
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Del(keys ...string) *IntCmd {
	args := append([]string{"DEL"}, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) Dump(key string) *StringCmd {
	req := NewStringCmd("DUMP", key)
	c.Process(req)
	return req
}

func (c *Client) Exists(key string) *BoolCmd {
	req := NewBoolCmd("EXISTS", key)
	c.Process(req)
	return req
}

func (c *Client) Expire(key string, dur time.Duration) *BoolCmd {
	req := NewBoolCmd("EXPIRE", key, strconv.FormatInt(int64(dur/time.Second), 10))
	c.Process(req)
	return req
}

func (c *Client) ExpireAt(key string, tm time.Time) *BoolCmd {
	req := NewBoolCmd("EXPIREAT", key, strconv.FormatInt(tm.Unix(), 10))
	c.Process(req)
	return req
}

func (c *Client) Keys(pattern string) *StringSliceCmd {
	req := NewStringSliceCmd("KEYS", pattern)
	c.Process(req)
	return req
}

func (c *Client) Migrate(host, port, key string, db, timeout int64) *StatusCmd {
	req := NewStatusCmd(
		"MIGRATE",
		host,
		port,
		key,
		strconv.FormatInt(db, 10),
		strconv.FormatInt(timeout, 10),
	)
	req.setReadTimeout(readTimeout(timeout))
	c.Process(req)
	return req
}

func (c *Client) Move(key string, db int64) *BoolCmd {
	req := NewBoolCmd("MOVE", key, strconv.FormatInt(db, 10))
	c.Process(req)
	return req
}

func (c *Client) ObjectRefCount(keys ...string) *IntCmd {
	args := append([]string{"OBJECT", "REFCOUNT"}, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ObjectEncoding(keys ...string) *StringCmd {
	args := append([]string{"OBJECT", "ENCODING"}, keys...)
	req := NewStringCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ObjectIdleTime(keys ...string) *DurationCmd {
	args := append([]string{"OBJECT", "IDLETIME"}, keys...)
	req := NewDurationCmd(time.Second, args...)
	c.Process(req)
	return req
}

func (c *Client) Persist(key string) *BoolCmd {
	req := NewBoolCmd("PERSIST", key)
	c.Process(req)
	return req
}

func (c *Client) PExpire(key string, dur time.Duration) *BoolCmd {
	req := NewBoolCmd("PEXPIRE", key, strconv.FormatInt(int64(dur/time.Millisecond), 10))
	c.Process(req)
	return req
}

func (c *Client) PExpireAt(key string, tm time.Time) *BoolCmd {
	req := NewBoolCmd(
		"PEXPIREAT",
		key,
		strconv.FormatInt(tm.UnixNano()/int64(time.Millisecond), 10),
	)
	c.Process(req)
	return req
}

func (c *Client) PTTL(key string) *DurationCmd {
	req := NewDurationCmd(time.Millisecond, "PTTL", key)
	c.Process(req)
	return req
}

func (c *Client) RandomKey() *StringCmd {
	req := NewStringCmd("RANDOMKEY")
	c.Process(req)
	return req
}

func (c *Client) Rename(key, newkey string) *StatusCmd {
	req := NewStatusCmd("RENAME", key, newkey)
	c.Process(req)
	return req
}

func (c *Client) RenameNX(key, newkey string) *BoolCmd {
	req := NewBoolCmd("RENAMENX", key, newkey)
	c.Process(req)
	return req
}

func (c *Client) Restore(key string, ttl int64, value string) *StatusCmd {
	req := NewStatusCmd(
		"RESTORE",
		key,
		strconv.FormatInt(ttl, 10),
		value,
	)
	c.Process(req)
	return req
}

type Sort struct {
	By            string
	Offset, Count float64
	Get           []string
	Order         string
	IsAlpha       bool
	Store         string
}

func (c *Client) Sort(key string, sort Sort) *StringSliceCmd {
	args := []string{"SORT", key}
	if sort.By != "" {
		args = append(args, sort.By)
	}
	if sort.Offset != 0 || sort.Count != 0 {
		args = append(args, "LIMIT", formatFloat(sort.Offset), formatFloat(sort.Count))
	}
	for _, get := range sort.Get {
		args = append(args, "GET", get)
	}
	if sort.Order != "" {
		args = append(args, sort.Order)
	}
	if sort.IsAlpha {
		args = append(args, "ALPHA")
	}
	if sort.Store != "" {
		args = append(args, "STORE", sort.Store)
	}
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) TTL(key string) *DurationCmd {
	req := NewDurationCmd(time.Second, "TTL", key)
	c.Process(req)
	return req
}

func (c *Client) Type(key string) *StatusCmd {
	req := NewStatusCmd("TYPE", key)
	c.Process(req)
	return req
}

func (c *Client) Scan(cursor int64, match string, count int64) *ScanCmd {
	args := []string{"SCAN", strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	req := NewScanCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"SSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	req := NewScanCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) HScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"HSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	req := NewScanCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZScan(key string, cursor int64, match string, count int64) *ScanCmd {
	args := []string{"ZSCAN", key, strconv.FormatInt(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	req := NewScanCmd(args...)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Append(key, value string) *IntCmd {
	req := NewIntCmd("APPEND", key, value)
	c.Process(req)
	return req
}

type BitCount struct {
	Start, End int64
}

func (c *Client) BitCount(key string, bitCount *BitCount) *IntCmd {
	args := []string{"BITCOUNT", key}
	if bitCount != nil {
		args = append(
			args,
			strconv.FormatInt(bitCount.Start, 10),
			strconv.FormatInt(bitCount.End, 10),
		)
	}
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) bitOp(op, destKey string, keys ...string) *IntCmd {
	args := []string{"BITOP", op, destKey}
	args = append(args, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) BitOpAnd(destKey string, keys ...string) *IntCmd {
	return c.bitOp("AND", destKey, keys...)
}

func (c *Client) BitOpOr(destKey string, keys ...string) *IntCmd {
	return c.bitOp("OR", destKey, keys...)
}

func (c *Client) BitOpXor(destKey string, keys ...string) *IntCmd {
	return c.bitOp("XOR", destKey, keys...)
}

func (c *Client) BitOpNot(destKey string, key string) *IntCmd {
	return c.bitOp("NOT", destKey, key)
}

func (c *Client) Decr(key string) *IntCmd {
	req := NewIntCmd("DECR", key)
	c.Process(req)
	return req
}

func (c *Client) DecrBy(key string, decrement int64) *IntCmd {
	req := NewIntCmd("DECRBY", key, strconv.FormatInt(decrement, 10))
	c.Process(req)
	return req
}

func (c *Client) Get(key string) *StringCmd {
	req := NewStringCmd("GET", key)
	c.Process(req)
	return req
}

func (c *Client) GetBit(key string, offset int64) *IntCmd {
	req := NewIntCmd("GETBIT", key, strconv.FormatInt(offset, 10))
	c.Process(req)
	return req
}

func (c *Client) GetRange(key string, start, end int64) *StringCmd {
	req := NewStringCmd(
		"GETRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(end, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) GetSet(key, value string) *StringCmd {
	req := NewStringCmd("GETSET", key, value)
	c.Process(req)
	return req
}

func (c *Client) Incr(key string) *IntCmd {
	req := NewIntCmd("INCR", key)
	c.Process(req)
	return req
}

func (c *Client) IncrBy(key string, value int64) *IntCmd {
	req := NewIntCmd("INCRBY", key, strconv.FormatInt(value, 10))
	c.Process(req)
	return req
}

func (c *Client) IncrByFloat(key string, value float64) *FloatCmd {
	req := NewFloatCmd("INCRBYFLOAT", key, formatFloat(value))
	c.Process(req)
	return req
}

func (c *Client) MGet(keys ...string) *SliceCmd {
	args := append([]string{"MGET"}, keys...)
	req := NewSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) MSet(pairs ...string) *StatusCmd {
	args := append([]string{"MSET"}, pairs...)
	req := NewStatusCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) MSetNX(pairs ...string) *BoolCmd {
	args := append([]string{"MSETNX"}, pairs...)
	req := NewBoolCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) PSetEx(key string, dur time.Duration, value string) *StatusCmd {
	req := NewStatusCmd(
		"PSETEX",
		key,
		strconv.FormatInt(int64(dur/time.Millisecond), 10),
		value,
	)
	c.Process(req)
	return req
}

func (c *Client) Set(key, value string) *StatusCmd {
	req := NewStatusCmd("SET", key, value)
	c.Process(req)
	return req
}

func (c *Client) SetBit(key string, offset int64, value int) *IntCmd {
	req := NewIntCmd(
		"SETBIT",
		key,
		strconv.FormatInt(offset, 10),
		strconv.FormatInt(int64(value), 10),
	)
	c.Process(req)
	return req
}

func (c *Client) SetEx(key string, dur time.Duration, value string) *StatusCmd {
	req := NewStatusCmd("SETEX", key, strconv.FormatInt(int64(dur/time.Second), 10), value)
	c.Process(req)
	return req
}

func (c *Client) SetNX(key, value string) *BoolCmd {
	req := NewBoolCmd("SETNX", key, value)
	c.Process(req)
	return req
}

func (c *Client) SetRange(key string, offset int64, value string) *IntCmd {
	req := NewIntCmd("SETRANGE", key, strconv.FormatInt(offset, 10), value)
	c.Process(req)
	return req
}

func (c *Client) StrLen(key string) *IntCmd {
	req := NewIntCmd("STRLEN", key)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) HDel(key string, fields ...string) *IntCmd {
	args := append([]string{"HDEL", key}, fields...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) HExists(key, field string) *BoolCmd {
	req := NewBoolCmd("HEXISTS", key, field)
	c.Process(req)
	return req
}

func (c *Client) HGet(key, field string) *StringCmd {
	req := NewStringCmd("HGET", key, field)
	c.Process(req)
	return req
}

func (c *Client) HGetAll(key string) *StringSliceCmd {
	req := NewStringSliceCmd("HGETALL", key)
	c.Process(req)
	return req
}

func (c *Client) HGetAllMap(key string) *StringStringMapCmd {
	req := NewStringStringMapCmd("HGETALL", key)
	c.Process(req)
	return req
}

func (c *Client) HIncrBy(key, field string, incr int64) *IntCmd {
	req := NewIntCmd("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Process(req)
	return req
}

func (c *Client) HIncrByFloat(key, field string, incr float64) *FloatCmd {
	req := NewFloatCmd("HINCRBYFLOAT", key, field, formatFloat(incr))
	c.Process(req)
	return req
}

func (c *Client) HKeys(key string) *StringSliceCmd {
	req := NewStringSliceCmd("HKEYS", key)
	c.Process(req)
	return req
}

func (c *Client) HLen(key string) *IntCmd {
	req := NewIntCmd("HLEN", key)
	c.Process(req)
	return req
}

func (c *Client) HMGet(key string, fields ...string) *SliceCmd {
	args := append([]string{"HMGET", key}, fields...)
	req := NewSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) HMSet(key, field, value string, pairs ...string) *StatusCmd {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	req := NewStatusCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) HSet(key, field, value string) *BoolCmd {
	req := NewBoolCmd("HSET", key, field, value)
	c.Process(req)
	return req
}

func (c *Client) HSetNX(key, field, value string) *BoolCmd {
	req := NewBoolCmd("HSETNX", key, field, value)
	c.Process(req)
	return req
}

func (c *Client) HVals(key string) *StringSliceCmd {
	req := NewStringSliceCmd("HVALS", key)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BLPop(timeout int64, keys ...string) *StringSliceCmd {
	args := append([]string{"BLPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewStringSliceCmd(args...)
	req.setReadTimeout(readTimeout(timeout))
	c.Process(req)
	return req
}

func (c *Client) BRPop(timeout int64, keys ...string) *StringSliceCmd {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewStringSliceCmd(args...)
	req.setReadTimeout(readTimeout(timeout))
	c.Process(req)
	return req
}

func (c *Client) BRPopLPush(source, destination string, timeout int64) *StringCmd {
	req := NewStringCmd(
		"BRPOPLPUSH",
		source,
		destination,
		strconv.FormatInt(timeout, 10),
	)
	req.setReadTimeout(readTimeout(timeout))
	c.Process(req)
	return req
}

func (c *Client) LIndex(key string, index int64) *StringCmd {
	req := NewStringCmd("LINDEX", key, strconv.FormatInt(index, 10))
	c.Process(req)
	return req
}

func (c *Client) LInsert(key, op, pivot, value string) *IntCmd {
	req := NewIntCmd("LINSERT", key, op, pivot, value)
	c.Process(req)
	return req
}

func (c *Client) LLen(key string) *IntCmd {
	req := NewIntCmd("LLEN", key)
	c.Process(req)
	return req
}

func (c *Client) LPop(key string) *StringCmd {
	req := NewStringCmd("LPOP", key)
	c.Process(req)
	return req
}

func (c *Client) LPush(key string, values ...string) *IntCmd {
	args := append([]string{"LPUSH", key}, values...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) LPushX(key, value string) *IntCmd {
	req := NewIntCmd("LPUSHX", key, value)
	c.Process(req)
	return req
}

func (c *Client) LRange(key string, start, stop int64) *StringSliceCmd {
	req := NewStringSliceCmd(
		"LRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) LRem(key string, count int64, value string) *IntCmd {
	req := NewIntCmd("LREM", key, strconv.FormatInt(count, 10), value)
	c.Process(req)
	return req
}

func (c *Client) LSet(key string, index int64, value string) *StatusCmd {
	req := NewStatusCmd("LSET", key, strconv.FormatInt(index, 10), value)
	c.Process(req)
	return req
}

func (c *Client) LTrim(key string, start, stop int64) *StatusCmd {
	req := NewStatusCmd(
		"LTRIM",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) RPop(key string) *StringCmd {
	req := NewStringCmd("RPOP", key)
	c.Process(req)
	return req
}

func (c *Client) RPopLPush(source, destination string) *StringCmd {
	req := NewStringCmd("RPOPLPUSH", source, destination)
	c.Process(req)
	return req
}

func (c *Client) RPush(key string, values ...string) *IntCmd {
	args := append([]string{"RPUSH", key}, values...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) RPushX(key string, value string) *IntCmd {
	req := NewIntCmd("RPUSHX", key, value)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) SAdd(key string, members ...string) *IntCmd {
	args := append([]string{"SADD", key}, members...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SCard(key string) *IntCmd {
	req := NewIntCmd("SCARD", key)
	c.Process(req)
	return req
}

func (c *Client) SDiff(keys ...string) *StringSliceCmd {
	args := append([]string{"SDIFF"}, keys...)
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SDiffStore(destination string, keys ...string) *IntCmd {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SInter(keys ...string) *StringSliceCmd {
	args := append([]string{"SINTER"}, keys...)
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SInterStore(destination string, keys ...string) *IntCmd {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SIsMember(key, member string) *BoolCmd {
	req := NewBoolCmd("SISMEMBER", key, member)
	c.Process(req)
	return req
}

func (c *Client) SMembers(key string) *StringSliceCmd {
	req := NewStringSliceCmd("SMEMBERS", key)
	c.Process(req)
	return req
}

func (c *Client) SMove(source, destination, member string) *BoolCmd {
	req := NewBoolCmd("SMOVE", source, destination, member)
	c.Process(req)
	return req
}

func (c *Client) SPop(key string) *StringCmd {
	req := NewStringCmd("SPOP", key)
	c.Process(req)
	return req
}

func (c *Client) SRandMember(key string) *StringCmd {
	req := NewStringCmd("SRANDMEMBER", key)
	c.Process(req)
	return req
}

func (c *Client) SRem(key string, members ...string) *IntCmd {
	args := append([]string{"SREM", key}, members...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SUnion(keys ...string) *StringSliceCmd {
	args := append([]string{"SUNION"}, keys...)
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) SUnionStore(destination string, keys ...string) *IntCmd {
	args := append([]string{"SUNIONSTORE", destination}, keys...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

type Z struct {
	Score  float64
	Member string
}

type ZStore struct {
	Weights   []int64
	Aggregate string
}

func (c *Client) ZAdd(key string, members ...Z) *IntCmd {
	args := []string{"ZADD", key}
	for _, m := range members {
		args = append(args, formatFloat(m.Score), m.Member)
	}
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZCard(key string) *IntCmd {
	req := NewIntCmd("ZCARD", key)
	c.Process(req)
	return req
}

func (c *Client) ZCount(key, min, max string) *IntCmd {
	req := NewIntCmd("ZCOUNT", key, min, max)
	c.Process(req)
	return req
}

func (c *Client) ZIncrBy(key string, increment float64, member string) *FloatCmd {
	req := NewFloatCmd("ZINCRBY", key, formatFloat(increment), member)
	c.Process(req)
	return req
}

func (c *Client) ZInterStore(
	destination string,
	store ZStore,
	keys ...string,
) *IntCmd {
	args := []string{"ZINTERSTORE", destination, strconv.FormatInt(int64(len(keys)), 10)}
	args = append(args, keys...)
	if len(store.Weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range store.Weights {
			args = append(args, strconv.FormatInt(weight, 10))
		}
	}
	if store.Aggregate != "" {
		args = append(args, "AGGREGATE", store.Aggregate)
	}
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) zRange(key string, start, stop int64, withScores bool) *StringSliceCmd {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRange(key string, start, stop int64) *StringSliceCmd {
	return c.zRange(key, start, stop, false)
}

func (c *Client) ZRangeWithScores(key string, start, stop int64) *StringSliceCmd {
	return c.zRange(key, start, stop, true)
}

func (c *Client) ZRangeWithScoresMap(key string, start, stop int64) *StringFloatMapCmd {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
		"WITHSCORES",
	}
	req := NewStringFloatMapCmd(args...)
	c.Process(req)
	return req
}

type ZRangeByScore struct {
	Min, Max string

	Offset, Count int64
}

func (c *Client) zRangeByScore(key string, opt ZRangeByScore, withScores bool) *StringSliceCmd {
	args := []string{"ZRANGEBYSCORE", key, opt.Min, opt.Max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRangeByScore(key string, opt ZRangeByScore) *StringSliceCmd {
	return c.zRangeByScore(key, opt, false)
}

func (c *Client) ZRangeByScoreWithScores(key string, opt ZRangeByScore) *StringSliceCmd {
	return c.zRangeByScore(key, opt, true)
}

func (c *Client) ZRangeByScoreWithScoresMap(key string, opt ZRangeByScore) *StringFloatMapCmd {
	args := []string{"ZRANGEBYSCORE", key, opt.Min, opt.Max, "WITHSCORES"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	req := NewStringFloatMapCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRank(key, member string) *IntCmd {
	req := NewIntCmd("ZRANK", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZRem(key string, members ...string) *IntCmd {
	args := append([]string{"ZREM", key}, members...)
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRemRangeByRank(key string, start, stop int64) *IntCmd {
	req := NewIntCmd(
		"ZREMRANGEBYRANK",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) ZRemRangeByScore(key, min, max string) *IntCmd {
	req := NewIntCmd("ZREMRANGEBYSCORE", key, min, max)
	c.Process(req)
	return req
}

func (c *Client) zRevRange(key, start, stop string, withScores bool) *StringSliceCmd {
	args := []string{"ZREVRANGE", key, start, stop}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRevRange(key, start, stop string) *StringSliceCmd {
	return c.zRevRange(key, start, stop, false)
}

func (c *Client) ZRevRangeWithScores(key, start, stop string) *StringSliceCmd {
	return c.zRevRange(key, start, stop, true)
}

func (c *Client) ZRevRangeWithScoresMap(key, start, stop string) *StringFloatMapCmd {
	args := []string{"ZREVRANGE", key, start, stop, "WITHSCORES"}
	req := NewStringFloatMapCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) zRevRangeByScore(key string, opt ZRangeByScore, withScores bool) *StringSliceCmd {
	args := []string{"ZREVRANGEBYSCORE", key, opt.Max, opt.Min}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	req := NewStringSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRevRangeByScore(key string, opt ZRangeByScore) *StringSliceCmd {
	return c.zRevRangeByScore(key, opt, false)
}

func (c *Client) ZRevRangeByScoreWithScores(key string, opt ZRangeByScore) *StringSliceCmd {
	return c.zRevRangeByScore(key, opt, true)
}

func (c *Client) ZRevRangeByScoreWithScoresMap(key string, opt ZRangeByScore) *StringFloatMapCmd {
	args := []string{"ZREVRANGEBYSCORE", key, opt.Max, opt.Min, "WITHSCORES"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(opt.Offset, 10),
			strconv.FormatInt(opt.Count, 10),
		)
	}
	req := NewStringFloatMapCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRevRank(key, member string) *IntCmd {
	req := NewIntCmd("ZREVRANK", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZScore(key, member string) *FloatCmd {
	req := NewFloatCmd("ZSCORE", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZUnionStore(
	destination string,
	store ZStore,
	keys ...string,
) *IntCmd {
	args := []string{"ZUNIONSTORE", destination, strconv.FormatInt(int64(len(keys)), 10)}
	args = append(args, keys...)
	if len(store.Weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range store.Weights {
			args = append(args, strconv.FormatInt(weight, 10))
		}
	}
	if store.Aggregate != "" {
		args = append(args, "AGGREGATE", store.Aggregate)
	}
	req := NewIntCmd(args...)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BgRewriteAOF() *StatusCmd {
	req := NewStatusCmd("BGREWRITEAOF")
	c.Process(req)
	return req
}

func (c *Client) BgSave() *StatusCmd {
	req := NewStatusCmd("BGSAVE")
	c.Process(req)
	return req
}

func (c *Client) ClientKill(ipPort string) *StatusCmd {
	req := NewStatusCmd("CLIENT", "KILL", ipPort)
	c.Process(req)
	return req
}

func (c *Client) ClientList() *StringCmd {
	req := NewStringCmd("CLIENT", "LIST")
	c.Process(req)
	return req
}

func (c *Client) ConfigGet(parameter string) *SliceCmd {
	req := NewSliceCmd("CONFIG", "GET", parameter)
	c.Process(req)
	return req
}

func (c *Client) ConfigResetStat() *StatusCmd {
	req := NewStatusCmd("CONFIG", "RESETSTAT")
	c.Process(req)
	return req
}

func (c *Client) ConfigSet(parameter, value string) *StatusCmd {
	req := NewStatusCmd("CONFIG", "SET", parameter, value)
	c.Process(req)
	return req
}

func (c *Client) DbSize() *IntCmd {
	req := NewIntCmd("DBSIZE")
	c.Process(req)
	return req
}

func (c *Client) FlushAll() *StatusCmd {
	req := NewStatusCmd("FLUSHALL")
	c.Process(req)
	return req
}

func (c *Client) FlushDb() *StatusCmd {
	req := NewStatusCmd("FLUSHDB")
	c.Process(req)
	return req
}

func (c *Client) Info() *StringCmd {
	req := NewStringCmd("INFO")
	c.Process(req)
	return req
}

func (c *Client) LastSave() *IntCmd {
	req := NewIntCmd("LASTSAVE")
	c.Process(req)
	return req
}

func (c *Client) Save() *StatusCmd {
	req := NewStatusCmd("SAVE")
	c.Process(req)
	return req
}

func (c *Client) shutdown(modifier string) *StatusCmd {
	var args []string
	if modifier == "" {
		args = []string{"SHUTDOWN"}
	} else {
		args = []string{"SHUTDOWN", modifier}
	}
	req := NewStatusCmd(args...)
	c.Process(req)
	c.Close()
	return req
}

func (c *Client) Shutdown() *StatusCmd {
	return c.shutdown("")
}

func (c *Client) ShutdownSave() *StatusCmd {
	return c.shutdown("SAVE")
}

func (c *Client) ShutdownNoSave() *StatusCmd {
	return c.shutdown("NOSAVE")
}

func (c *Client) SlaveOf(host, port string) *StatusCmd {
	req := NewStatusCmd("SLAVEOF", host, port)
	c.Process(req)
	return req
}

func (c *Client) SlowLog() {
	panic("not implemented")
}

func (c *Client) Sync() {
	panic("not implemented")
}

func (c *Client) Time() *StringSliceCmd {
	req := NewStringSliceCmd("TIME")
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Eval(script string, keys []string, args []string) *Cmd {
	reqArgs := []string{"EVAL", script, strconv.FormatInt(int64(len(keys)), 10)}
	reqArgs = append(reqArgs, keys...)
	reqArgs = append(reqArgs, args...)
	req := NewCmd(reqArgs...)
	c.Process(req)
	return req
}

func (c *Client) EvalSha(sha1 string, keys []string, args []string) *Cmd {
	reqArgs := []string{"EVALSHA", sha1, strconv.FormatInt(int64(len(keys)), 10)}
	reqArgs = append(reqArgs, keys...)
	reqArgs = append(reqArgs, args...)
	req := NewCmd(reqArgs...)
	c.Process(req)
	return req
}

func (c *Client) ScriptExists(scripts ...string) *BoolSliceCmd {
	args := append([]string{"SCRIPT", "EXISTS"}, scripts...)
	req := NewBoolSliceCmd(args...)
	c.Process(req)
	return req
}

func (c *Client) ScriptFlush() *StatusCmd {
	req := NewStatusCmd("SCRIPT", "FLUSH")
	c.Process(req)
	return req
}

func (c *Client) ScriptKill() *StatusCmd {
	req := NewStatusCmd("SCRIPT", "KILL")
	c.Process(req)
	return req
}

func (c *Client) ScriptLoad(script string) *StringCmd {
	req := NewStringCmd("SCRIPT", "LOAD", script)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) DebugObject(key string) *StringCmd {
	req := NewStringCmd("DEBUG", "OBJECT", key)
	c.Process(req)
	return req
}
