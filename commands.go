package redis

import (
	"strconv"
)

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 32)
}

//------------------------------------------------------------------------------

func (c *Client) Auth(password string) *StatusReq {
	req := NewStatusReq("AUTH", password)
	c.Process(req)
	return req
}

func (c *Client) Echo(message string) *StringReq {
	req := NewStringReq("ECHO", message)
	c.Process(req)
	return req
}

func (c *Client) Ping() *StatusReq {
	req := NewStatusReq("PING")
	c.Process(req)
	return req
}

func (c *Client) Select(index int64) *StatusReq {
	req := NewStatusReq("SELECT", strconv.FormatInt(index, 10))
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Del(keys ...string) *IntReq {
	args := append([]string{"DEL"}, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) Dump(key string) *StringReq {
	req := NewStringReq("DUMP", key)
	c.Process(req)
	return req
}

func (c *Client) Exists(key string) *BoolReq {
	req := NewBoolReq("EXISTS", key)
	c.Process(req)
	return req
}

func (c *Client) Expire(key string, seconds int64) *BoolReq {
	req := NewBoolReq("EXPIRE", key, strconv.FormatInt(seconds, 10))
	c.Process(req)
	return req
}

func (c *Client) ExpireAt(key string, timestamp int64) *BoolReq {
	req := NewBoolReq("EXPIREAT", key, strconv.FormatInt(timestamp, 10))
	c.Process(req)
	return req
}

func (c *Client) Keys(pattern string) *StringSliceReq {
	req := NewStringSliceReq("KEYS", pattern)
	c.Process(req)
	return req
}

func (c *Client) Migrate(host, port, key string, db, timeout int64) *StatusReq {
	req := NewStatusReq(
		"MIGRATE",
		host,
		port,
		key,
		strconv.FormatInt(db, 10),
		strconv.FormatInt(timeout, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) Move(key string, db int64) *BoolReq {
	req := NewBoolReq("MOVE", key, strconv.FormatInt(db, 10))
	c.Process(req)
	return req
}

func (c *Client) ObjectRefCount(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "REFCOUNT"}, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ObjectEncoding(keys ...string) *StringReq {
	args := append([]string{"OBJECT", "ENCODING"}, keys...)
	req := NewStringReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ObjectIdleTime(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "IDLETIME"}, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) Persist(key string) *BoolReq {
	req := NewBoolReq("PERSIST", key)
	c.Process(req)
	return req
}

func (c *Client) PExpire(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIRE", key, strconv.FormatInt(milliseconds, 10))
	c.Process(req)
	return req
}

func (c *Client) PExpireAt(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIREAT", key, strconv.FormatInt(milliseconds, 10))
	c.Process(req)
	return req
}

func (c *Client) PTTL(key string) *IntReq {
	req := NewIntReq("PTTL", key)
	c.Process(req)
	return req
}

func (c *Client) RandomKey() *StringReq {
	req := NewStringReq("RANDOMKEY")
	c.Process(req)
	return req
}

func (c *Client) Rename(key, newkey string) *StatusReq {
	req := NewStatusReq("RENAME", key, newkey)
	c.Process(req)
	return req
}

func (c *Client) RenameNX(key, newkey string) *BoolReq {
	req := NewBoolReq("RENAMENX", key, newkey)
	c.Process(req)
	return req
}

func (c *Client) Restore(key string, ttl int64, value string) *StatusReq {
	req := NewStatusReq(
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

func (c *Client) Sort(key string, sort Sort) *StringSliceReq {
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
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) TTL(key string) *IntReq {
	req := NewIntReq("TTL", key)
	c.Process(req)
	return req
}

func (c *Client) Type(key string) *StatusReq {
	req := NewStatusReq("TYPE", key)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Append(key, value string) *IntReq {
	req := NewIntReq("APPEND", key, value)
	c.Process(req)
	return req
}

type BitCount struct {
	Start, End int64
}

func (c *Client) BitCount(key string, bitCount *BitCount) *IntReq {
	args := []string{"BITCOUNT", key}
	if bitCount != nil {
		args = append(
			args,
			strconv.FormatInt(bitCount.Start, 10),
			strconv.FormatInt(bitCount.End, 10),
		)
	}
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) bitOp(op, destKey string, keys ...string) *IntReq {
	args := []string{"BITOP", op, destKey}
	args = append(args, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) BitOpAnd(destKey string, keys ...string) *IntReq {
	return c.bitOp("AND", destKey, keys...)
}

func (c *Client) BitOpOr(destKey string, keys ...string) *IntReq {
	return c.bitOp("OR", destKey, keys...)
}

func (c *Client) BitOpXor(destKey string, keys ...string) *IntReq {
	return c.bitOp("XOR", destKey, keys...)
}

func (c *Client) BitOpNot(destKey string, key string) *IntReq {
	return c.bitOp("NOT", destKey, key)
}

func (c *Client) Decr(key string) *IntReq {
	req := NewIntReq("DECR", key)
	c.Process(req)
	return req
}

func (c *Client) DecrBy(key string, decrement int64) *IntReq {
	req := NewIntReq("DECRBY", key, strconv.FormatInt(decrement, 10))
	c.Process(req)
	return req
}

func (c *Client) Get(key string) *StringReq {
	req := NewStringReq("GET", key)
	c.Process(req)
	return req
}

func (c *Client) GetBit(key string, offset int64) *IntReq {
	req := NewIntReq("GETBIT", key, strconv.FormatInt(offset, 10))
	c.Process(req)
	return req
}

func (c *Client) GetRange(key string, start, end int64) *StringReq {
	req := NewStringReq(
		"GETRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(end, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) GetSet(key, value string) *StringReq {
	req := NewStringReq("GETSET", key, value)
	c.Process(req)
	return req
}

func (c *Client) Incr(key string) *IntReq {
	req := NewIntReq("INCR", key)
	c.Process(req)
	return req
}

func (c *Client) IncrBy(key string, value int64) *IntReq {
	req := NewIntReq("INCRBY", key, strconv.FormatInt(value, 10))
	c.Process(req)
	return req
}

func (c *Client) IncrByFloat(key string, value float64) *FloatReq {
	req := NewFloatReq("INCRBYFLOAT", key, formatFloat(value))
	c.Process(req)
	return req
}

func (c *Client) MGet(keys ...string) *IfaceSliceReq {
	args := append([]string{"MGET"}, keys...)
	req := NewIfaceSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) MSet(pairs ...string) *StatusReq {
	args := append([]string{"MSET"}, pairs...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *Client) MSetNX(pairs ...string) *BoolReq {
	args := append([]string{"MSETNX"}, pairs...)
	req := NewBoolReq(args...)
	c.Process(req)
	return req
}

func (c *Client) PSetEx(key string, milliseconds int64, value string) *StatusReq {
	req := NewStatusReq(
		"PSETEX",
		key,
		strconv.FormatInt(milliseconds, 10),
		value,
	)
	c.Process(req)
	return req
}

func (c *Client) Set(key, value string) *StatusReq {
	req := NewStatusReq("SET", key, value)
	c.Process(req)
	return req
}

func (c *Client) SetBit(key string, offset int64, value int) *IntReq {
	req := NewIntReq(
		"SETBIT",
		key,
		strconv.FormatInt(offset, 10),
		strconv.FormatInt(int64(value), 10),
	)
	c.Process(req)
	return req
}

func (c *Client) SetEx(key string, seconds int64, value string) *StatusReq {
	req := NewStatusReq("SETEX", key, strconv.FormatInt(seconds, 10), value)
	c.Process(req)
	return req
}

func (c *Client) SetNX(key, value string) *BoolReq {
	req := NewBoolReq("SETNX", key, value)
	c.Process(req)
	return req
}

func (c *Client) SetRange(key string, offset int64, value string) *IntReq {
	req := NewIntReq("SETRANGE", key, strconv.FormatInt(offset, 10), value)
	c.Process(req)
	return req
}

func (c *Client) StrLen(key string) *IntReq {
	req := NewIntReq("STRLEN", key)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) HDel(key string, fields ...string) *IntReq {
	args := append([]string{"HDEL", key}, fields...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) HExists(key, field string) *BoolReq {
	req := NewBoolReq("HEXISTS", key, field)
	c.Process(req)
	return req
}

func (c *Client) HGet(key, field string) *StringReq {
	req := NewStringReq("HGET", key, field)
	c.Process(req)
	return req
}

func (c *Client) HGetAll(key string) *StringSliceReq {
	req := NewStringSliceReq("HGETALL", key)
	c.Process(req)
	return req
}

func (c *Client) HIncrBy(key, field string, incr int64) *IntReq {
	req := NewIntReq("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Process(req)
	return req
}

func (c *Client) HIncrByFloat(key, field string, incr float64) *FloatReq {
	req := NewFloatReq("HINCRBYFLOAT", key, field, formatFloat(incr))
	c.Process(req)
	return req
}

func (c *Client) HKeys(key string) *StringSliceReq {
	req := NewStringSliceReq("HKEYS", key)
	c.Process(req)
	return req
}

func (c *Client) HLen(key string) *IntReq {
	req := NewIntReq("HLEN", key)
	c.Process(req)
	return req
}

func (c *Client) HMGet(key string, fields ...string) *IfaceSliceReq {
	args := append([]string{"HMGET", key}, fields...)
	req := NewIfaceSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) HMSet(key, field, value string, pairs ...string) *StatusReq {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *Client) HSet(key, field, value string) *BoolReq {
	req := NewBoolReq("HSET", key, field, value)
	c.Process(req)
	return req
}

func (c *Client) HSetNX(key, field, value string) *BoolReq {
	req := NewBoolReq("HSETNX", key, field, value)
	c.Process(req)
	return req
}

func (c *Client) HVals(key string) *StringSliceReq {
	req := NewStringSliceReq("HVALS", key)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BLPop(timeout int64, keys ...string) *StringSliceReq {
	args := append([]string{"BLPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) BRPop(timeout int64, keys ...string) *StringSliceReq {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) BRPopLPush(source, destination string, timeout int64) *StringReq {
	req := NewStringReq(
		"BRPOPLPUSH",
		source,
		destination,
		strconv.FormatInt(timeout, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) LIndex(key string, index int64) *StringReq {
	req := NewStringReq("LINDEX", key, strconv.FormatInt(index, 10))
	c.Process(req)
	return req
}

func (c *Client) LInsert(key, op, pivot, value string) *IntReq {
	req := NewIntReq("LINSERT", key, op, pivot, value)
	c.Process(req)
	return req
}

func (c *Client) LLen(key string) *IntReq {
	req := NewIntReq("LLEN", key)
	c.Process(req)
	return req
}

func (c *Client) LPop(key string) *StringReq {
	req := NewStringReq("LPOP", key)
	c.Process(req)
	return req
}

func (c *Client) LPush(key string, values ...string) *IntReq {
	args := append([]string{"LPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) LPushX(key, value string) *IntReq {
	req := NewIntReq("LPUSHX", key, value)
	c.Process(req)
	return req
}

func (c *Client) LRange(key string, start, stop int64) *StringSliceReq {
	req := NewStringSliceReq(
		"LRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) LRem(key string, count int64, value string) *IntReq {
	req := NewIntReq("LREM", key, strconv.FormatInt(count, 10), value)
	c.Process(req)
	return req
}

func (c *Client) LSet(key string, index int64, value string) *StatusReq {
	req := NewStatusReq("LSET", key, strconv.FormatInt(index, 10), value)
	c.Process(req)
	return req
}

func (c *Client) LTrim(key string, start, stop int64) *StatusReq {
	req := NewStatusReq(
		"LTRIM",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) RPop(key string) *StringReq {
	req := NewStringReq("RPOP", key)
	c.Process(req)
	return req
}

func (c *Client) RPopLPush(source, destination string) *StringReq {
	req := NewStringReq("RPOPLPUSH", source, destination)
	c.Process(req)
	return req
}

func (c *Client) RPush(key string, values ...string) *IntReq {
	args := append([]string{"RPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) RPushX(key string, value string) *IntReq {
	req := NewIntReq("RPUSHX", key, value)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) SAdd(key string, members ...string) *IntReq {
	args := append([]string{"SADD", key}, members...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SCard(key string) *IntReq {
	req := NewIntReq("SCARD", key)
	c.Process(req)
	return req
}

func (c *Client) SDiff(keys ...string) *StringSliceReq {
	args := append([]string{"SDIFF"}, keys...)
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SDiffStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SInter(keys ...string) *StringSliceReq {
	args := append([]string{"SINTER"}, keys...)
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SInterStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SIsMember(key, member string) *BoolReq {
	req := NewBoolReq("SISMEMBER", key, member)
	c.Process(req)
	return req
}

func (c *Client) SMembers(key string) *StringSliceReq {
	req := NewStringSliceReq("SMEMBERS", key)
	c.Process(req)
	return req
}

func (c *Client) SMove(source, destination, member string) *BoolReq {
	req := NewBoolReq("SMOVE", source, destination, member)
	c.Process(req)
	return req
}

func (c *Client) SPop(key string) *StringReq {
	req := NewStringReq("SPOP", key)
	c.Process(req)
	return req
}

func (c *Client) SRandMember(key string) *StringReq {
	req := NewStringReq("SRANDMEMBER", key)
	c.Process(req)
	return req
}

func (c *Client) SRem(key string, members ...string) *IntReq {
	args := append([]string{"SREM", key}, members...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SUnion(keys ...string) *StringSliceReq {
	args := append([]string{"SUNION"}, keys...)
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) SUnionStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SUNIONSTORE", destination}, keys...)
	req := NewIntReq(args...)
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

func (m *Z) ScoreString() string {
	return formatFloat(m.Score)
}

func (c *Client) ZAdd(key string, members ...Z) *IntReq {
	args := []string{"ZADD", key}
	for _, m := range members {
		args = append(args, m.ScoreString(), m.Member)
	}
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZCard(key string) *IntReq {
	req := NewIntReq("ZCARD", key)
	c.Process(req)
	return req
}

func (c *Client) ZCount(key, min, max string) *IntReq {
	req := NewIntReq("ZCOUNT", key, min, max)
	c.Process(req)
	return req
}

func (c *Client) ZIncrBy(key string, increment float64, member string) *FloatReq {
	req := NewFloatReq("ZINCRBY", key, formatFloat(increment), member)
	c.Process(req)
	return req
}

func (c *Client) ZInterStore(
	destination string,
	numkeys int64,
	store ZStore,
	keys ...string,
) *IntReq {
	args := []string{"ZINTERSTORE", destination, strconv.FormatInt(numkeys, 10)}
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
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) zRange(key string, start, stop int64, withScores bool) *StringSliceReq {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRange(key string, start, stop int64) *StringSliceReq {
	return c.zRange(key, start, stop, false)
}

func (c *Client) ZRangeWithScores(key string, start, stop int64) *StringSliceReq {
	return c.zRange(key, start, stop, true)
}

func (c *Client) zRangeByScore(
	key string,
	min, max string,
	withScores bool,
	offset, count int64,
) *StringSliceReq {
	args := []string{"ZRANGEBYSCORE", key, min, max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if offset != 0 || count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(offset, 10),
			strconv.FormatInt(count, 10),
		)
	}
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRangeByScore(key string, min, max string, offset, count int64) *StringSliceReq {
	return c.zRangeByScore(key, min, max, false, offset, count)
}

func (c *Client) ZRangeByScoreWithScores(key string, min, max string, offset, count int64) *StringSliceReq {
	return c.zRangeByScore(key, min, max, true, offset, count)
}

func (c *Client) ZRank(key, member string) *IntReq {
	req := NewIntReq("ZRANK", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZRem(key string, members ...string) *IntReq {
	args := append([]string{"ZREM", key}, members...)
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRemRangeByRank(key string, start, stop int64) *IntReq {
	req := NewIntReq(
		"ZREMRANGEBYRANK",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(req)
	return req
}

func (c *Client) ZRemRangeByScore(key, min, max string) *IntReq {
	req := NewIntReq("ZREMRANGEBYSCORE", key, min, max)
	c.Process(req)
	return req
}

func (c *Client) zRevRange(key, start, stop string, withScores bool) *StringSliceReq {
	args := []string{"ZREVRANGE", key, start, stop}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRevRange(key, start, stop string) *StringSliceReq {
	return c.zRevRange(key, start, stop, false)
}

func (c *Client) ZRevRangeWithScores(key, start, stop string) *StringSliceReq {
	return c.zRevRange(key, start, stop, true)
}

func (c *Client) zRevRangeByScore(key, start, stop string, withScores bool, offset, count int64) *StringSliceReq {
	args := []string{"ZREVRANGEBYSCORE", key, start, stop}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if offset != 0 || count != 0 {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(offset, 10),
			strconv.FormatInt(count, 10),
		)
	}
	req := NewStringSliceReq(args...)
	c.Process(req)
	return req
}

func (c *Client) ZRevRangeByScore(key, start, stop string, offset, count int64) *StringSliceReq {
	return c.zRevRangeByScore(key, start, stop, false, offset, count)
}

func (c *Client) ZRevRangeByScoreWithScores(key, start, stop string, offset, count int64) *StringSliceReq {
	return c.zRevRangeByScore(key, start, stop, false, offset, count)
}

func (c *Client) ZRevRank(key, member string) *IntReq {
	req := NewIntReq("ZREVRANK", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZScore(key, member string) *FloatReq {
	req := NewFloatReq("ZSCORE", key, member)
	c.Process(req)
	return req
}

func (c *Client) ZUnionStore(
	destination string,
	numkeys int64,
	store ZStore,
	keys ...string,
) *IntReq {
	args := []string{"ZUNIONSTORE", destination, strconv.FormatInt(numkeys, 10)}
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
	req := NewIntReq(args...)
	c.Process(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BgRewriteAOF() *StatusReq {
	req := NewStatusReq("BGREWRITEAOF")
	c.Process(req)
	return req
}

func (c *Client) BgSave() *StatusReq {
	req := NewStatusReq("BGSAVE")
	c.Process(req)
	return req
}

func (c *Client) ClientKill(ipPort string) *StatusReq {
	req := NewStatusReq("CLIENT", "KILL", ipPort)
	c.Process(req)
	return req
}

func (c *Client) ClientList() *StringReq {
	req := NewStringReq("CLIENT", "LIST")
	c.Process(req)
	return req
}

func (c *Client) ConfigGet(parameter string) *StringSliceReq {
	req := NewStringSliceReq("CONFIG", "GET", parameter)
	c.Process(req)
	return req
}

func (c *Client) ConfigResetStat() *StatusReq {
	req := NewStatusReq("CONFIG", "RESETSTAT")
	c.Process(req)
	return req
}

func (c *Client) ConfigSet(parameter, value string) *StatusReq {
	req := NewStatusReq("CONFIG", "SET", parameter, value)
	c.Process(req)
	return req
}

func (c *Client) DbSize() *IntReq {
	req := NewIntReq("DBSIZE")
	c.Process(req)
	return req
}

func (c *Client) FlushAll() *StatusReq {
	req := NewStatusReq("FLUSHALL")
	c.Process(req)
	return req
}

func (c *Client) FlushDb() *StatusReq {
	req := NewStatusReq("FLUSHDB")
	c.Process(req)
	return req
}

func (c *Client) Info() *StringReq {
	req := NewStringReq("INFO")
	c.Process(req)
	return req
}

func (c *Client) LastSave() *IntReq {
	req := NewIntReq("LASTSAVE")
	c.Process(req)
	return req
}

func (c *Client) Monitor() {
	panic("not implemented")
}

func (c *Client) Save() *StatusReq {
	req := NewStatusReq("SAVE")
	c.Process(req)
	return req
}

func (c *Client) Shutdown() {
	panic("not implemented")
}

func (c *Client) SlaveOf(host, port string) *StatusReq {
	req := NewStatusReq("SLAVEOF", host, port)
	c.Process(req)
	return req
}

func (c *Client) SlowLog() {
	panic("not implemented")
}

func (c *Client) Sync() {
	panic("not implemented")
}

func (c *Client) Time() *StringSliceReq {
	req := NewStringSliceReq("TIME")
	c.Process(req)
	return req
}
