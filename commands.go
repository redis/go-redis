package redis

import (
	"strconv"
)

//------------------------------------------------------------------------------

type Limit struct {
	Offset, Count int64
}

func NewLimit(offset, count int64) *Limit {
	return &Limit{offset, count}
}

//------------------------------------------------------------------------------

func (c *Client) Auth(password string) *StatusReq {
	req := NewStatusReq("AUTH", password)
	c.Queue(req)
	return req
}

func (c *Client) Echo(message string) *BulkReq {
	req := NewBulkReq("ECHO", message)
	c.Queue(req)
	return req
}

func (c *Client) Ping() *StatusReq {
	req := NewStatusReq("PING")
	c.Queue(req)
	return req
}

func (c *Client) Quit() *StatusReq {
	req := NewStatusReq("QUIT")
	c.Run(req)
	c.Close()
	return req
}

func (c *Client) Select(index int64) *StatusReq {
	req := NewStatusReq("SELECT", strconv.FormatInt(index, 10))
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Flushall() *StatusReq {
	req := NewStatusReq("FLUSHALL")
	c.Queue(req)
	return req
}

func (c *Client) Flushdb() *StatusReq {
	req := NewStatusReq("FLUSHDB")
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Del(keys ...string) *IntReq {
	args := append([]string{"DEL"}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) Dump(key string) *BulkReq {
	req := NewBulkReq("DUMP", key)
	c.Queue(req)
	return req
}

func (c *Client) Exists(key string) *BoolReq {
	req := NewBoolReq("EXISTS", key)
	c.Queue(req)
	return req
}

func (c *Client) Expire(key string, seconds int64) *BoolReq {
	req := NewBoolReq("EXPIRE", key, strconv.FormatInt(seconds, 10))
	c.Queue(req)
	return req
}

func (c *Client) ExpireAt(key string, timestamp int64) *BoolReq {
	req := NewBoolReq("EXPIREAT", key, strconv.FormatInt(timestamp, 10))
	c.Queue(req)
	return req
}

func (c *Client) Keys(pattern string) *MultiBulkReq {
	req := NewMultiBulkReq("KEYS", pattern)
	c.Queue(req)
	return req
}

func (c *Client) Migrate(host string, port int32, key, db string, timeout int64) *StatusReq {
	req := NewStatusReq(
		"MIGRATE",
		host,
		strconv.FormatInt(int64(port), 10),
		key,
		db,
		strconv.FormatInt(timeout, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) Move(key string, db int64) *BoolReq {
	req := NewBoolReq("MOVE", key, strconv.FormatInt(db, 10))
	c.Queue(req)
	return req
}

func (c *Client) ObjectRefCount(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "REFCOUNT"}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ObjectEncoding(keys ...string) *BulkReq {
	args := append([]string{"OBJECT", "ENCODING"}, keys...)
	req := NewBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ObjectIdleTime(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "IDLETIME"}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) Persist(key string) *BoolReq {
	req := NewBoolReq("PERSIST", key)
	c.Queue(req)
	return req
}

func (c *Client) Pexpire(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIRE", key, strconv.FormatInt(milliseconds, 10))
	c.Queue(req)
	return req
}

func (c *Client) PexpireAt(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIREAT", key, strconv.FormatInt(milliseconds, 10))
	c.Queue(req)
	return req
}

func (c *Client) PTTL(key string) *IntReq {
	req := NewIntReq("PTTL", key)
	c.Queue(req)
	return req
}

func (c *Client) RandomKey() *BulkReq {
	req := NewBulkReq("RANDOMKEY")
	c.Queue(req)
	return req
}

func (c *Client) Rename(key, newkey string) *StatusReq {
	req := NewStatusReq("RENAME", key, newkey)
	c.Queue(req)
	return req
}

func (c *Client) RenameNX(key, newkey string) *BoolReq {
	req := NewBoolReq("RENAMENX", key, newkey)
	c.Queue(req)
	return req
}

func (c *Client) Restore(key, ttl int64, value string) *StatusReq {
	req := NewStatusReq(
		"RESTORE",
		strconv.FormatInt(ttl, 10),
		value,
	)
	c.Queue(req)
	return req
}

func (c *Client) Sort(key string, params ...string) *MultiBulkReq {
	args := append([]string{"SORT", key}, params...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) TTL(key string) *IntReq {
	req := NewIntReq("TTL", key)
	c.Queue(req)
	return req
}

func (c *Client) Type(key string) *StatusReq {
	req := NewStatusReq("TYPE", key)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Append(key, value string) *IntReq {
	req := NewIntReq("APPEND", key, value)
	c.Queue(req)
	return req
}

// BitCount

// BitOp

func (c *Client) Decr(key string) *IntReq {
	req := NewIntReq("DECR", key)
	c.Queue(req)
	return req
}

func (c *Client) DecrBy(key string, decrement int64) *IntReq {
	req := NewIntReq("DECRBY", key, strconv.FormatInt(decrement, 10))
	c.Queue(req)
	return req
}

func (c *Client) Get(key string) *BulkReq {
	req := NewBulkReq("GET", key)
	c.Queue(req)
	return req
}

func (c *Client) GetBit(key string, offset int64) *IntReq {
	req := NewIntReq("GETBIT", key, strconv.FormatInt(offset, 10))
	c.Queue(req)
	return req
}

func (c *Client) GetRange(key string, start, end int64) *BulkReq {
	req := NewBulkReq(
		"GETRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(end, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) GetSet(key, value string) *BulkReq {
	req := NewBulkReq("GETSET", key, value)
	c.Queue(req)
	return req
}

func (c *Client) Incr(key string) *IntReq {
	req := NewIntReq("INCR", key)
	c.Queue(req)
	return req
}

func (c *Client) IncrBy(key string, value int64) *IntReq {
	req := NewIntReq("INCRBY", key, strconv.FormatInt(value, 10))
	c.Queue(req)
	return req
}

// incrbyfloat

func (c *Client) MGet(keys ...string) *MultiBulkReq {
	args := append([]string{"MGET"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) MSet(pairs ...string) *StatusReq {
	args := append([]string{"MSET"}, pairs...)
	req := NewStatusReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) MSetNX(pairs ...string) *BoolReq {
	args := append([]string{"MSETNX"}, pairs...)
	req := NewBoolReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) PSetEx(key string, milliseconds int64, value string) *StatusReq {
	req := NewStatusReq(
		"PSETEX",
		key,
		strconv.FormatInt(milliseconds, 10),
		value,
	)
	c.Queue(req)
	return req
}

func (c *Client) Set(key, value string) *StatusReq {
	req := NewStatusReq("SET", key, value)
	c.Queue(req)
	return req
}

func (c *Client) SetBit(key string, offset int64, value int) *IntReq {
	req := NewIntReq(
		"SETBIT",
		key,
		strconv.FormatInt(offset, 10),
		strconv.FormatInt(int64(value), 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) SetEx(key string, seconds int64, value string) *StatusReq {
	req := NewStatusReq("SETEX", key, strconv.FormatInt(seconds, 10), value)
	c.Queue(req)
	return req
}

func (c *Client) SetNx(key, value string) *BoolReq {
	req := NewBoolReq("SETNX", key, value)
	c.Queue(req)
	return req
}

func (c *Client) SetRange(key string, offset int64, value string) *IntReq {
	req := NewIntReq("SETRANGE", key, strconv.FormatInt(offset, 10), value)
	c.Queue(req)
	return req
}

func (c *Client) StrLen(key string) *IntReq {
	req := NewIntReq("STRLEN", key)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) HDel(key string, fields ...string) *IntReq {
	args := append([]string{"HDEL", key}, fields...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) HExists(key, field string) *BoolReq {
	req := NewBoolReq("HEXISTS", key, field)
	c.Queue(req)
	return req
}

func (c *Client) HGet(key, field string) *BulkReq {
	req := NewBulkReq("HGET", key, field)
	c.Queue(req)
	return req
}

func (c *Client) HGetAll(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HGETALL", key)
	c.Queue(req)
	return req
}

func (c *Client) HIncrBy(key, field string, incr int64) *IntReq {
	req := NewIntReq("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Queue(req)
	return req
}

// hincrbyfloat

func (c *Client) HKeys(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HKEYS", key)
	c.Queue(req)
	return req
}

func (c *Client) HLen(key string) *IntReq {
	req := NewIntReq("HLEN", key)
	c.Queue(req)
	return req
}

func (c *Client) HMGet(key string, fields ...string) *MultiBulkReq {
	args := append([]string{"HMGET", key}, fields...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) HMSet(key, field, value string, pairs ...string) *StatusReq {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	req := NewStatusReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) HSet(key, field, value string) *BoolReq {
	req := NewBoolReq("HSET", key, field, value)
	c.Queue(req)
	return req
}

func (c *Client) HSetNX(key, field, value string) *BoolReq {
	req := NewBoolReq("HSETNX", key, field, value)
	c.Queue(req)
	return req
}

func (c *Client) HVals(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HVALS", key)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BLPop(timeout int64, keys ...string) *MultiBulkReq {
	args := append([]string{"BLPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) BRPop(timeout int64, keys ...string) *MultiBulkReq {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) BRPopLPush(source, destination string, timeout int64) *BulkReq {
	req := NewBulkReq(
		"BRPOPLPUSH",
		source,
		destination,
		strconv.FormatInt(timeout, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) LIndex(key string, index int64) *BulkReq {
	req := NewBulkReq("LINDEX", key, strconv.FormatInt(index, 10))
	c.Queue(req)
	return req
}

func (c *Client) LInsert(key, op, pivot, value string) *IntReq {
	req := NewIntReq("LINSERT", key, op, pivot, value)
	c.Queue(req)
	return req
}

func (c *Client) LLen(key string) *IntReq {
	req := NewIntReq("LLEN", key)
	c.Queue(req)
	return req
}

func (c *Client) LPop(key string) *BulkReq {
	req := NewBulkReq("LPOP", key)
	c.Queue(req)
	return req
}

func (c *Client) LPush(key string, values ...string) *IntReq {
	args := append([]string{"LPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) LPushX(key, value string) *IntReq {
	req := NewIntReq("LPUSHX", key, value)
	c.Queue(req)
	return req
}

func (c *Client) LRange(key string, start, stop int64) *MultiBulkReq {
	req := NewMultiBulkReq(
		"LRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) LRem(key string, count int64, value string) *IntReq {
	req := NewIntReq("LREM", key, strconv.FormatInt(count, 10), value)
	c.Queue(req)
	return req
}

func (c *Client) LSet(key string, index int64, value string) *StatusReq {
	req := NewStatusReq("LSET", key, strconv.FormatInt(index, 10), value)
	c.Queue(req)
	return req
}

func (c *Client) LTrim(key string, start, stop int64) *StatusReq {
	req := NewStatusReq(
		"LTRIM",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) RPop(key string) *BulkReq {
	req := NewBulkReq("RPOP", key)
	c.Queue(req)
	return req
}

func (c *Client) RPopLPush(source, destination string) *BulkReq {
	req := NewBulkReq("RPOPLPUSH", source, destination)
	c.Queue(req)
	return req
}

func (c *Client) RPush(key string, values ...string) *IntReq {
	args := append([]string{"RPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) RPushX(key string, value string) *IntReq {
	req := NewIntReq("RPUSHX", key, value)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) SAdd(key string, members ...string) *IntReq {
	args := append([]string{"SADD", key}, members...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SCard(key string) *IntReq {
	req := NewIntReq("SCARD", key)
	c.Queue(req)
	return req
}

func (c *Client) SDiff(keys ...string) *MultiBulkReq {
	args := append([]string{"SDIFF"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SDiffStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SInter(keys ...string) *MultiBulkReq {
	args := append([]string{"SINTER"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SInterStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SIsMember(key, member string) *BoolReq {
	req := NewBoolReq("SISMEMBER", key, member)
	c.Queue(req)
	return req
}

func (c *Client) SMembers(key string) *MultiBulkReq {
	req := NewMultiBulkReq("SMEMBERS", key)
	c.Queue(req)
	return req
}

func (c *Client) SMove(source, destination, member string) *BoolReq {
	req := NewBoolReq("SMOVE", source, destination, member)
	c.Queue(req)
	return req
}

func (c *Client) SPop(key string) *BulkReq {
	req := NewBulkReq("SPOP", key)
	c.Queue(req)
	return req
}

func (c *Client) SRandMember(key string) *BulkReq {
	req := NewBulkReq("SRANDMEMBER", key)
	c.Queue(req)
	return req
}

func (c *Client) SRem(key string, members ...string) *IntReq {
	args := append([]string{"SREM", key}, members...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SUnion(keys ...string) *MultiBulkReq {
	args := append([]string{"SUNION"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) SUnionStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SUNIONSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

type ZMember struct {
	Score  float64
	Member string
}

func NewZMember(score float64, member string) *ZMember {
	return &ZMember{score, member}
}

func (m *ZMember) ScoreString() string {
	return strconv.FormatFloat(m.Score, 'f', -1, 32)
}

func (c *Client) ZAdd(key string, members ...*ZMember) *IntReq {
	args := []string{"ZADD", key}
	for _, m := range members {
		args = append(args, m.ScoreString(), m.Member)
	}
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZCard(key string) *IntReq {
	req := NewIntReq("ZCARD", key)
	c.Queue(req)
	return req
}

func (c *Client) ZCount(key, min, max string) *IntReq {
	req := NewIntReq("ZCOUNT", key, min, max)
	c.Queue(req)
	return req
}

func (c *Client) ZIncrBy(key string, increment int64, member string) *IntReq {
	req := NewIntReq("ZINCRBY", key, strconv.FormatInt(increment, 10), member)
	c.Queue(req)
	return req
}

func (c *Client) ZInterStore(
	destination string,
	numkeys int64,
	keys []string,
	weights []int64,
	aggregate string,
) *IntReq {
	args := []string{"ZINTERSTORE", destination, strconv.FormatInt(numkeys, 10)}
	args = append(args, keys...)
	if weights != nil {
		args = append(args, "WEIGHTS")
		for _, w := range weights {
			args = append(args, strconv.FormatInt(w, 10))
		}
	}
	if aggregate != "" {
		args = append(args, "AGGREGATE", aggregate)
	}
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRange(key string, start, stop int64, withScores bool) *MultiBulkReq {
	args := []string{
		"ZRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRangeByScore(
	key string,
	min, max string,
	withScores bool,
	limit *Limit,
) *MultiBulkReq {
	args := []string{"ZRANGEBYSCORE", key, min, max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limit != nil {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(limit.Offset, 10),
			strconv.FormatInt(limit.Count, 10),
		)
	}
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRank(key, member string) *IntNilReq {
	req := NewIntNilReq("ZRANK", key, member)
	c.Queue(req)
	return req
}

func (c *Client) ZRem(key string, members ...string) *IntReq {
	args := append([]string{"ZREM", key}, members...)
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRemRangeByRank(key string, start, stop int64) *IntReq {
	req := NewIntReq(
		"ZREMRANGEBYRANK",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Queue(req)
	return req
}

func (c *Client) ZRemRangeByScore(key, min, max string) *IntReq {
	req := NewIntReq("ZREMRANGEBYSCORE", key, min, max)
	c.Queue(req)
	return req
}

func (c *Client) ZRevRange(key, start, stop string, withScores bool) *MultiBulkReq {
	args := []string{"ZREVRANGE", key, start, stop}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRevRangeByScore(
	key, start, stop string,
	withScores bool,
	limit *Limit,
) *MultiBulkReq {
	args := []string{"ZREVRANGEBYSCORE", key, start, stop}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limit != nil {
		args = append(
			args,
			"LIMIT",
			strconv.FormatInt(limit.Offset, 10),
			strconv.FormatInt(limit.Count, 10),
		)
	}
	req := NewMultiBulkReq(args...)
	c.Queue(req)
	return req
}

func (c *Client) ZRevRank(key, member string) *IntNilReq {
	req := NewIntNilReq("ZREVRANK", key, member)
	c.Queue(req)
	return req
}

func (c *Client) ZScore(key, member string) *FloatReq {
	req := NewFloatReq("ZSCORE", key, member)
	c.Queue(req)
	return req
}

func (c *Client) ZUnionStore(
	destination string,
	numkeys int64,
	keys []string,
	weights []int64,
	aggregate string,
) *IntReq {
	args := []string{"ZUNIONSTORE", destination, strconv.FormatInt(numkeys, 10)}
	args = append(args, keys...)
	if weights != nil {
		args = append(args, "WEIGHTS")
		for _, w := range weights {
			args = append(args, strconv.FormatInt(w, 10))
		}
	}
	if aggregate != "" {
		args = append(args, "AGGREGATE", aggregate)
	}
	req := NewIntReq(args...)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) PubSubClient() *PubSubClient {
	return NewPubSubClient(c.connect, c.disconnect)
}

func (c *Client) Publish(channel, message string) *IntReq {
	req := NewIntReq("PUBLISH", channel, message)
	c.Queue(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Multi() *Client {
	return NewMultiClient(c.connect, c.disconnect)
}
