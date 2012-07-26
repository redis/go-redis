package redis

import (
	"strconv"
)

//------------------------------------------------------------------------------

func (c *Client) Auth(password string) *StatusReq {
	req := NewStatusReq("AUTH", password)
	c.Run(req)
	return req
}

func (c *Client) Echo(message string) *BulkReq {
	req := NewBulkReq("ECHO", message)
	c.Run(req)
	return req
}

func (c *Client) Ping() *StatusReq {
	req := NewStatusReq("PING")
	c.Run(req)
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
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Flushall() *StatusReq {
	req := NewStatusReq("FLUSHALL")
	c.Run(req)
	return req
}

func (c *Client) Flushdb() *StatusReq {
	req := NewStatusReq("FLUSHDB")
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Del(keys ...string) *IntReq {
	args := append([]string{"DEL"}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Dump(key string) *BulkReq {
	req := NewBulkReq("DUMP", key)
	c.Run(req)
	return req
}

func (c *Client) Exists(key string) *BoolReq {
	req := NewBoolReq("EXISTS", key)
	c.Run(req)
	return req
}

func (c *Client) Expire(key string, seconds int64) *BoolReq {
	req := NewBoolReq("EXPIRE", key, strconv.FormatInt(seconds, 10))
	c.Run(req)
	return req
}

func (c *Client) ExpireAt(key string, timestamp int64) *BoolReq {
	req := NewBoolReq("EXPIREAT", key, strconv.FormatInt(timestamp, 10))
	c.Run(req)
	return req
}

func (c *Client) Keys(pattern string) *MultiBulkReq {
	req := NewMultiBulkReq("KEYS", pattern)
	c.Run(req)
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
	c.Run(req)
	return req
}

func (c *Client) Move(key string, db int64) *BoolReq {
	req := NewBoolReq("MOVE", key, strconv.FormatInt(db, 10))
	c.Run(req)
	return req
}

func (c *Client) ObjectRefCount(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "REFCOUNT"}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) ObjectEncoding(keys ...string) *BulkReq {
	args := append([]string{"OBJECT", "ENCODING"}, keys...)
	req := NewBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) ObjectIdleTime(keys ...string) *IntReq {
	args := append([]string{"OBJECT", "IDLETIME"}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Persist(key string) *BoolReq {
	req := NewBoolReq("PERSIST", key)
	c.Run(req)
	return req
}

func (c *Client) Pexpire(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIRE", key, strconv.FormatInt(milliseconds, 10))
	c.Run(req)
	return req
}

func (c *Client) PexpireAt(key string, milliseconds int64) *BoolReq {
	req := NewBoolReq("PEXPIREAT", key, strconv.FormatInt(milliseconds, 10))
	c.Run(req)
	return req
}

func (c *Client) PTTL(key string) *IntReq {
	req := NewIntReq("PTTL", key)
	c.Run(req)
	return req
}

func (c *Client) RandomKey() *BulkReq {
	req := NewBulkReq("RANDOMKEY")
	c.Run(req)
	return req
}

func (c *Client) Rename(key, newkey string) *StatusReq {
	req := NewStatusReq("RENAME", key, newkey)
	c.Run(req)
	return req
}

func (c *Client) RenameNX(key, newkey string) *BoolReq {
	req := NewBoolReq("RENAMENX", key, newkey)
	c.Run(req)
	return req
}

func (c *Client) Restore(key, ttl int64, value string) *StatusReq {
	req := NewStatusReq(
		"RESTORE",
		strconv.FormatInt(ttl, 10),
		value,
	)
	c.Run(req)
	return req
}

func (c *Client) Sort(key string, params ...string) *MultiBulkReq {
	args := append([]string{"SORT", key}, params...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) TTL(key string) *IntReq {
	req := NewIntReq("TTL", key)
	c.Run(req)
	return req
}

func (c *Client) Type(key string) *StatusReq {
	req := NewStatusReq("TYPE", key)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Append(key, value string) *IntReq {
	req := NewIntReq("APPEND", key, value)
	c.Run(req)
	return req
}

// BitCount

// BitOp

func (c *Client) Decr(key string) *IntReq {
	req := NewIntReq("DECR", key)
	c.Run(req)
	return req
}

func (c *Client) DecrBy(key string, decrement int64) *IntReq {
	req := NewIntReq("DECRBY", key, strconv.FormatInt(decrement, 10))
	c.Run(req)
	return req
}

func (c *Client) Get(key string) *BulkReq {
	req := NewBulkReq("GET", key)
	c.Run(req)
	return req
}

func (c *Client) GetBit(key string, offset int64) *IntReq {
	req := NewIntReq("GETBIT", key, strconv.FormatInt(offset, 10))
	c.Run(req)
	return req
}

func (c *Client) GetRange(key string, start, end int64) *BulkReq {
	req := NewBulkReq(
		"GETRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(end, 10),
	)
	c.Run(req)
	return req
}

func (c *Client) GetSet(key, value string) *BulkReq {
	req := NewBulkReq("GETSET", key, value)
	c.Run(req)
	return req
}

func (c *Client) Incr(key string) *IntReq {
	req := NewIntReq("INCR", key)
	c.Run(req)
	return req
}

func (c *Client) IncrBy(key string, value int64) *IntReq {
	req := NewIntReq("INCRBY", key, strconv.FormatInt(value, 10))
	c.Run(req)
	return req
}

// incrbyfloat

func (c *Client) MGet(keys ...string) *MultiBulkReq {
	args := append([]string{"MGET"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) MSet(pairs ...string) *StatusReq {
	args := append([]string{"MSET"}, pairs...)
	req := NewStatusReq(args...)
	c.Run(req)
	return req
}

func (c *Client) MSetNX(pairs ...string) *BoolReq {
	args := append([]string{"MSETNX"}, pairs...)
	req := NewBoolReq(args...)
	c.Run(req)
	return req
}

func (c *Client) PSetEx(key string, milliseconds int64, value string) *StatusReq {
	req := NewStatusReq(
		"PSETEX",
		key,
		strconv.FormatInt(milliseconds, 10),
		value,
	)
	c.Run(req)
	return req
}

func (c *Client) Set(key, value string) *StatusReq {
	req := NewStatusReq("SET", key, value)
	c.Run(req)
	return req
}

func (c *Client) SetBit(key string, offset int64, value int) *IntReq {
	req := NewIntReq(
		"SETBIT",
		key,
		strconv.FormatInt(offset, 10),
		strconv.FormatInt(int64(value), 10),
	)
	c.Run(req)
	return req
}

func (c *Client) SetEx(key string, seconds int64, value string) *StatusReq {
	req := NewStatusReq("SETEX", key, strconv.FormatInt(seconds, 10), value)
	c.Run(req)
	return req
}

func (c *Client) SetNx(key, value string) *BoolReq {
	req := NewBoolReq("SETNX", key, value)
	c.Run(req)
	return req
}

func (c *Client) SetRange(key string, offset int64, value string) *IntReq {
	req := NewIntReq("SETRANGE", key, strconv.FormatInt(offset, 10), value)
	c.Run(req)
	return req
}

func (c *Client) StrLen(key string) *IntReq {
	req := NewIntReq("STRLEN", key)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) HDel(key string, fields ...string) *IntReq {
	args := append([]string{"HDEL", key}, fields...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) HExists(key, field string) *BoolReq {
	req := NewBoolReq("HEXISTS", key, field)
	c.Run(req)
	return req
}

func (c *Client) HGet(key, field string) *BulkReq {
	req := NewBulkReq("HGET", key, field)
	c.Run(req)
	return req
}

func (c *Client) HGetAll(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HGETALL", key)
	c.Run(req)
	return req
}

func (c *Client) HIncrBy(key, field string, incr int64) *IntReq {
	req := NewIntReq("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Run(req)
	return req
}

// hincrbyfloat

func (c *Client) HKeys(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HKEYS", key)
	c.Run(req)
	return req
}

func (c *Client) HLen(key string) *IntReq {
	req := NewIntReq("HLEN", key)
	c.Run(req)
	return req
}

func (c *Client) HMGet(key string, fields ...string) *MultiBulkReq {
	args := append([]string{"HMGET", key}, fields...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) HMSet(key, field, value string, pairs ...string) *StatusReq {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	req := NewStatusReq(args...)
	c.Run(req)
	return req
}

func (c *Client) HSet(key, field, value string) *BoolReq {
	req := NewBoolReq("HSET", key, field, value)
	c.Run(req)
	return req
}

func (c *Client) HSetNX(key, field, value string) *BoolReq {
	req := NewBoolReq("HSETNX", key, field, value)
	c.Run(req)
	return req
}

func (c *Client) HVals(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HVALS", key)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) BLPop(timeout int64, keys ...string) *MultiBulkReq {
	args := append([]string{"BLPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) BRPop(timeout int64, keys ...string) *MultiBulkReq {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, strconv.FormatInt(timeout, 10))
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) BRPopLPush(source, destination string, timeout int64) *BulkReq {
	req := NewBulkReq(
		"BRPOPLPUSH",
		source,
		destination,
		strconv.FormatInt(timeout, 10),
	)
	c.Run(req)
	return req
}

func (c *Client) LIndex(key string, index int64) *BulkReq {
	req := NewBulkReq("LINDEX", key, strconv.FormatInt(index, 10))
	c.Run(req)
	return req
}

func (c *Client) LInsert(key, op, pivot, value string) *IntReq {
	req := NewIntReq("LINSERT", key, op, pivot, value)
	c.Run(req)
	return req
}

func (c *Client) LLen(key string) *IntReq {
	req := NewIntReq("LLEN", key)
	c.Run(req)
	return req
}

func (c *Client) LPop(key string) *BulkReq {
	req := NewBulkReq("LPOP", key)
	c.Run(req)
	return req
}

func (c *Client) LPush(key string, values ...string) *IntReq {
	args := append([]string{"LPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) LPushX(key, value string) *IntReq {
	req := NewIntReq("LPUSHX", key, value)
	c.Run(req)
	return req
}

func (c *Client) LRange(key string, start, stop int64) *MultiBulkReq {
	req := NewMultiBulkReq(
		"LRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Run(req)
	return req
}

func (c *Client) LRem(key string, count int64, value string) *IntReq {
	req := NewIntReq("LREM", key, strconv.FormatInt(count, 10), value)
	c.Run(req)
	return req
}

func (c *Client) LSet(key string, index int64, value string) *StatusReq {
	req := NewStatusReq("LSET", key, strconv.FormatInt(index, 10), value)
	c.Run(req)
	return req
}

func (c *Client) LTrim(key string, start, stop int64) *StatusReq {
	req := NewStatusReq(
		"LTRIM",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Run(req)
	return req
}

func (c *Client) RPop(key string) *BulkReq {
	req := NewBulkReq("RPOP", key)
	c.Run(req)
	return req
}

func (c *Client) RPopLPush(source, destination string) *BulkReq {
	req := NewBulkReq("RPOPLPUSH", source, destination)
	c.Run(req)
	return req
}

func (c *Client) RPush(key string, values ...string) *IntReq {
	args := append([]string{"RPUSH", key}, values...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) RPushX(key string, value string) *IntReq {
	req := NewIntReq("RPUSHX", key, value)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) SAdd(key string, members ...string) *IntReq {
	args := append([]string{"SADD", key}, members...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SCard(key string) *IntReq {
	req := NewIntReq("SCARD", key)
	c.Run(req)
	return req
}

func (c *Client) SDiff(keys ...string) *MultiBulkReq {
	args := append([]string{"SDIFF"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SDiffStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SInter(keys ...string) *MultiBulkReq {
	args := append([]string{"SINTER"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SInterStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SIsMember(key, member string) *BoolReq {
	req := NewBoolReq("SISMEMBER", key, member)
	c.Run(req)
	return req
}

func (c *Client) SMembers(key string) *MultiBulkReq {
	req := NewMultiBulkReq("SMEMBERS", key)
	c.Run(req)
	return req
}

func (c *Client) SMove(source, destination, member string) *BoolReq {
	req := NewBoolReq("SMOVE", source, destination, member)
	c.Run(req)
	return req
}

func (c *Client) SPop(key string) *BulkReq {
	req := NewBulkReq("SPOP", key)
	c.Run(req)
	return req
}

func (c *Client) SRandMember(key string) *BulkReq {
	req := NewBulkReq("SRANDMEMBER", key)
	c.Run(req)
	return req
}

func (c *Client) SRem(key string, members ...string) *IntReq {
	args := append([]string{"SREM", key}, members...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SUnion(keys ...string) *MultiBulkReq {
	args := append([]string{"SUNION"}, keys...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) SUnionStore(destination string, keys ...string) *IntReq {
	args := append([]string{"SUNIONSTORE", destination}, keys...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) PubSubClient() *PubSubClient {
	return NewPubSubClient(c.connect, c.disconnect)
}

func (c *Client) Publish(channel, message string) *IntReq {
	req := NewIntReq("PUBLISH", channel, message)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Multi() *Client {
	return NewMultiClient(c.connect, c.disconnect)
}
