package redis

import (
	"io"
	"strconv"
	"time"

	"gopkg.in/redis.v4/internal"
)

func formatInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

func formatUint(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func readTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 0
	}
	return timeout + time.Second
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) string {
	if dur > 0 && dur < time.Millisecond {
		internal.Logf(
			"specified duration is %s, but minimal supported value is %s",
			dur, time.Millisecond,
		)
	}
	return formatInt(int64(dur / time.Millisecond))
}

func formatSec(dur time.Duration) string {
	if dur > 0 && dur < time.Second {
		internal.Logf(
			"specified duration is %s, but minimal supported value is %s",
			dur, time.Second,
		)
	}
	return formatInt(int64(dur / time.Second))
}

type commandable struct {
	process func(cmd Cmder)
}

func (c *commandable) Process(cmd Cmder) {
	c.process(cmd)
}

//------------------------------------------------------------------------------

func (c *commandable) Auth(password string) *StatusCmd {
	cmd := NewStatusCmd("auth", password)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Echo(message string) *StringCmd {
	cmd := NewStringCmd("echo", message)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Ping() *StatusCmd {
	cmd := NewStatusCmd("ping")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Quit() *StatusCmd {
	panic("not implemented")
}

func (c *commandable) Select(index int64) *StatusCmd {
	cmd := NewStatusCmd("select", index)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) Del(keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "DEL"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Dump(key string) *StringCmd {
	cmd := NewStringCmd("dump", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Exists(key string) *BoolCmd {
	cmd := NewBoolCmd("exists", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Expire(key string, expiration time.Duration) *BoolCmd {
	cmd := NewBoolCmd("expire", key, formatSec(expiration))
	c.Process(cmd)
	return cmd
}

func (c *commandable) ExpireAt(key string, tm time.Time) *BoolCmd {
	cmd := NewBoolCmd("expireat", key, tm.Unix())
	c.Process(cmd)
	return cmd
}

func (c *commandable) Keys(pattern string) *StringSliceCmd {
	cmd := NewStringSliceCmd("keys", pattern)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Migrate(host, port, key string, db int64, timeout time.Duration) *StatusCmd {
	cmd := NewStatusCmd(
		"migrate",
		host,
		port,
		key,
		db,
		formatMs(timeout),
	)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) Move(key string, db int64) *BoolCmd {
	cmd := NewBoolCmd("move", key, db)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectRefCount(keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "object"
	args[1] = "refcount"
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectEncoding(keys ...string) *StringCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "object"
	args[1] = "encoding"
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewStringCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ObjectIdleTime(keys ...string) *DurationCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "object"
	args[1] = "idletime"
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewDurationCmd(time.Second, args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Persist(key string) *BoolCmd {
	cmd := NewBoolCmd("persist", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PExpire(key string, expiration time.Duration) *BoolCmd {
	cmd := NewBoolCmd("pexpire", key, formatMs(expiration))
	c.Process(cmd)
	return cmd
}

func (c *commandable) PExpireAt(key string, tm time.Time) *BoolCmd {
	cmd := NewBoolCmd(
		"pexpireat",
		key,
		tm.UnixNano()/int64(time.Millisecond),
	)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PTTL(key string) *DurationCmd {
	cmd := NewDurationCmd(time.Millisecond, "pttl", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RandomKey() *StringCmd {
	cmd := NewStringCmd("randomkey")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Rename(key, newkey string) *StatusCmd {
	cmd := NewStatusCmd("rename", key, newkey)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RenameNX(key, newkey string) *BoolCmd {
	cmd := NewBoolCmd("renamenx", key, newkey)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Restore(key string, ttl time.Duration, value string) *StatusCmd {
	cmd := NewStatusCmd(
		"restore",
		key,
		formatMs(ttl),
		value,
	)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RestoreReplace(key string, ttl time.Duration, value string) *StatusCmd {
	cmd := NewStatusCmd(
		"restore",
		key,
		formatMs(ttl),
		value,
		"replace",
	)
	c.Process(cmd)
	return cmd
}

type Sort struct {
	By            string
	Offset, Count float64
	Get           []string
	Order         string
	IsAlpha       bool
	Store         string
}

func (sort *Sort) args(key string) []interface{} {
	args := []interface{}{"sort", key}
	if sort.By != "" {
		args = append(args, "by", sort.By)
	}
	if sort.Offset != 0 || sort.Count != 0 {
		args = append(args, "limit", sort.Offset, sort.Count)
	}
	for _, get := range sort.Get {
		args = append(args, "get", get)
	}
	if sort.Order != "" {
		args = append(args, sort.Order)
	}
	if sort.IsAlpha {
		args = append(args, "alpha")
	}
	if sort.Store != "" {
		args = append(args, "store", sort.Store)
	}
	return args
}

func (c *commandable) Sort(key string, sort Sort) *StringSliceCmd {
	cmd := NewStringSliceCmd(sort.args(key)...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SortInterfaces(key string, sort Sort) *SliceCmd {
	cmd := NewSliceCmd(sort.args(key)...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) TTL(key string) *DurationCmd {
	cmd := NewDurationCmd(time.Second, "ttl", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Type(key string) *StatusCmd {
	cmd := NewStatusCmd("type", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Scan(cursor uint64, match string, count int64) Scanner {
	args := []interface{}{"scan", cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return Scanner{
		client:  c,
		ScanCmd: cmd,
	}
}

func (c *commandable) SScan(key string, cursor uint64, match string, count int64) Scanner {
	args := []interface{}{"sscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return Scanner{
		client:  c,
		ScanCmd: cmd,
	}
}

func (c *commandable) HScan(key string, cursor uint64, match string, count int64) Scanner {
	args := []interface{}{"hscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return Scanner{
		client:  c,
		ScanCmd: cmd,
	}
}

func (c *commandable) ZScan(key string, cursor uint64, match string, count int64) Scanner {
	args := []interface{}{"zscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(args...)
	c.Process(cmd)
	return Scanner{
		client:  c,
		ScanCmd: cmd,
	}
}

//------------------------------------------------------------------------------

func (c *commandable) Append(key, value string) *IntCmd {
	cmd := NewIntCmd("append", key, value)
	c.Process(cmd)
	return cmd
}

type BitCount struct {
	Start, End int64
}

func (c *commandable) BitCount(key string, bitCount *BitCount) *IntCmd {
	args := []interface{}{"bitcount", key}
	if bitCount != nil {
		args = append(
			args,
			bitCount.Start,
			bitCount.End,
		)
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) bitOp(op, destKey string, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "bitop"
	args[1] = op
	args[2] = destKey
	for i, key := range keys {
		args[3+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) BitOpAnd(destKey string, keys ...string) *IntCmd {
	return c.bitOp("and", destKey, keys...)
}

func (c *commandable) BitOpOr(destKey string, keys ...string) *IntCmd {
	return c.bitOp("or", destKey, keys...)
}

func (c *commandable) BitOpXor(destKey string, keys ...string) *IntCmd {
	return c.bitOp("xor", destKey, keys...)
}

func (c *commandable) BitOpNot(destKey string, key string) *IntCmd {
	return c.bitOp("not", destKey, key)
}

func (c *commandable) BitPos(key string, bit int64, pos ...int64) *IntCmd {
	args := make([]interface{}, 3+len(pos))
	args[0] = "bitpos"
	args[1] = key
	args[2] = bit
	switch len(pos) {
	case 0:
	case 1:
		args[3] = pos[0]
	case 2:
		args[3] = pos[0]
		args[4] = pos[1]
	default:
		panic("too many arguments")
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Decr(key string) *IntCmd {
	cmd := NewIntCmd("decr", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) DecrBy(key string, decrement int64) *IntCmd {
	cmd := NewIntCmd("decrby", key, decrement)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Get(key string) *StringCmd {
	cmd := NewStringCmd("get", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GetBit(key string, offset int64) *IntCmd {
	cmd := NewIntCmd("getbit", key, offset)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GetRange(key string, start, end int64) *StringCmd {
	cmd := NewStringCmd("getrange", key, start, end)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GetSet(key string, value interface{}) *StringCmd {
	cmd := NewStringCmd("getset", key, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) Incr(key string) *IntCmd {
	cmd := NewIntCmd("incr", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) IncrBy(key string, value int64) *IntCmd {
	cmd := NewIntCmd("incrby", key, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) IncrByFloat(key string, value float64) *FloatCmd {
	cmd := NewFloatCmd("incrbyfloat", key, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MGet(keys ...string) *SliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MSet(pairs ...string) *StatusCmd {
	args := make([]interface{}, 1+len(pairs))
	args[0] = "mset"
	for i, pair := range pairs {
		args[1+i] = pair
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) MSetNX(pairs ...string) *BoolCmd {
	args := make([]interface{}, 1+len(pairs))
	args[0] = "msetnx"
	for i, pair := range pairs {
		args[1+i] = pair
	}
	cmd := NewBoolCmd(args...)
	c.Process(cmd)
	return cmd
}

// Redis `SET key value [expiration]` command.
//
// Zero expiration means the key has no expiration time.
func (c *commandable) Set(key string, value interface{}, expiration time.Duration) *StatusCmd {
	args := make([]interface{}, 3, 4)
	args[0] = "set"
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(expiration))
		} else {
			args = append(args, "ex", formatSec(expiration))
		}
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SetBit(key string, offset int64, value int) *IntCmd {
	cmd := NewIntCmd(
		"SETBIT",
		key,
		offset,
		value,
	)
	c.Process(cmd)
	return cmd
}

// Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
func (c *commandable) SetNX(key string, value interface{}, expiration time.Duration) *BoolCmd {
	var cmd *BoolCmd
	if expiration == 0 {
		// Use old `SETNX` to support old Redis versions.
		cmd = NewBoolCmd("setnx", key, value)
	} else {
		if usePrecise(expiration) {
			cmd = NewBoolCmd("set", key, value, "px", formatMs(expiration), "nx")
		} else {
			cmd = NewBoolCmd("set", key, value, "ex", formatSec(expiration), "nx")
		}
	}
	c.Process(cmd)
	return cmd
}

// Redis `SET key value [expiration] XX` command.
//
// Zero expiration means the key has no expiration time.
func (c *commandable) SetXX(key string, value interface{}, expiration time.Duration) *BoolCmd {
	var cmd *BoolCmd
	if usePrecise(expiration) {
		cmd = NewBoolCmd("set", key, value, "px", formatMs(expiration), "xx")
	} else {
		cmd = NewBoolCmd("set", key, value, "ex", formatSec(expiration), "xx")
	}
	c.Process(cmd)
	return cmd
}

func (c *commandable) SetRange(key string, offset int64, value string) *IntCmd {
	cmd := NewIntCmd("setrange", key, offset, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) StrLen(key string) *IntCmd {
	cmd := NewIntCmd("strlen", key)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) HDel(key string, fields ...string) *IntCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hdel"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HExists(key, field string) *BoolCmd {
	cmd := NewBoolCmd("hexists", key, field)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HGet(key, field string) *StringCmd {
	cmd := NewStringCmd("hget", key, field)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HGetAll(key string) *StringStringMapCmd {
	cmd := NewStringStringMapCmd("hgetall", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HIncrBy(key, field string, incr int64) *IntCmd {
	cmd := NewIntCmd("hincrby", key, field, incr)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HIncrByFloat(key, field string, incr float64) *FloatCmd {
	cmd := NewFloatCmd("hincrbyfloat", key, field, incr)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HKeys(key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("hkeys", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HLen(key string) *IntCmd {
	cmd := NewIntCmd("hlen", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HMGet(key string, fields ...string) *SliceCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hmget"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HMSet(key string, fields map[string]string) *StatusCmd {
	args := make([]interface{}, 2+len(fields)*2)
	args[0] = "hmset"
	args[1] = key
	i := 2
	for k, v := range fields {
		args[i] = k
		args[i+1] = v
		i += 2
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HSet(key, field, value string) *BoolCmd {
	cmd := NewBoolCmd("hset", key, field, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HSetNX(key, field, value string) *BoolCmd {
	cmd := NewBoolCmd("hsetnx", key, field, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) HVals(key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("hvals", key)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) BLPop(timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "blpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(timeout)
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) BRPop(timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "brpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(keys)+1] = formatSec(timeout)
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) BRPopLPush(source, destination string, timeout time.Duration) *StringCmd {
	cmd := NewStringCmd(
		"brpoplpush",
		source,
		destination,
		formatSec(timeout),
	)
	cmd.setReadTimeout(readTimeout(timeout))
	c.Process(cmd)
	return cmd
}

func (c *commandable) LIndex(key string, index int64) *StringCmd {
	cmd := NewStringCmd("lindex", key, index)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LInsert(key, op, pivot, value string) *IntCmd {
	cmd := NewIntCmd("linsert", key, op, pivot, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LLen(key string) *IntCmd {
	cmd := NewIntCmd("llen", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LPop(key string) *StringCmd {
	cmd := NewStringCmd("lpop", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LPush(key string, values ...string) *IntCmd {
	args := make([]interface{}, 2+len(values))
	args[0] = "lpush"
	args[1] = key
	for i, value := range values {
		args[2+i] = value
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LPushX(key, value interface{}) *IntCmd {
	cmd := NewIntCmd("lpushx", key, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LRange(key string, start, stop int64) *StringSliceCmd {
	cmd := NewStringSliceCmd(
		"lrange",
		key,
		start,
		stop,
	)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LRem(key string, count int64, value interface{}) *IntCmd {
	cmd := NewIntCmd("lrem", key, count, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LSet(key string, index int64, value interface{}) *StatusCmd {
	cmd := NewStatusCmd("lset", key, index, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LTrim(key string, start, stop int64) *StatusCmd {
	cmd := NewStatusCmd(
		"ltrim",
		key,
		start,
		stop,
	)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RPop(key string) *StringCmd {
	cmd := NewStringCmd("rpop", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RPopLPush(source, destination string) *StringCmd {
	cmd := NewStringCmd("rpoplpush", source, destination)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RPush(key string, values ...string) *IntCmd {
	args := make([]interface{}, 2+len(values))
	args[0] = "rpush"
	args[1] = key
	for i, value := range values {
		args[2+i] = value
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) RPushX(key string, value interface{}) *IntCmd {
	cmd := NewIntCmd("rpushx", key, value)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) SAdd(key string, members ...string) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "sadd"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SCard(key string) *IntCmd {
	cmd := NewIntCmd("scard", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SDiff(keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sdiff"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SDiffStore(destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sdiffstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SInter(keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sinter"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SInterStore(destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sinterstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SIsMember(key string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd("sismember", key, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SMembers(key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("smembers", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SMove(source, destination string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd("smove", source, destination, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SPop(key string) *StringCmd {
	cmd := NewStringCmd("spop", key)
	c.Process(cmd)
	return cmd
}

// Redis `SRANDMEMBER key` command.
func (c *commandable) SRandMember(key string) *StringCmd {
	cmd := NewStringCmd("srandmember", key)
	c.Process(cmd)
	return cmd
}

// Redis `SRANDMEMBER key count` command.
func (c *commandable) SRandMemberN(key string, count int64) *StringSliceCmd {
	cmd := NewStringSliceCmd("srandmember", key, count)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SRem(key string, members ...string) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "srem"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SUnion(keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sunion"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SUnionStore(destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sunionstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

// Z represents sorted set member.
type Z struct {
	Score  float64
	Member interface{}
}

// ZStore is used as an arg to ZInterStore and ZUnionStore.
type ZStore struct {
	Weights []float64
	// Can be SUM, MIN or MAX.
	Aggregate string
}

func (c *commandable) zAdd(a []interface{}, n int, members ...Z) *IntCmd {
	for i, m := range members {
		a[n+2*i] = m.Score
		a[n+2*i+1] = m.Member
	}
	cmd := NewIntCmd(a...)
	c.Process(cmd)
	return cmd
}

// Redis `ZADD key score member [score member ...]` command.
func (c *commandable) ZAdd(key string, members ...Z) *IntCmd {
	const n = 2
	a := make([]interface{}, n+2*len(members))
	a[0], a[1] = "zadd", key
	return c.zAdd(a, n, members...)
}

// Redis `ZADD key NX score member [score member ...]` command.
func (c *commandable) ZAddNX(key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "nx"
	return c.zAdd(a, n, members...)
}

// Redis `ZADD key XX score member [score member ...]` command.
func (c *commandable) ZAddXX(key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "xx"
	return c.zAdd(a, n, members...)
}

// Redis `ZADD key CH score member [score member ...]` command.
func (c *commandable) ZAddCh(key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "ch"
	return c.zAdd(a, n, members...)
}

// Redis `ZADD key NX CH score member [score member ...]` command.
func (c *commandable) ZAddNXCh(key string, members ...Z) *IntCmd {
	const n = 4
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2], a[3] = "zadd", key, "nx", "ch"
	return c.zAdd(a, n, members...)
}

// Redis `ZADD key XX CH score member [score member ...]` command.
func (c *commandable) ZAddXXCh(key string, members ...Z) *IntCmd {
	const n = 4
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2], a[3] = "zadd", key, "xx", "ch"
	return c.zAdd(a, n, members...)
}

func (c *commandable) zIncr(a []interface{}, n int, members ...Z) *FloatCmd {
	for i, m := range members {
		a[n+2*i] = m.Score
		a[n+2*i+1] = m.Member
	}
	cmd := NewFloatCmd(a...)
	c.Process(cmd)
	return cmd
}

// Redis `ZADD key INCR score member` command.
func (c *commandable) ZIncr(key string, member Z) *FloatCmd {
	const n = 3
	a := make([]interface{}, n+2)
	a[0], a[1], a[2] = "zadd", key, "incr"
	return c.zIncr(a, n, member)
}

// Redis `ZADD key NX INCR score member` command.
func (c *commandable) ZIncrNX(key string, member Z) *FloatCmd {
	const n = 4
	a := make([]interface{}, n+2)
	a[0], a[1], a[2], a[3] = "zadd", key, "incr", "nx"
	return c.zIncr(a, n, member)
}

// Redis `ZADD key XX INCR score member` command.
func (c *commandable) ZIncrXX(key string, member Z) *FloatCmd {
	const n = 4
	a := make([]interface{}, n+2)
	a[0], a[1], a[2], a[3] = "zadd", key, "incr", "xx"
	return c.zIncr(a, n, member)
}

func (c *commandable) ZCard(key string) *IntCmd {
	cmd := NewIntCmd("zcard", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZCount(key, min, max string) *IntCmd {
	cmd := NewIntCmd("zcount", key, min, max)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZIncrBy(key string, increment float64, member string) *FloatCmd {
	cmd := NewFloatCmd("zincrby", key, increment, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZInterStore(destination string, store ZStore, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "zinterstore"
	args[1] = destination
	args[2] = strconv.Itoa(len(keys))
	for i, key := range keys {
		args[3+i] = key
	}
	if len(store.Weights) > 0 {
		args = append(args, "weights")
		for _, weight := range store.Weights {
			args = append(args, weight)
		}
	}
	if store.Aggregate != "" {
		args = append(args, "aggregate", store.Aggregate)
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) zRange(key string, start, stop int64, withScores bool) *StringSliceCmd {
	args := []interface{}{
		"zrange",
		key,
		start,
		stop,
	}
	if withScores {
		args = append(args, "withscores")
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRange(key string, start, stop int64) *StringSliceCmd {
	return c.zRange(key, start, stop, false)
}

func (c *commandable) ZRangeWithScores(key string, start, stop int64) *ZSliceCmd {
	cmd := NewZSliceCmd("zrange", key, start, stop, "withscores")
	c.Process(cmd)
	return cmd
}

type ZRangeBy struct {
	Min, Max      string
	Offset, Count int64
}

func (c *commandable) zRangeBy(zcmd, key string, opt ZRangeBy, withScores bool) *StringSliceCmd {
	args := []interface{}{zcmd, key, opt.Min, opt.Max}
	if withScores {
		args = append(args, "withscores")
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRangeByScore(key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRangeBy("zrangebyscore", key, opt, false)
}

func (c *commandable) ZRangeByLex(key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRangeBy("zrangebylex", key, opt, false)
}

func (c *commandable) ZRangeByScoreWithScores(key string, opt ZRangeBy) *ZSliceCmd {
	args := []interface{}{"zrangebyscore", key, opt.Min, opt.Max, "withscores"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewZSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRank(key, member string) *IntCmd {
	cmd := NewIntCmd("zrank", key, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRem(key string, members ...string) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "zrem"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRemRangeByRank(key string, start, stop int64) *IntCmd {
	cmd := NewIntCmd(
		"zremrangebyrank",
		key,
		start,
		stop,
	)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRemRangeByScore(key, min, max string) *IntCmd {
	cmd := NewIntCmd("zremrangebyscore", key, min, max)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRevRange(key string, start, stop int64) *StringSliceCmd {
	cmd := NewStringSliceCmd("zrevrange", key, start, stop)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRevRangeWithScores(key string, start, stop int64) *ZSliceCmd {
	cmd := NewZSliceCmd("zrevrange", key, start, stop, "withscores")
	c.Process(cmd)
	return cmd
}

func (c *commandable) zRevRangeBy(zcmd, key string, opt ZRangeBy) *StringSliceCmd {
	args := []interface{}{zcmd, key, opt.Max, opt.Min}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRevRangeByScore(key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRevRangeBy("zrevrangebyscore", key, opt)
}

func (c *commandable) ZRevRangeByLex(key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRevRangeBy("zrevrangebylex", key, opt)
}

func (c *commandable) ZRevRangeByScoreWithScores(key string, opt ZRangeBy) *ZSliceCmd {
	args := []interface{}{"zrevrangebyscore", key, opt.Max, opt.Min, "withscores"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewZSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZRevRank(key, member string) *IntCmd {
	cmd := NewIntCmd("zrevrank", key, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZScore(key, member string) *FloatCmd {
	cmd := NewFloatCmd("zscore", key, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ZUnionStore(dest string, store ZStore, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "zunionstore"
	args[1] = dest
	args[2] = strconv.Itoa(len(keys))
	for i, key := range keys {
		args[3+i] = key
	}
	if len(store.Weights) > 0 {
		args = append(args, "weights")
		for _, weight := range store.Weights {
			args = append(args, weight)
		}
	}
	if store.Aggregate != "" {
		args = append(args, "aggregate", store.Aggregate)
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) PFAdd(key string, fields ...string) *IntCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "pfadd"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PFCount(keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "pfcount"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PFMerge(dest string, keys ...string) *StatusCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "pfmerge"
	args[1] = dest
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) BgRewriteAOF() *StatusCmd {
	cmd := NewStatusCmd("bgrewriteaof")
	c.Process(cmd)
	return cmd
}

func (c *commandable) BgSave() *StatusCmd {
	cmd := NewStatusCmd("bgsave")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientKill(ipPort string) *StatusCmd {
	cmd := NewStatusCmd("client", "kill", ipPort)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientList() *StringCmd {
	cmd := NewStringCmd("client", "list")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClientPause(dur time.Duration) *BoolCmd {
	cmd := NewBoolCmd("client", "pause", formatMs(dur))
	c.Process(cmd)
	return cmd
}

// ClientSetName assigns a name to the one of many connections in the pool.
func (c *commandable) ClientSetName(name string) *BoolCmd {
	cmd := NewBoolCmd("client", "setname", name)
	c.Process(cmd)
	return cmd
}

// ClientGetName returns the name of the one of many connections in the pool.
func (c *Client) ClientGetName() *StringCmd {
	cmd := NewStringCmd("client", "getname")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigGet(parameter string) *SliceCmd {
	cmd := NewSliceCmd("config", "get", parameter)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigResetStat() *StatusCmd {
	cmd := NewStatusCmd("config", "resetstat")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ConfigSet(parameter, value string) *StatusCmd {
	cmd := NewStatusCmd("config", "set", parameter, value)
	c.Process(cmd)
	return cmd
}

func (c *commandable) DbSize() *IntCmd {
	cmd := NewIntCmd("dbsize")
	c.Process(cmd)
	return cmd
}

func (c *commandable) FlushAll() *StatusCmd {
	cmd := NewStatusCmd("flushall")
	c.Process(cmd)
	return cmd
}

func (c *commandable) FlushDb() *StatusCmd {
	cmd := NewStatusCmd("flushdb")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Info(section ...string) *StringCmd {
	args := []interface{}{"info"}
	if len(section) > 0 {
		args = append(args, section[0])
	}
	cmd := NewStringCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) LastSave() *IntCmd {
	cmd := NewIntCmd("lastsave")
	c.Process(cmd)
	return cmd
}

func (c *commandable) Save() *StatusCmd {
	cmd := NewStatusCmd("save")
	c.Process(cmd)
	return cmd
}

func (c *commandable) shutdown(modifier string) *StatusCmd {
	var args []interface{}
	if modifier == "" {
		args = []interface{}{"shutdown"}
	} else {
		args = []interface{}{"shutdown", modifier}
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	if err := cmd.Err(); err != nil {
		if err == io.EOF {
			// Server quit as expected.
			cmd.err = nil
		}
	} else {
		// Server did not quit. String reply contains the reason.
		cmd.err = errorf(cmd.val)
		cmd.val = ""
	}
	return cmd
}

func (c *commandable) Shutdown() *StatusCmd {
	return c.shutdown("")
}

func (c *commandable) ShutdownSave() *StatusCmd {
	return c.shutdown("save")
}

func (c *commandable) ShutdownNoSave() *StatusCmd {
	return c.shutdown("nosave")
}

func (c *commandable) SlaveOf(host, port string) *StatusCmd {
	cmd := NewStatusCmd("slaveof", host, port)
	c.Process(cmd)
	return cmd
}

func (c *commandable) SlowLog() {
	panic("not implemented")
}

func (c *commandable) Sync() {
	panic("not implemented")
}

func (c *commandable) Time() *StringSliceCmd {
	cmd := NewStringSliceCmd("time")
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) Eval(script string, keys []string, args ...interface{}) *Cmd {
	cmdArgs := make([]interface{}, 3+len(keys)+len(args))
	cmdArgs[0] = "eval"
	cmdArgs[1] = script
	cmdArgs[2] = strconv.Itoa(len(keys))
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	pos := 3 + len(keys)
	for i, arg := range args {
		cmdArgs[pos+i] = arg
	}
	cmd := NewCmd(cmdArgs...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) EvalSha(sha1 string, keys []string, args ...interface{}) *Cmd {
	cmdArgs := make([]interface{}, 3+len(keys)+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = sha1
	cmdArgs[2] = strconv.Itoa(len(keys))
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	pos := 3 + len(keys)
	for i, arg := range args {
		cmdArgs[pos+i] = arg
	}
	cmd := NewCmd(cmdArgs...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptExists(scripts ...string) *BoolSliceCmd {
	args := make([]interface{}, 2+len(scripts))
	args[0] = "script"
	args[1] = "exists"
	for i, script := range scripts {
		args[2+i] = script
	}
	cmd := NewBoolSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptFlush() *StatusCmd {
	cmd := NewStatusCmd("script", "flush")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptKill() *StatusCmd {
	cmd := NewStatusCmd("script", "kill")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ScriptLoad(script string) *StringCmd {
	cmd := NewStringCmd("script", "load", script)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) DebugObject(key string) *StringCmd {
	cmd := NewStringCmd("debug", "object", key)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) PubSubChannels(pattern string) *StringSliceCmd {
	args := []interface{}{"pubsub", "channels"}
	if pattern != "*" {
		args = append(args, pattern)
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PubSubNumSub(channels ...string) *StringIntMapCmd {
	args := make([]interface{}, 2+len(channels))
	args[0] = "pubsub"
	args[1] = "numsub"
	for i, channel := range channels {
		args[2+i] = channel
	}
	cmd := NewStringIntMapCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) PubSubNumPat() *IntCmd {
	cmd := NewIntCmd("pubsub", "numpat")
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) ClusterSlots() *ClusterSlotsCmd {
	cmd := NewClusterSlotsCmd("cluster", "slots")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterNodes() *StringCmd {
	cmd := NewStringCmd("cluster", "nodes")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterMeet(host, port string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "meet", host, port)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterForget(nodeID string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "forget", nodeID)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterReplicate(nodeID string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "replicate", nodeID)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterResetSoft() *StatusCmd {
	cmd := NewStatusCmd("cluster", "reset", "soft")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterResetHard() *StatusCmd {
	cmd := NewStatusCmd("cluster", "reset", "hard")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterInfo() *StringCmd {
	cmd := NewStringCmd("cluster", "info")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterKeySlot(key string) *IntCmd {
	cmd := NewIntCmd("cluster", "keyslot", key)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterCountFailureReports(nodeID string) *IntCmd {
	cmd := NewIntCmd("cluster", "count-failure-reports", nodeID)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterCountKeysInSlot(slot int) *IntCmd {
	cmd := NewIntCmd("cluster", "countkeysinslot", slot)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterDelSlots(slots ...int) *StatusCmd {
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "delslots"
	for i, slot := range slots {
		args[2+i] = slot
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterDelSlotsRange(min, max int) *StatusCmd {
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterDelSlots(slots...)
}

func (c *commandable) ClusterSaveConfig() *StatusCmd {
	cmd := NewStatusCmd("cluster", "saveconfig")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterSlaves(nodeID string) *StringSliceCmd {
	cmd := NewStringSliceCmd("cluster", "slaves", nodeID)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ReadOnly() *StatusCmd {
	cmd := NewStatusCmd("readonly")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ReadWrite() *StatusCmd {
	cmd := NewStatusCmd("readwrite")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterFailover() *StatusCmd {
	cmd := NewStatusCmd("cluster", "failover")
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterAddSlots(slots ...int) *StatusCmd {
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "addslots"
	for i, num := range slots {
		args[2+i] = strconv.Itoa(num)
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) ClusterAddSlotsRange(min, max int) *StatusCmd {
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterAddSlots(slots...)
}

//------------------------------------------------------------------------------

func (c *commandable) GeoAdd(key string, geoLocation ...*GeoLocation) *IntCmd {
	args := make([]interface{}, 2+3*len(geoLocation))
	args[0] = "geoadd"
	args[1] = key
	for i, eachLoc := range geoLocation {
		args[2+3*i] = eachLoc.Longitude
		args[2+3*i+1] = eachLoc.Latitude
		args[2+3*i+2] = eachLoc.Name
	}
	cmd := NewIntCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GeoRadius(key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadius", key, longitude, latitude)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GeoRadiusByMember(key, member string, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadiusbymember", key, member)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GeoDist(key string, member1, member2, unit string) *FloatCmd {
	if unit == "" {
		unit = "km"
	}
	cmd := NewFloatCmd("geodist", key, member1, member2, unit)
	c.Process(cmd)
	return cmd
}

func (c *commandable) GeoHash(key string, members ...string) *StringSliceCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "geohash"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewStringSliceCmd(args...)
	c.Process(cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *commandable) Command() *CommandsInfoCmd {
	cmd := NewCommandsInfoCmd("command")
	c.Process(cmd)
	return cmd
}
