package redis

import (
	"context"
	"io"
	"time"

	"github.com/kirk91/redis/internal"
)

func readTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 0
	}
	return timeout + 10*time.Second
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		internal.Logf(
			"specified duration is %s, but minimal supported value is %s",
			dur, time.Millisecond,
		)
	}
	return int64(dur / time.Millisecond)
}

func formatSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		internal.Logf(
			"specified duration is %s, but minimal supported value is %s",
			dur, time.Second,
		)
	}
	return int64(dur / time.Second)
}

type Cmdable interface {
	Pipeline() Pipeliner
	Pipelined(fn func(Pipeliner) error) ([]Cmder, error)

	TxPipelined(fn func(Pipeliner) error) ([]Cmder, error)
	TxPipeline() Pipeliner

	ClientGetName(ctx context.Context) *StringCmd
	Echo(ctx context.Context, message interface{}) *StringCmd
	Ping(ctx context.Context) *StatusCmd
	Quit(ctx context.Context) *StatusCmd
	Del(ctx context.Context, keys ...string) *IntCmd
	Unlink(ctx context.Context, keys ...string) *IntCmd
	Dump(ctx context.Context, key string) *StringCmd
	Exists(ctx context.Context, keys ...string) *IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd
	ExpireAt(ctx context.Context, key string, tm time.Time) *BoolCmd
	Keys(ctx context.Context, pattern string) *StringSliceCmd
	Migrate(ctx context.Context, host, port, key string, db int64, timeout time.Duration) *StatusCmd
	Move(ctx context.Context, key string, db int64) *BoolCmd
	ObjectRefCount(ctx context.Context, key string) *IntCmd
	ObjectEncoding(ctx context.Context, key string) *StringCmd
	ObjectIdleTime(ctx context.Context, key string) *DurationCmd
	Persist(ctx context.Context, key string) *BoolCmd
	PExpire(ctx context.Context, key string, expiration time.Duration) *BoolCmd
	PExpireAt(ctx context.Context, key string, tm time.Time) *BoolCmd
	PTTL(ctx context.Context, key string) *DurationCmd
	RandomKey(ctx context.Context) *StringCmd
	Rename(ctx context.Context, key, newkey string) *StatusCmd
	RenameNX(ctx context.Context, key, newkey string) *BoolCmd
	Restore(ctx context.Context, key string, ttl time.Duration, value string) *StatusCmd
	RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *StatusCmd
	Sort(ctx context.Context, key string, sort Sort) *StringSliceCmd
	SortInterfaces(ctx context.Context, key string, sort Sort) *SliceCmd
	TTL(ctx context.Context, key string) *DurationCmd
	Type(ctx context.Context, key string) *StatusCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd
	SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd
	HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd
	ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd
	Append(ctx context.Context, key, value string) *IntCmd
	BitCount(ctx context.Context, key string, bitCount *BitCount) *IntCmd
	BitOpAnd(ctx context.Context, destKey string, keys ...string) *IntCmd
	BitOpOr(ctx context.Context, destKey string, keys ...string) *IntCmd
	BitOpXor(ctx context.Context, destKey string, keys ...string) *IntCmd
	BitOpNot(ctx context.Context, destKey string, key string) *IntCmd
	BitPos(ctx context.Context, key string, bit int64, pos ...int64) *IntCmd
	Decr(ctx context.Context, key string) *IntCmd
	DecrBy(ctx context.Context, key string, decrement int64) *IntCmd
	Get(ctx context.Context, key string) *StringCmd
	GetBit(ctx context.Context, key string, offset int64) *IntCmd
	GetRange(ctx context.Context, key string, start, end int64) *StringCmd
	GetSet(ctx context.Context, key string, value interface{}) *StringCmd
	Incr(ctx context.Context, key string) *IntCmd
	IncrBy(ctx context.Context, key string, value int64) *IntCmd
	IncrByFloat(ctx context.Context, key string, value float64) *FloatCmd
	MGet(ctx context.Context, keys ...string) *SliceCmd
	MSet(ctx context.Context, pairs ...interface{}) *StatusCmd
	MSetNX(ctx context.Context, pairs ...interface{}) *BoolCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *StatusCmd
	SetBit(ctx context.Context, key string, offset int64, value int) *IntCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd
	SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd
	SetRange(ctx context.Context, key string, offset int64, value string) *IntCmd
	StrLen(ctx context.Context, key string) *IntCmd
	HDel(ctx context.Context, key string, fields ...string) *IntCmd
	HExists(ctx context.Context, key, field string) *BoolCmd
	HGet(ctx context.Context, key, field string) *StringCmd
	HGetAll(ctx context.Context, key string) *StringStringMapCmd
	HIncrBy(ctx context.Context, key, field string, incr int64) *IntCmd
	HIncrByFloat(ctx context.Context, key, field string, incr float64) *FloatCmd
	HKeys(ctx context.Context, key string) *StringSliceCmd
	HLen(ctx context.Context, key string) *IntCmd
	HMGet(ctx context.Context, key string, fields ...string) *SliceCmd
	HMSet(ctx context.Context, key string, fields map[string]interface{}) *StatusCmd
	HSet(ctx context.Context, key, field string, value interface{}) *BoolCmd
	HSetNX(ctx context.Context, key, field string, value interface{}) *BoolCmd
	HVals(ctx context.Context, key string) *StringSliceCmd
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd
	BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *StringCmd
	LIndex(ctx context.Context, key string, index int64) *StringCmd
	LInsert(ctx context.Context, key, op string, pivot, value interface{}) *IntCmd
	LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *IntCmd
	LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *IntCmd
	LLen(ctx context.Context, key string) *IntCmd
	LPop(ctx context.Context, key string) *StringCmd
	LPush(ctx context.Context, key string, values ...interface{}) *IntCmd
	LPushX(ctx context.Context, key string, value interface{}) *IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value interface{}) *IntCmd
	LSet(ctx context.Context, key string, index int64, value interface{}) *StatusCmd
	LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd
	RPop(ctx context.Context, key string) *StringCmd
	RPopLPush(ctx context.Context, source, destination string) *StringCmd
	RPush(ctx context.Context, key string, values ...interface{}) *IntCmd
	RPushX(ctx context.Context, key string, value interface{}) *IntCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *IntCmd
	SCard(ctx context.Context, key string) *IntCmd
	SDiff(ctx context.Context, keys ...string) *StringSliceCmd
	SDiffStore(ctx context.Context, destination string, keys ...string) *IntCmd
	SInter(ctx context.Context, keys ...string) *StringSliceCmd
	SInterStore(ctx context.Context, destination string, keys ...string) *IntCmd
	SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd
	SMembers(ctx context.Context, key string) *StringSliceCmd
	SMove(ctx context.Context, source, destination string, member interface{}) *BoolCmd
	SPop(ctx context.Context, key string) *StringCmd
	SPopN(ctx context.Context, key string, count int64) *StringSliceCmd
	SRandMember(ctx context.Context, key string) *StringCmd
	SRandMemberN(ctx context.Context, key string, count int64) *StringSliceCmd
	SRem(ctx context.Context, key string, members ...interface{}) *IntCmd
	SUnion(ctx context.Context, keys ...string) *StringSliceCmd
	SUnionStore(ctx context.Context, destination string, keys ...string) *IntCmd
	ZAdd(ctx context.Context, key string, members ...Z) *IntCmd
	ZAddNX(ctx context.Context, key string, members ...Z) *IntCmd
	ZAddXX(ctx context.Context, key string, members ...Z) *IntCmd
	ZAddCh(ctx context.Context, key string, members ...Z) *IntCmd
	ZAddNXCh(ctx context.Context, key string, members ...Z) *IntCmd
	ZAddXXCh(ctx context.Context, key string, members ...Z) *IntCmd
	ZIncr(ctx context.Context, key string, member Z) *FloatCmd
	ZIncrNX(ctx context.Context, key string, member Z) *FloatCmd
	ZIncrXX(ctx context.Context, key string, member Z) *FloatCmd
	ZCard(ctx context.Context, key string) *IntCmd
	ZCount(ctx context.Context, key, min, max string) *IntCmd
	ZLexCount(ctx context.Context, key, min, max string) *IntCmd
	ZIncrBy(ctx context.Context, key string, increment float64, member string) *FloatCmd
	ZInterStore(ctx context.Context, destination string, store ZStore, keys ...string) *IntCmd
	ZRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) *ZSliceCmd
	ZRangeByScore(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd
	ZRangeByLex(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd
	ZRangeByScoreWithScores(ctx context.Context, key string, opt ZRangeBy) *ZSliceCmd
	ZRank(ctx context.Context, key, member string) *IntCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *IntCmd
	ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *IntCmd
	ZRemRangeByScore(ctx context.Context, key, min, max string) *IntCmd
	ZRemRangeByLex(ctx context.Context, key, min, max string) *IntCmd
	ZRevRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd
	ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *ZSliceCmd
	ZRevRangeByScore(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd
	ZRevRangeByLex(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd
	ZRevRangeByScoreWithScores(ctx context.Context, key string, opt ZRangeBy) *ZSliceCmd
	ZRevRank(ctx context.Context, key, member string) *IntCmd
	ZScore(ctx context.Context, key, member string) *FloatCmd
	ZUnionStore(ctx context.Context, dest string, store ZStore, keys ...string) *IntCmd
	PFAdd(ctx context.Context, key string, els ...interface{}) *IntCmd
	PFCount(ctx context.Context, keys ...string) *IntCmd
	PFMerge(ctx context.Context, dest string, keys ...string) *StatusCmd
	BgRewriteAOF(ctx context.Context) *StatusCmd
	BgSave(ctx context.Context) *StatusCmd
	ClientKill(ctx context.Context, ipPort string) *StatusCmd
	ClientList(ctx context.Context) *StringCmd
	ClientPause(ctx context.Context, dur time.Duration) *BoolCmd
	ConfigGet(ctx context.Context, parameter string) *SliceCmd
	ConfigResetStat(ctx context.Context) *StatusCmd
	ConfigSet(ctx context.Context, parameter, value string) *StatusCmd
	DBSize(ctx context.Context) *IntCmd
	FlushAll(ctx context.Context) *StatusCmd
	FlushAllAsync(ctx context.Context) *StatusCmd
	FlushDB(ctx context.Context) *StatusCmd
	FlushDBAsync(ctx context.Context) *StatusCmd
	Info(ctx context.Context, section ...string) *StringCmd
	LastSave(ctx context.Context) *IntCmd
	Save(ctx context.Context) *StatusCmd
	Shutdown(ctx context.Context) *StatusCmd
	ShutdownSave(ctx context.Context) *StatusCmd
	ShutdownNoSave(ctx context.Context) *StatusCmd
	SlaveOf(ctx context.Context, host, port string) *StatusCmd
	Time(ctx context.Context) *TimeCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd
	ScriptExists(ctx context.Context, scripts ...string) *BoolSliceCmd
	ScriptFlush(ctx context.Context) *StatusCmd
	ScriptKill(ctx context.Context) *StatusCmd
	ScriptLoad(ctx context.Context, script string) *StringCmd
	DebugObject(ctx context.Context, key string) *StringCmd
	Publish(ctx context.Context, channel string, message interface{}) *IntCmd
	PubSubChannels(ctx context.Context, pattern string) *StringSliceCmd
	PubSubNumSub(ctx context.Context, channels ...string) *StringIntMapCmd
	PubSubNumPat(ctx context.Context) *IntCmd
	ClusterSlots(ctx context.Context) *ClusterSlotsCmd
	ClusterNodes(ctx context.Context) *StringCmd
	ClusterMeet(ctx context.Context, host, port string) *StatusCmd
	ClusterForget(ctx context.Context, nodeID string) *StatusCmd
	ClusterReplicate(ctx context.Context, nodeID string) *StatusCmd
	ClusterResetSoft(ctx context.Context) *StatusCmd
	ClusterResetHard(ctx context.Context) *StatusCmd
	ClusterInfo(ctx context.Context) *StringCmd
	ClusterKeySlot(ctx context.Context, key string) *IntCmd
	ClusterCountFailureReports(ctx context.Context, nodeID string) *IntCmd
	ClusterCountKeysInSlot(ctx context.Context, slot int) *IntCmd
	ClusterDelSlots(ctx context.Context, slots ...int) *StatusCmd
	ClusterDelSlotsRange(ctx context.Context, min, max int) *StatusCmd
	ClusterSaveConfig(ctx context.Context) *StatusCmd
	ClusterSlaves(ctx context.Context, nodeID string) *StringSliceCmd
	ClusterFailover(ctx context.Context) *StatusCmd
	ClusterAddSlots(ctx context.Context, slots ...int) *StatusCmd
	ClusterAddSlotsRange(ctx context.Context, min, max int) *StatusCmd
	GeoAdd(ctx context.Context, key string, geoLocation ...*GeoLocation) *IntCmd
	GeoPos(ctx context.Context, key string, members ...string) *GeoPosCmd
	GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusRO(ctx context.Context, key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusByMember(ctx context.Context, key, member string, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusByMemberRO(ctx context.Context, key, member string, query *GeoRadiusQuery) *GeoLocationCmd
	GeoDist(ctx context.Context, key string, member1, member2, unit string) *FloatCmd
	GeoHash(ctx context.Context, key string, members ...string) *StringSliceCmd
	Command(ctx context.Context) *CommandsInfoCmd
}

type StatefulCmdable interface {
	Cmdable
	Auth(ctx context.Context, password string) *StatusCmd
	Select(ctx context.Context, index int) *StatusCmd
	ClientSetName(ctx context.Context, name string) *BoolCmd
	ReadOnly(ctx context.Context) *StatusCmd
	ReadWrite(cxt context.Context) *StatusCmd
}

var _ Cmdable = (*Client)(nil)
var _ Cmdable = (*Tx)(nil)
var _ Cmdable = (*Ring)(nil)
var _ Cmdable = (*ClusterClient)(nil)

type cmdable struct {
	process func(ctx context.Context, cmd Cmder) error
}

func (c *cmdable) setProcessor(fn func(context.Context, Cmder) error) {
	c.process = fn
}

type statefulCmdable struct {
	cmdable
	process func(ctx context.Context, cmd Cmder) error
}

func (c *statefulCmdable) setProcessor(fn func(context.Context, Cmder) error) {
	c.process = fn
	c.cmdable.setProcessor(fn)
}

//------------------------------------------------------------------------------

func (c *statefulCmdable) Auth(ctx context.Context, password string) *StatusCmd {
	cmd := NewStatusCmd("auth", password)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Echo(ctx context.Context, message interface{}) *StringCmd {
	cmd := NewStringCmd("echo", message)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Ping(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("ping")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Wait(ctx context.Context, numSlaves int, timeout time.Duration) *IntCmd {
	cmd := NewIntCmd("wait", numSlaves, int(timeout/time.Millisecond))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Quit(ctx context.Context) *StatusCmd {
	panic("not implemented")
}

func (c *statefulCmdable) Select(ctx context.Context, index int) *StatusCmd {
	cmd := NewStatusCmd("select", index)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) Del(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "del"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Unlink(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "unlink"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Dump(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("dump", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Exists(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "exists"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	cmd := NewBoolCmd("expire", key, formatSec(expiration))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ExpireAt(ctx context.Context, key string, tm time.Time) *BoolCmd {
	cmd := NewBoolCmd("expireat", key, tm.Unix())
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Keys(ctx context.Context, pattern string) *StringSliceCmd {
	cmd := NewStringSliceCmd("keys", pattern)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Migrate(ctx context.Context, host, port, key string, db int64, timeout time.Duration) *StatusCmd {
	cmd := NewStatusCmd(
		"migrate",
		host,
		port,
		key,
		db,
		formatMs(timeout),
	)
	cmd.setReadTimeout(readTimeout(timeout))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Move(ctx context.Context, key string, db int64) *BoolCmd {
	cmd := NewBoolCmd("move", key, db)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ObjectRefCount(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("object", "refcount", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ObjectEncoding(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("object", "encoding", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ObjectIdleTime(ctx context.Context, key string) *DurationCmd {
	cmd := NewDurationCmd(time.Second, "object", "idletime", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Persist(ctx context.Context, key string) *BoolCmd {
	cmd := NewBoolCmd("persist", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PExpire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	cmd := NewBoolCmd("pexpire", key, formatMs(expiration))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PExpireAt(ctx context.Context, key string, tm time.Time) *BoolCmd {
	cmd := NewBoolCmd(
		"pexpireat",
		key,
		tm.UnixNano()/int64(time.Millisecond),
	)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PTTL(ctx context.Context, key string) *DurationCmd {
	cmd := NewDurationCmd(time.Millisecond, "pttl", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RandomKey(ctx context.Context) *StringCmd {
	cmd := NewStringCmd("randomkey")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Rename(ctx context.Context, key, newkey string) *StatusCmd {
	cmd := NewStatusCmd("rename", key, newkey)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RenameNX(ctx context.Context, key, newkey string) *BoolCmd {
	cmd := NewBoolCmd("renamenx", key, newkey)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Restore(ctx context.Context, key string, ttl time.Duration, value string) *StatusCmd {
	cmd := NewStatusCmd(
		"restore",
		key,
		formatMs(ttl),
		value,
	)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *StatusCmd {
	cmd := NewStatusCmd(
		"restore",
		key,
		formatMs(ttl),
		value,
		"replace",
	)
	c.process(ctx, cmd)
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

func (c *cmdable) Sort(ctx context.Context, key string, sort Sort) *StringSliceCmd {
	cmd := NewStringSliceCmd(sort.args(key)...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SortInterfaces(ctx context.Context, key string, sort Sort) *SliceCmd {
	cmd := NewSliceCmd(sort.args(key)...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) TTL(ctx context.Context, key string) *DurationCmd {
	cmd := NewDurationCmd(time.Second, "ttl", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Type(ctx context.Context, key string) *StatusCmd {
	cmd := NewStatusCmd("type", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"scan", cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(c.process, args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"sscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(c.process, args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"hscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(c.process, args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"zscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(c.process, args...)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) Append(ctx context.Context, key, value string) *IntCmd {
	cmd := NewIntCmd("append", key, value)
	c.process(ctx, cmd)
	return cmd
}

type BitCount struct {
	Start, End int64
}

func (c *cmdable) BitCount(ctx context.Context, key string, bitCount *BitCount) *IntCmd {
	args := []interface{}{"bitcount", key}
	if bitCount != nil {
		args = append(
			args,
			bitCount.Start,
			bitCount.End,
		)
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) bitOp(ctx context.Context, op, destKey string, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "bitop"
	args[1] = op
	args[2] = destKey
	for i, key := range keys {
		args[3+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) BitOpAnd(ctx context.Context, destKey string, keys ...string) *IntCmd {
	return c.bitOp(ctx, "and", destKey, keys...)
}

func (c *cmdable) BitOpOr(ctx context.Context, destKey string, keys ...string) *IntCmd {
	return c.bitOp(ctx, "or", destKey, keys...)
}

func (c *cmdable) BitOpXor(ctx context.Context, destKey string, keys ...string) *IntCmd {
	return c.bitOp(ctx, "xor", destKey, keys...)
}

func (c *cmdable) BitOpNot(ctx context.Context, destKey string, key string) *IntCmd {
	return c.bitOp(ctx, "not", destKey, key)
}

func (c *cmdable) BitPos(ctx context.Context, key string, bit int64, pos ...int64) *IntCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Decr(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("decr", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) DecrBy(ctx context.Context, key string, decrement int64) *IntCmd {
	cmd := NewIntCmd("decrby", key, decrement)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Get(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("get", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GetBit(ctx context.Context, key string, offset int64) *IntCmd {
	cmd := NewIntCmd("getbit", key, offset)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GetRange(ctx context.Context, key string, start, end int64) *StringCmd {
	cmd := NewStringCmd("getrange", key, start, end)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GetSet(ctx context.Context, key string, value interface{}) *StringCmd {
	cmd := NewStringCmd("getset", key, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Incr(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("incr", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) IncrBy(ctx context.Context, key string, value int64) *IntCmd {
	cmd := NewIntCmd("incrby", key, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) IncrByFloat(ctx context.Context, key string, value float64) *FloatCmd {
	cmd := NewFloatCmd("incrbyfloat", key, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) MGet(ctx context.Context, keys ...string) *SliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) MSet(ctx context.Context, pairs ...interface{}) *StatusCmd {
	args := make([]interface{}, 1+len(pairs))
	args[0] = "mset"
	for i, pair := range pairs {
		args[1+i] = pair
	}
	cmd := NewStatusCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) MSetNX(ctx context.Context, pairs ...interface{}) *BoolCmd {
	args := make([]interface{}, 1+len(pairs))
	args[0] = "msetnx"
	for i, pair := range pairs {
		args[1+i] = pair
	}
	cmd := NewBoolCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SET key value [expiration]` command.
//
// Use expiration for `SETEX`-like behavior.
// Zero expiration means the key has no expiration time.
func (c *cmdable) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *StatusCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SetBit(ctx context.Context, key string, offset int64, value int) *IntCmd {
	cmd := NewIntCmd(
		"setbit",
		key,
		offset,
		value,
	)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
func (c *cmdable) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd {
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
	c.process(ctx, cmd)
	return cmd
}

// Redis `SET key value [expiration] XX` command.
//
// Zero expiration means the key has no expiration time.
func (c *cmdable) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd {
	var cmd *BoolCmd
	if expiration == 0 {
		cmd = NewBoolCmd("set", key, value, "xx")
	} else {
		if usePrecise(expiration) {
			cmd = NewBoolCmd("set", key, value, "px", formatMs(expiration), "xx")
		} else {
			cmd = NewBoolCmd("set", key, value, "ex", formatSec(expiration), "xx")
		}
	}
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SetRange(ctx context.Context, key string, offset int64, value string) *IntCmd {
	cmd := NewIntCmd("setrange", key, offset, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) StrLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("strlen", key)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) HDel(ctx context.Context, key string, fields ...string) *IntCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hdel"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HExists(ctx context.Context, key, field string) *BoolCmd {
	cmd := NewBoolCmd("hexists", key, field)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HGet(ctx context.Context, key, field string) *StringCmd {
	cmd := NewStringCmd("hget", key, field)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HGetAll(ctx context.Context, key string) *StringStringMapCmd {
	cmd := NewStringStringMapCmd("hgetall", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HIncrBy(ctx context.Context, key, field string, incr int64) *IntCmd {
	cmd := NewIntCmd("hincrby", key, field, incr)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HIncrByFloat(ctx context.Context, key, field string, incr float64) *FloatCmd {
	cmd := NewFloatCmd("hincrbyfloat", key, field, incr)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HKeys(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("hkeys", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("hlen", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HMGet(ctx context.Context, key string, fields ...string) *SliceCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hmget"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HMSet(ctx context.Context, key string, fields map[string]interface{}) *StatusCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HSet(ctx context.Context, key, field string, value interface{}) *BoolCmd {
	cmd := NewBoolCmd("hset", key, field, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HSetNX(ctx context.Context, key, field string, value interface{}) *BoolCmd {
	cmd := NewBoolCmd("hsetnx", key, field, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) HVals(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("hvals", key)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "blpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(timeout)
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "brpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(keys)+1] = formatSec(timeout)
	cmd := NewStringSliceCmd(args...)
	cmd.setReadTimeout(readTimeout(timeout))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *StringCmd {
	cmd := NewStringCmd(
		"brpoplpush",
		source,
		destination,
		formatSec(timeout),
	)
	cmd.setReadTimeout(readTimeout(timeout))
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LIndex(ctx context.Context, key string, index int64) *StringCmd {
	cmd := NewStringCmd("lindex", key, index)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LInsert(ctx context.Context, key, op string, pivot, value interface{}) *IntCmd {
	cmd := NewIntCmd("linsert", key, op, pivot, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *IntCmd {
	cmd := NewIntCmd("linsert", key, "before", pivot, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *IntCmd {
	cmd := NewIntCmd("linsert", key, "after", pivot, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("llen", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("lpop", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LPush(ctx context.Context, key string, values ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(values))
	args[0] = "lpush"
	args[1] = key
	for i, value := range values {
		args[2+i] = value
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LPushX(ctx context.Context, key string, value interface{}) *IntCmd {
	cmd := NewIntCmd("lpushx", key, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	cmd := NewStringSliceCmd(
		"lrange",
		key,
		start,
		stop,
	)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LRem(ctx context.Context, key string, count int64, value interface{}) *IntCmd {
	cmd := NewIntCmd("lrem", key, count, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LSet(ctx context.Context, key string, index int64, value interface{}) *StatusCmd {
	cmd := NewStatusCmd("lset", key, index, value)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd {
	cmd := NewStatusCmd(
		"ltrim",
		key,
		start,
		stop,
	)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("rpop", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RPopLPush(ctx context.Context, source, destination string) *StringCmd {
	cmd := NewStringCmd("rpoplpush", source, destination)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RPush(ctx context.Context, key string, values ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(values))
	args[0] = "rpush"
	args[1] = key
	for i, value := range values {
		args[2+i] = value
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) RPushX(ctx context.Context, key string, value interface{}) *IntCmd {
	cmd := NewIntCmd("rpushx", key, value)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) SAdd(ctx context.Context, key string, members ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "sadd"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SCard(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("scard", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SDiff(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sdiff"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SDiffStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sdiffstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SInter(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sinter"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SInterStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sinterstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd("sismember", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SMembers(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd("smembers", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SMove(ctx context.Context, source, destination string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd("smove", source, destination, member)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SPOP key` command.
func (c *cmdable) SPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("spop", key)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SPOP key count` command.
func (c *cmdable) SPopN(ctx context.Context, key string, count int64) *StringSliceCmd {
	cmd := NewStringSliceCmd("spop", key, count)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SRANDMEMBER key` command.
func (c *cmdable) SRandMember(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("srandmember", key)
	c.process(ctx, cmd)
	return cmd
}

// Redis `SRANDMEMBER key count` command.
func (c *cmdable) SRandMemberN(ctx context.Context, key string, count int64) *StringSliceCmd {
	cmd := NewStringSliceCmd("srandmember", key, count)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SRem(ctx context.Context, key string, members ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "srem"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SUnion(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sunion"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SUnionStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sunionstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
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

func (c *cmdable) zAdd(ctx context.Context, a []interface{}, n int, members ...Z) *IntCmd {
	for i, m := range members {
		a[n+2*i] = m.Score
		a[n+2*i+1] = m.Member
	}
	cmd := NewIntCmd(a...)
	c.process(ctx, cmd)
	return cmd
}

// Redis `ZADD key score member [score member ...]` command.
func (c *cmdable) ZAdd(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 2
	a := make([]interface{}, n+2*len(members))
	a[0], a[1] = "zadd", key
	return c.zAdd(ctx, a, n, members...)
}

// Redis `ZADD key NX score member [score member ...]` command.
func (c *cmdable) ZAddNX(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "nx"
	return c.zAdd(ctx, a, n, members...)
}

// Redis `ZADD key XX score member [score member ...]` command.
func (c *cmdable) ZAddXX(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "xx"
	return c.zAdd(ctx, a, n, members...)
}

// Redis `ZADD key CH score member [score member ...]` command.
func (c *cmdable) ZAddCh(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 3
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2] = "zadd", key, "ch"
	return c.zAdd(ctx, a, n, members...)
}

// Redis `ZADD key NX CH score member [score member ...]` command.
func (c *cmdable) ZAddNXCh(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 4
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2], a[3] = "zadd", key, "nx", "ch"
	return c.zAdd(ctx, a, n, members...)
}

// Redis `ZADD key XX CH score member [score member ...]` command.
func (c *cmdable) ZAddXXCh(ctx context.Context, key string, members ...Z) *IntCmd {
	const n = 4
	a := make([]interface{}, n+2*len(members))
	a[0], a[1], a[2], a[3] = "zadd", key, "xx", "ch"
	return c.zAdd(ctx, a, n, members...)
}

func (c *cmdable) zIncr(ctx context.Context, a []interface{}, n int, members ...Z) *FloatCmd {
	for i, m := range members {
		a[n+2*i] = m.Score
		a[n+2*i+1] = m.Member
	}
	cmd := NewFloatCmd(a...)
	c.process(ctx, cmd)
	return cmd
}

// Redis `ZADD key INCR score member` command.
func (c *cmdable) ZIncr(ctx context.Context, key string, member Z) *FloatCmd {
	const n = 3
	a := make([]interface{}, n+2)
	a[0], a[1], a[2] = "zadd", key, "incr"
	return c.zIncr(ctx, a, n, member)
}

// Redis `ZADD key NX INCR score member` command.
func (c *cmdable) ZIncrNX(ctx context.Context, key string, member Z) *FloatCmd {
	const n = 4
	a := make([]interface{}, n+2)
	a[0], a[1], a[2], a[3] = "zadd", key, "incr", "nx"
	return c.zIncr(ctx, a, n, member)
}

// Redis `ZADD key XX INCR score member` command.
func (c *cmdable) ZIncrXX(ctx context.Context, key string, member Z) *FloatCmd {
	const n = 4
	a := make([]interface{}, n+2)
	a[0], a[1], a[2], a[3] = "zadd", key, "incr", "xx"
	return c.zIncr(ctx, a, n, member)
}

func (c *cmdable) ZCard(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("zcard", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZCount(ctx context.Context, key, min, max string) *IntCmd {
	cmd := NewIntCmd("zcount", key, min, max)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZLexCount(ctx context.Context, key, min, max string) *IntCmd {
	cmd := NewIntCmd("zlexcount", key, min, max)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZIncrBy(ctx context.Context, key string, increment float64, member string) *FloatCmd {
	cmd := NewFloatCmd("zincrby", key, increment, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZInterStore(ctx context.Context, destination string, store ZStore, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "zinterstore"
	args[1] = destination
	args[2] = len(keys)
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) zRange(ctx context.Context, key string, start, stop int64, withScores bool) *StringSliceCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	return c.zRange(ctx, key, start, stop, false)
}

func (c *cmdable) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *ZSliceCmd {
	cmd := NewZSliceCmd("zrange", key, start, stop, "withscores")
	c.process(ctx, cmd)
	return cmd
}

type ZRangeBy struct {
	Min, Max      string
	Offset, Count int64
}

func (c *cmdable) zRangeBy(ctx context.Context, zcmd, key string, opt ZRangeBy, withScores bool) *StringSliceCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRangeByScore(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRangeBy(ctx, "zrangebyscore", key, opt, false)
}

func (c *cmdable) ZRangeByLex(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRangeBy(ctx, "zrangebylex", key, opt, false)
}

func (c *cmdable) ZRangeByScoreWithScores(ctx context.Context, key string, opt ZRangeBy) *ZSliceCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRank(ctx context.Context, key, member string) *IntCmd {
	cmd := NewIntCmd("zrank", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRem(ctx context.Context, key string, members ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "zrem"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *IntCmd {
	cmd := NewIntCmd(
		"zremrangebyrank",
		key,
		start,
		stop,
	)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRemRangeByScore(ctx context.Context, key, min, max string) *IntCmd {
	cmd := NewIntCmd("zremrangebyscore", key, min, max)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRemRangeByLex(ctx context.Context, key, min, max string) *IntCmd {
	cmd := NewIntCmd("zremrangebylex", key, min, max)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRevRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	cmd := NewStringSliceCmd("zrevrange", key, start, stop)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *ZSliceCmd {
	cmd := NewZSliceCmd("zrevrange", key, start, stop, "withscores")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) zRevRangeBy(ctx context.Context, zcmd, key string, opt ZRangeBy) *StringSliceCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRevRangeByScore(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRevRangeBy(ctx, "zrevrangebyscore", key, opt)
}

func (c *cmdable) ZRevRangeByLex(ctx context.Context, key string, opt ZRangeBy) *StringSliceCmd {
	return c.zRevRangeBy(ctx, "zrevrangebylex", key, opt)
}

func (c *cmdable) ZRevRangeByScoreWithScores(ctx context.Context, key string, opt ZRangeBy) *ZSliceCmd {
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
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZRevRank(ctx context.Context, key, member string) *IntCmd {
	cmd := NewIntCmd("zrevrank", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZScore(ctx context.Context, key, member string) *FloatCmd {
	cmd := NewFloatCmd("zscore", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ZUnionStore(ctx context.Context, dest string, store ZStore, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "zunionstore"
	args[1] = dest
	args[2] = len(keys)
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
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) PFAdd(ctx context.Context, key string, els ...interface{}) *IntCmd {
	args := make([]interface{}, 2+len(els))
	args[0] = "pfadd"
	args[1] = key
	for i, el := range els {
		args[2+i] = el
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PFCount(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "pfcount"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PFMerge(ctx context.Context, dest string, keys ...string) *StatusCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "pfmerge"
	args[1] = dest
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) BgRewriteAOF(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("bgrewriteaof")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) BgSave(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("bgsave")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClientKill(ctx context.Context, ipPort string) *StatusCmd {
	cmd := NewStatusCmd("client", "kill", ipPort)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClientList(ctx context.Context) *StringCmd {
	cmd := NewStringCmd("client", "list")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClientPause(ctx context.Context, dur time.Duration) *BoolCmd {
	cmd := NewBoolCmd("client", "pause", formatMs(dur))
	c.process(ctx, cmd)
	return cmd
}

// ClientSetName assigns a name to the connection.
func (c *statefulCmdable) ClientSetName(ctx context.Context, name string) *BoolCmd {
	cmd := NewBoolCmd("client", "setname", name)
	c.process(ctx, cmd)
	return cmd
}

// ClientGetName returns the name of the connection.
func (c *cmdable) ClientGetName(ctx context.Context) *StringCmd {
	cmd := NewStringCmd("client", "getname")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ConfigGet(ctx context.Context, parameter string) *SliceCmd {
	cmd := NewSliceCmd("config", "get", parameter)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ConfigResetStat(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("config", "resetstat")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ConfigSet(ctx context.Context, parameter, value string) *StatusCmd {
	cmd := NewStatusCmd("config", "set", parameter, value)
	c.process(ctx, cmd)
	return cmd
}

// Deperecated. Use DBSize instead.
func (c *cmdable) DbSize(ctx context.Context) *IntCmd {
	return c.DBSize(ctx)
}

func (c *cmdable) DBSize(ctx context.Context) *IntCmd {
	cmd := NewIntCmd("dbsize")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) FlushAll(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("flushall")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) FlushAllAsync(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("flushall", "async")
	c.process(ctx, cmd)
	return cmd
}

// Deprecated. Use FlushDB instead.
func (c *cmdable) FlushDb(ctx context.Context) *StatusCmd {
	return c.FlushDB(ctx)
}

func (c *cmdable) FlushDB(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("flushdb")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) FlushDBAsync(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("flushdb", "async")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Info(ctx context.Context, section ...string) *StringCmd {
	args := []interface{}{"info"}
	if len(section) > 0 {
		args = append(args, section[0])
	}
	cmd := NewStringCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) LastSave(ctx context.Context) *IntCmd {
	cmd := NewIntCmd("lastsave")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) Save(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("save")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) shutdown(ctx context.Context, modifier string) *StatusCmd {
	var args []interface{}
	if modifier == "" {
		args = []interface{}{"shutdown"}
	} else {
		args = []interface{}{"shutdown", modifier}
	}
	cmd := NewStatusCmd(args...)
	c.process(ctx, cmd)
	if err := cmd.Err(); err != nil {
		if err == io.EOF {
			// Server quit as expected.
			cmd.err = nil
		}
	} else {
		// Server did not quit. String reply contains the reason.
		cmd.err = internal.RedisError(cmd.val)
		cmd.val = ""
	}
	return cmd
}

func (c *cmdable) Shutdown(ctx context.Context) *StatusCmd {
	return c.shutdown(ctx, "")
}

func (c *cmdable) ShutdownSave(ctx context.Context) *StatusCmd {
	return c.shutdown(ctx, "save")
}

func (c *cmdable) ShutdownNoSave(ctx context.Context) *StatusCmd {
	return c.shutdown(ctx, "nosave")
}

func (c *cmdable) SlaveOf(ctx context.Context, host, port string) *StatusCmd {
	cmd := NewStatusCmd("slaveof", host, port)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) SlowLog(ctx context.Context) {
	panic("not implemented")
}

func (c *cmdable) Sync(ctx context.Context) {
	panic("not implemented")
}

func (c *cmdable) Time(ctx context.Context) *TimeCmd {
	cmd := NewTimeCmd("time")
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd {
	cmdArgs := make([]interface{}, 3+len(keys)+len(args))
	cmdArgs[0] = "eval"
	cmdArgs[1] = script
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	pos := 3 + len(keys)
	for i, arg := range args {
		cmdArgs[pos+i] = arg
	}
	cmd := NewCmd(cmdArgs...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd {
	cmdArgs := make([]interface{}, 3+len(keys)+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = sha1
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	pos := 3 + len(keys)
	for i, arg := range args {
		cmdArgs[pos+i] = arg
	}
	cmd := NewCmd(cmdArgs...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ScriptExists(ctx context.Context, scripts ...string) *BoolSliceCmd {
	args := make([]interface{}, 2+len(scripts))
	args[0] = "script"
	args[1] = "exists"
	for i, script := range scripts {
		args[2+i] = script
	}
	cmd := NewBoolSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ScriptFlush(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("script", "flush")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ScriptKill(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("script", "kill")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ScriptLoad(ctx context.Context, script string) *StringCmd {
	cmd := NewStringCmd("script", "load", script)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) DebugObject(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd("debug", "object", key)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// Publish posts the message to the channel.
func (c *cmdable) Publish(ctx context.Context, channel string, message interface{}) *IntCmd {
	cmd := NewIntCmd("publish", channel, message)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PubSubChannels(ctx context.Context, pattern string) *StringSliceCmd {
	args := []interface{}{"pubsub", "channels"}
	if pattern != "*" {
		args = append(args, pattern)
	}
	cmd := NewStringSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PubSubNumSub(ctx context.Context, channels ...string) *StringIntMapCmd {
	args := make([]interface{}, 2+len(channels))
	args[0] = "pubsub"
	args[1] = "numsub"
	for i, channel := range channels {
		args[2+i] = channel
	}
	cmd := NewStringIntMapCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) PubSubNumPat(ctx context.Context) *IntCmd {
	cmd := NewIntCmd("pubsub", "numpat")
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) ClusterSlots(ctx context.Context) *ClusterSlotsCmd {
	cmd := NewClusterSlotsCmd("cluster", "slots")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterNodes(ctx context.Context) *StringCmd {
	cmd := NewStringCmd("cluster", "nodes")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterMeet(ctx context.Context, host, port string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "meet", host, port)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterForget(ctx context.Context, nodeID string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "forget", nodeID)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterReplicate(ctx context.Context, nodeID string) *StatusCmd {
	cmd := NewStatusCmd("cluster", "replicate", nodeID)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterResetSoft(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("cluster", "reset", "soft")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterResetHard(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("cluster", "reset", "hard")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterInfo(ctx context.Context) *StringCmd {
	cmd := NewStringCmd("cluster", "info")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterKeySlot(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd("cluster", "keyslot", key)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterCountFailureReports(ctx context.Context, nodeID string) *IntCmd {
	cmd := NewIntCmd("cluster", "count-failure-reports", nodeID)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterCountKeysInSlot(ctx context.Context, slot int) *IntCmd {
	cmd := NewIntCmd("cluster", "countkeysinslot", slot)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterDelSlots(ctx context.Context, slots ...int) *StatusCmd {
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "delslots"
	for i, slot := range slots {
		args[2+i] = slot
	}
	cmd := NewStatusCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterDelSlotsRange(ctx context.Context, min, max int) *StatusCmd {
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterDelSlots(ctx, slots...)
}

func (c *cmdable) ClusterSaveConfig(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("cluster", "saveconfig")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterSlaves(ctx context.Context, nodeID string) *StringSliceCmd {
	cmd := NewStringSliceCmd("cluster", "slaves", nodeID)
	c.process(ctx, cmd)
	return cmd
}

func (c *statefulCmdable) ReadOnly(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("readonly")
	c.process(ctx, cmd)
	return cmd
}

func (c *statefulCmdable) ReadWrite(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("readwrite")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterFailover(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd("cluster", "failover")
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterAddSlots(ctx context.Context, slots ...int) *StatusCmd {
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "addslots"
	for i, num := range slots {
		args[2+i] = num
	}
	cmd := NewStatusCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) ClusterAddSlotsRange(ctx context.Context, min, max int) *StatusCmd {
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterAddSlots(ctx, slots...)
}

//------------------------------------------------------------------------------

func (c *cmdable) GeoAdd(ctx context.Context, key string, geoLocation ...*GeoLocation) *IntCmd {
	args := make([]interface{}, 2+3*len(geoLocation))
	args[0] = "geoadd"
	args[1] = key
	for i, eachLoc := range geoLocation {
		args[2+3*i] = eachLoc.Longitude
		args[2+3*i+1] = eachLoc.Latitude
		args[2+3*i+2] = eachLoc.Name
	}
	cmd := NewIntCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadius", key, longitude, latitude)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoRadiusRO(ctx context.Context, key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadius_ro", key, longitude, latitude)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoRadiusByMember(ctx context.Context, key, member string, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadiusbymember", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoRadiusByMemberRO(ctx context.Context, key, member string, query *GeoRadiusQuery) *GeoLocationCmd {
	cmd := NewGeoLocationCmd(query, "georadiusbymember_ro", key, member)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoDist(ctx context.Context, key string, member1, member2, unit string) *FloatCmd {
	if unit == "" {
		unit = "km"
	}
	cmd := NewFloatCmd("geodist", key, member1, member2, unit)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoHash(ctx context.Context, key string, members ...string) *StringSliceCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "geohash"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewStringSliceCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

func (c *cmdable) GeoPos(ctx context.Context, key string, members ...string) *GeoPosCmd {
	args := make([]interface{}, 2+len(members))
	args[0] = "geopos"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewGeoPosCmd(args...)
	c.process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *cmdable) Command(ctx context.Context) *CommandsInfoCmd {
	cmd := NewCommandsInfoCmd("command")
	c.process(ctx, cmd)
	return cmd
}
