package redis_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	. "launchpad.net/gocheck"

	"github.com/vmihailenco/redis"
)

//------------------------------------------------------------------------------

type RedisTest struct {
	client, multiClient *redis.Client
}

var _ = Suite(&RedisTest{})

func Test(t *testing.T) { TestingT(t) }

//------------------------------------------------------------------------------

func (t *RedisTest) SetUpTest(c *C) {
	t.client = redis.NewTCPClient(":6379", "", 0)
	_, err := t.client.Flushdb().Reply()
	c.Check(err, IsNil)

	t.multiClient = t.client.Multi()
}

func (t *RedisTest) TearDownTest(c *C) {
	_, err := t.client.Flushdb().Reply()
	c.Check(err, IsNil)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestRunWithMissingReplyPart(c *C) {
	req := t.client.Set("foo", "bar")

	v, err := t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	ok, err := req.Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAuth(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestEcho(c *C) {
	echo, err := t.client.Echo("hello").Reply()
	c.Check(err, IsNil)
	c.Check(echo, Equals, "hello")
}

func (t *RedisTest) TestPing(c *C) {
	pong, err := t.client.Ping().Reply()
	c.Check(err, IsNil)
	c.Check(pong, Equals, "PONG")
}

func (t *RedisTest) TestSelect(c *C) {
	ok, err := t.client.Select(1).Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestDel(c *C) {
	t.client.Set("key1", "Hello")
	t.client.Set("key2", "World")

	n, err := t.client.Del("key1", "key2", "key3").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestDump(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestExists(c *C) {
	t.client.Set("key1", "Hello")

	exists, err := t.client.Exists("key1").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, true)

	exists, err = t.client.Exists("key2").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, false)
}

func (t *RedisTest) TestExpire(c *C) {
	t.client.Set("key", "Hello").Reply()

	isSet, err := t.client.Expire("key", 10).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	ttl, err := t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(10))

	t.client.Set("key", "Hello World")

	ttl, err = t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(-1))
}

func (t *RedisTest) TestExpireAt(c *C) {
	t.client.Set("key", "Hello").Reply()

	exists, err := t.client.Exists("key").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, true)

	isSet, err := t.client.ExpireAt("key", 1293840000).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	exists, err = t.client.Exists("key").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, false)
}

func (t *RedisTest) TestKeys(c *C) {
	t.client.MSet("one", "1", "two", "2", "three", "3", "four", "4").Reply()

	keys, err := t.client.Keys("*o*").Reply()
	c.Check(err, IsNil)
	c.Check(keys, DeepEquals, []interface{}{"four", "two", "one"})

	keys, err = t.client.Keys("t??").Reply()
	c.Check(err, IsNil)
	c.Check(keys, DeepEquals, []interface{}{"two"})

	keys, err = t.client.Keys("*").Reply()
	c.Check(err, IsNil)
	c.Check(keys, DeepEquals, []interface{}{"four", "three", "two", "one"})
}

func (t *RedisTest) TestMigrate(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestMove(c *C) {
	isMoved, err := t.client.Move("foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, false)

	t.client.Set("foo", "bar").Reply()

	isMoved, err = t.client.Move("foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, true)

	v, err := t.client.Get("foo").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")

	t.client.Select(1)

	v, err = t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestObject(c *C) {
	t.client.Set("foo", "bar").Reply()

	n, err := t.client.ObjectRefCount("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	enc, err := t.client.ObjectEncoding("foo").Reply()
	c.Check(err, IsNil)
	c.Check(enc, Equals, "raw")

	n, err = t.client.ObjectIdleTime("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestPersist(c *C) {
	t.client.Set("key", "Hello").Reply()
	t.client.Expire("key", 10).Reply()

	ttl, err := t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(10))

	isPersisted, err := t.client.Persist("key").Reply()
	c.Check(err, IsNil)
	c.Check(isPersisted, Equals, true)

	ttl, err = t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(-1))
}

func (t *RedisTest) TestPExpire(c *C) {
	c.Skip("not implemented")

	t.client.Set("key", "Hello").Reply()

	isSet, err := t.client.PExpire("key", 1500).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	ttl, err := t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, 1)

	pttl, err := t.client.PTTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(pttl, Equals, 1500)
}

func (t *RedisTest) TestPExpireAt(c *C) {
	c.Skip("not implemented")

	t.client.Set("key", "Hello").Reply()

	isSet, err := t.client.PExpireAt("key", 1555555555005).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	ttl, err := t.client.TTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, 211915059)

	pttl, err := t.client.PTTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(pttl, Equals, int64(211915059461))
}

func (t *RedisTest) TestPTTL(c *C) {
	c.Skip("not implemented")

	t.client.Set("key", "Hello").Reply()
	t.client.Expire("key", 1).Reply()

	pttl, err := t.client.PTTL("key").Reply()
	c.Check(err, IsNil)
	c.Check(pttl, Equals, int64(999))
}

func (t *RedisTest) TestRandomKey(c *C) {
	key, err := t.client.RandomKey().Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(key, Equals, "")

	t.client.Set("foo", "bar").Reply()

	key, err = t.client.RandomKey().Reply()
	c.Check(err, IsNil)
	c.Check(key, Equals, "foo")
}

func (t *RedisTest) TestRename(c *C) {
	t.client.Set("foo", "bar").Reply()

	status, err := t.client.Rename("foo", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(status, Equals, "OK")

	v, err := t.client.Get("foo1").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestRenameNX(c *C) {
	t.client.Set("foo", "bar").Reply()

	renamed, err := t.client.RenameNX("foo", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(renamed, Equals, true)

	v, err := t.client.Get("foo1").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestRestore(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSort(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestTTL(c *C) {
	ttl, err := t.client.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(-1))

	t.client.Set("foo", "bar").Reply()
	t.client.Expire("foo", 60)

	ttl, err = t.client.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(60))
}

func (t *RedisTest) Type(c *C) {
	t.client.Set("foo", "bar").Reply()

	type_, err := t.client.Type("foo").Reply()
	c.Check(err, IsNil)
	c.Check(type_, Equals, "string")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAppend(c *C) {
	l, err := t.client.Append("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(l, Equals, int64(3))
}

func (t *RedisTest) TestDecr(c *C) {
	n, err := t.client.Decr("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-1))
}

func (t *RedisTest) TestDecrBy(c *C) {
	n, err := t.client.DecrBy("foo", 10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-10))
}

func (t *RedisTest) TestGet(c *C) {
	v, err := t.client.Get("foo").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestSetGetBig(c *C) {
	v, err := t.client.GetBit("foo", 5).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(0))

	v, err = t.client.SetBit("foo", 5, 1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(0))

	v, err = t.client.GetBit("foo", 5).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(1))
}

func (t *RedisTest) TestGetRange(c *C) {
	t.client.Set("foo", "hello")

	v, err := t.client.GetRange("foo", 0, 1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "he")
}

func (t *RedisTest) TestGetSet(c *C) {
	t.client.Set("foo", "bar")

	v, err := t.client.GetSet("foo", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	v, err = t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar2")
}

func (t *RedisTest) TestIncr(c *C) {
	n, err := t.client.Incr("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))
}

func (t *RedisTest) TestIncrBy(c *C) {
	n, err := t.client.IncrBy("foo", 10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(10))
}

func (t *RedisTest) TestMsetMget(c *C) {
	ok, err := t.client.MSet("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.client.MGet("foo1", "foo2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"bar1", "bar2"})
}

func (t *RedisTest) MSetNX(c *C) {
	isSet, err := t.client.MSetNX("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.client.MSetNX("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) PSetEx(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSetGet(c *C) {
	ok, err := t.client.Set("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	v, err := t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestSetEx(c *C) {
	ok, err := t.client.SetEx("foo", 10, "bar").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	ttl, err := t.client.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(10))
}

func (t *RedisTest) TestSetNx(c *C) {
	isSet, err := t.client.SetNx("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.client.SetNx("foo", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)

	v, err := t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestSetRange(c *C) {
	t.client.Set("foo", "Hello World").Reply()

	n, err := t.client.SetRange("foo", 6, "Redis").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(11))

	v, err := t.client.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "Hello Redis")
}

func (t *RedisTest) TestStrLen(c *C) {
	t.client.Set("foo", "bar").Reply()

	n, err := t.client.StrLen("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	n, err = t.client.StrLen("_").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestHDel(c *C) {
	t.client.HSet("myhash", "foo", "bar").Reply()

	n, err := t.client.HDel("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.HDel("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestHExists(c *C) {
	t.client.HSet("myhash", "foo", "bar").Reply()

	n, err := t.client.HExists("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, true)

	n, err = t.client.HExists("myhash", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, false)
}

func (t *RedisTest) TestHGet(c *C) {
	t.client.HSet("myhash", "foo", "bar").Reply()

	v, err := t.client.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	v, err = t.client.HGet("myhash", "foo1").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestHGetAll(c *C) {
	t.client.HSet("myhash", "foo1", "bar1").Reply()
	t.client.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.client.HGetAll("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"foo1", "bar1", "foo2", "bar2"})
}

func (t *RedisTest) TestHIncrBy(c *C) {
	t.client.HSet("myhash", "foo", "5").Reply()

	n, err := t.client.HIncrBy("myhash", "foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(6))

	n, err = t.client.HIncrBy("myhash", "foo", -1).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(5))

	n, err = t.client.HIncrBy("myhash", "foo", -10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-5))
}

func (t *RedisTest) TestHIncrByFloat(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestHKeys(c *C) {
	t.client.HSet("myhash", "foo1", "bar1").Reply()
	t.client.HSet("myhash", "foo2", "bar2").Reply()

	keys, err := t.client.HKeys("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(keys, DeepEquals, []interface{}{"foo1", "foo2"})
}

func (t *RedisTest) TestHLen(c *C) {
	t.client.HSet("myhash", "foo1", "bar1").Reply()
	t.client.HSet("myhash", "foo2", "bar2").Reply()

	n, err := t.client.HLen("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestHMGet(c *C) {
	t.client.HSet("myhash", "foo1", "bar1").Reply()
	t.client.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.client.HMGet("myhash", "foo1", "foo2", "_").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"bar1", "bar2", nil})
}

func (t *RedisTest) TestHMSet(c *C) {
	ok, err := t.client.HMSet("myhash", "foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	v1, err := t.client.HGet("myhash", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(v1, Equals, "bar1")

	v2, err := t.client.HGet("myhash", "foo2").Reply()
	c.Check(err, IsNil)
	c.Check(v2, Equals, "bar2")
}

func (t *RedisTest) TestHSet(c *C) {
	isNew, err := t.client.HSet("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isNew, Equals, true)

	v, err := t.client.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestHSetNX(c *C) {
	isSet, err := t.client.HSetNX("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.client.HSetNX("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)

	v, err := t.client.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestHVals(c *C) {
	t.client.HSet("myhash", "foo1", "bar1").Reply()
	t.client.HSet("myhash", "foo2", "bar2").Reply()

	vals, err := t.client.HVals("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(vals, DeepEquals, []interface{}{"bar1", "bar2"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestBLPop(c *C) {
	t.client.RPush("list1", "a", "b", "c").Reply()

	values, err := t.client.BLPop(0, "list1", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"list1", "a"})
}

func (t *RedisTest) TestBrPop(c *C) {
	t.client.RPush("list1", "a", "b", "c").Reply()

	values, err := t.client.BRPop(0, "list1", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"list1", "c"})
}

func (t *RedisTest) TestBRPopLPush(c *C) {
	t.client.RPush("list1", "a", "b", "c").Reply()

	v, err := t.client.BRPopLPush("list1", "list2", 0).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "c")
}

func (t *RedisTest) TestLIndex(c *C) {
	t.client.LPush("list", "World")
	t.client.LPush("list", "Hello")

	v, err := t.client.LIndex("list", 0).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "Hello")

	v, err = t.client.LIndex("list", -1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "World")

	v, err = t.client.LIndex("list", 3).Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestLInsert(c *C) {
	t.client.RPush("list", "Hello").Reply()
	t.client.RPush("list", "World").Reply()

	n, err := t.client.LInsert("list", "BEFORE", "World", "There").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "There", "World"})
}

func (t *RedisTest) TestLLen(c *C) {
	t.client.LPush("list", "World").Reply()
	t.client.LPush("list", "Hello").Reply()

	n, err := t.client.LLen("list").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestLPop(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	v, err := t.client.LPop("list").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "one")

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestLPush(c *C) {
	t.client.LPush("list", "World").Reply()
	t.client.LPush("list", "Hello").Reply()

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestLPushX(c *C) {
	t.client.LPush("list", "World").Reply()

	n, err := t.client.LPushX("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.client.LPushX("list2", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})

	values, err = t.client.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRange(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	values, err := t.client.LRange("list", 0, 0).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one"})

	values, err = t.client.LRange("list", -3, 2).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.client.LRange("list", -100, 100).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.client.LRange("list", 5, 10).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRem(c *C) {
	t.client.RPush("list", "hello").Reply()
	t.client.RPush("list", "hello").Reply()
	t.client.RPush("list", "foo").Reply()
	t.client.RPush("list", "hello").Reply()

	n, err := t.client.LRem("list", -2, "hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"hello", "foo"})
}

func (t *RedisTest) TestLSet(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	ok, err := t.client.LSet("list", 0, "four").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	ok, err = t.client.LSet("list", -2, "five").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"four", "five", "three"})
}

func (t *RedisTest) TestLTrim(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	ok, err := t.client.LTrim("list", 1, -1).Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestRPop(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	v, err := t.client.RPop("list").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "three")

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two"})
}

func (t *RedisTest) TestRPopLPush(c *C) {
	t.client.RPush("list", "one").Reply()
	t.client.RPush("list", "two").Reply()
	t.client.RPush("list", "three").Reply()

	v, err := t.client.RPopLPush("list", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "three")

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two"})

	values, err = t.client.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three"})
}

func (t *RedisTest) TestRPush(c *C) {
	n, err := t.client.RPush("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.RPush("list", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestRPushX(c *C) {
	n, err := t.client.RPush("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.RPushX("list", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.client.RPushX("list2", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.client.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})

	values, err = t.client.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestSAdd(c *C) {
	n, err := t.client.SAdd("set", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	members, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(members, DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSCard(c *C) {
	n, err := t.client.SAdd("set", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	card, err := t.client.SCard("set").Reply()
	c.Check(err, IsNil)
	c.Check(card, Equals, int64(2))
}

func (t *RedisTest) TestSDiff(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	values, err := t.client.SDiff("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSDiffStore(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	n, err := t.client.SDiffStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSInter(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	values, err := t.client.SInter("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestSInterStore(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	n, err := t.client.SInterStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestIsMember(c *C) {
	t.client.SAdd("set", "one").Reply()

	isMember, err := t.client.SIsMember("set", "one").Reply()
	c.Check(err, IsNil)
	c.Check(isMember, Equals, true)

	isMember, err = t.client.SIsMember("set", "two").Reply()
	c.Check(err, IsNil)
	c.Check(isMember, Equals, false)
}

func (t *RedisTest) TestSMembers(c *C) {
	t.client.SAdd("set", "Hello").Reply()
	t.client.SAdd("set", "World").Reply()

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSMove(c *C) {
	t.client.SAdd("set1", "one").Reply()
	t.client.SAdd("set1", "two").Reply()

	t.client.SAdd("set2", "three").Reply()

	isMoved, err := t.client.SMove("set1", "set2", "two").Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, true)

	values, err := t.client.SMembers("set1").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one"})

	values, err = t.client.SMembers("set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSPop(c *C) {
	t.client.SAdd("set", "one").Reply()
	t.client.SAdd("set", "two").Reply()
	t.client.SAdd("set", "three").Reply()

	v, err := t.client.SPop("set").Reply()
	c.Check(err, IsNil)
	c.Check(v, Not(Equals), "")

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
}

func (t *RedisTest) TestSRandMember(c *C) {
	t.client.SAdd("set", "one").Reply()
	t.client.SAdd("set", "two").Reply()
	t.client.SAdd("set", "three").Reply()

	v, err := t.client.SRandMember("set").Reply()
	c.Check(err, IsNil)
	c.Check(v, Not(Equals), "")

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
}

func (t *RedisTest) TestSRem(c *C) {
	t.client.SAdd("set", "one").Reply()
	t.client.SAdd("set", "two").Reply()
	t.client.SAdd("set", "three").Reply()

	n, err := t.client.SRem("set", "one").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.SRem("set", "four").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSUnion(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	values, err := t.client.SUnion("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 5)
}

func (t *RedisTest) TestSUnionStore(c *C) {
	t.client.SAdd("set1", "a").Reply()
	t.client.SAdd("set1", "b").Reply()
	t.client.SAdd("set1", "c").Reply()

	t.client.SAdd("set2", "c").Reply()
	t.client.SAdd("set2", "d").Reply()
	t.client.SAdd("set2", "e").Reply()

	n, err := t.client.SUnionStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(5))

	values, err := t.client.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 5)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestZAdd(c *C) {
	n, err := t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.ZAdd("zset", redis.NewZMember(1, "uno")).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.ZAdd("zset", redis.NewZMember(3, "two")).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.client.ZRange("zset", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "1", "uno", "1", "two", "3"})
}

func (t *RedisTest) TestZCard(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()

	n, err := t.client.ZCard("zset").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestZCount(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZCount("zset", "-inf", "+inf").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	n, err = t.client.ZCount("zset", "(1", "3").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestZIncrBy(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZIncrBy("zset", 2, "one").Reply()

	values, err := t.client.ZRange("zset", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "2", "one", "3"})
}

func (t *RedisTest) TestZInterStore(c *C) {
	t.client.ZAdd("zset1", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset1", redis.NewZMember(2, "two")).Reply()

	t.client.ZAdd("zset2", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset2", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset3", redis.NewZMember(3, "two")).Reply()

	n, err := t.client.ZInterStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.client.ZRange("out", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "5", "two", "10"})
}

func (t *RedisTest) TestZRange(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	values, err := t.client.ZRange("zset", 0, -1, false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.client.ZRange("zset", 2, 3, false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three"})

	values, err = t.client.ZRange("zset", -2, -1, false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestZRangeByScore(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	values, err := t.client.ZRangeByScore("zset", "-inf", "+inf", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.client.ZRangeByScore("zset", "1", "2", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "two"})

	values, err = t.client.ZRangeByScore("zset", "(1", "2", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two"})

	values, err = t.client.ZRangeByScore("zset", "(1", "(2", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRank(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZRank("zset", "three").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.client.ZRank("zset", "four").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestZRem(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZRem("zset", "two").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	values, err := t.client.ZRange("zset", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "1", "three", "3"})
}

func (t *RedisTest) TestZRemRangeByRank(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZRemRangeByRank("zset", 0, 1).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.client.ZRange("zset", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three", "3"})
}

func (t *RedisTest) TestZRemRangeByScore(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZRemRangeByScore("zset", "-inf", "(2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	values, err := t.client.ZRange("zset", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "2", "three", "3"})
}

func (t *RedisTest) TestZRevRange(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	values, err := t.client.ZRevRange("zset", "0", "-1", false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three", "two", "one"})

	values, err = t.client.ZRevRange("zset", "2", "3", false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one"})

	values, err = t.client.ZRevRange("zset", "-2", "-1", false).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two", "one"})
}

func (t *RedisTest) TestZRevRangeByScore(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	values, err := t.client.ZRevRangeByScore("zset", "+inf", "-inf", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"three", "two", "one"})

	values, err = t.client.ZRevRangeByScore("zset", "2", "(1", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"two"})

	values, err = t.client.ZRevRangeByScore("zset", "(2", "(1", false, nil).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRevRank(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZRevRank("zset", "one").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.client.ZRevRank("zset", "four").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestZScore(c *C) {
	t.client.ZAdd("zset", redis.NewZMember(1.001, "one")).Reply()

	score, err := t.client.ZScore("zset", "one").Reply()
	c.Check(err, IsNil)
	c.Check(score, Equals, float64(1.001))
}

func (t *RedisTest) TestZUnionStore(c *C) {
	t.client.ZAdd("zset1", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset1", redis.NewZMember(2, "two")).Reply()

	t.client.ZAdd("zset2", redis.NewZMember(1, "one")).Reply()
	t.client.ZAdd("zset2", redis.NewZMember(2, "two")).Reply()
	t.client.ZAdd("zset2", redis.NewZMember(3, "three")).Reply()

	n, err := t.client.ZUnionStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	values, err := t.client.ZRange("out", 0, -1, true).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"one", "5", "three", "9", "two", "10"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestPubSub(c *C) {
	pubsub, err := t.client.PubSubClient()
	c.Check(err, IsNil)

	ch, err := pubsub.Subscribe("mychannel")
	c.Check(err, IsNil)
	c.Check(ch, Not(Equals), nil)

	ch, err = pubsub.Subscribe("mychannel2")
	c.Check(err, IsNil)
	c.Check(ch, Not(Equals), nil)

	n, err := t.client.Publish("mychannel", "hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.client.Publish("mychannel2", "hello2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	err = pubsub.Unsubscribe("mychannel")
	c.Check(err, IsNil)

	err = pubsub.Unsubscribe("mychannel2")
	c.Check(err, IsNil)

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "subscribe")
		c.Check(msg.Channel, Equals, "mychannel")
		c.Check(msg.Number, Equals, int64(1))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "subscribe")
		c.Check(msg.Channel, Equals, "mychannel2")
		c.Check(msg.Number, Equals, int64(2))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "message")
		c.Check(msg.Channel, Equals, "mychannel")
		c.Check(msg.Message, Equals, "hello")
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "message")
		c.Check(msg.Channel, Equals, "mychannel2")
		c.Check(msg.Message, Equals, "hello2")
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "unsubscribe")
		c.Check(msg.Channel, Equals, "mychannel")
		c.Check(msg.Number, Equals, int64(1))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Check(msg.Err, Equals, nil)
		c.Check(msg.Name, Equals, "unsubscribe")
		c.Check(msg.Channel, Equals, "mychannel2")
		c.Check(msg.Number, Equals, int64(0))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestPipelining(c *C) {
	_, err := t.client.Set("foo2", "bar2").Reply()
	c.Check(err, IsNil)

	setReq := t.multiClient.Set("foo1", "bar1")
	getReq := t.multiClient.Get("foo2")

	reqs, err := t.multiClient.RunQueued()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 2)

	ok, err := setReq.Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	v, err := getReq.Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar2")
}

func (t *RedisTest) TestRunQueuedOnEmptyQueue(c *C) {
	reqs, err := t.client.RunQueued()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 0)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestDiscard(c *C) {
	multiC := t.client.Multi()

	multiC.Set("foo1", "bar1")
	multiC.Discard()
	multiC.Set("foo2", "bar2")

	reqs, err := multiC.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 1)

	v, err := t.client.Get("foo1").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")

	v, err = t.client.Get("foo2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar2")
}

func (t *RedisTest) TestMultiExec(c *C) {
	multiC := t.client.Multi()

	setR := multiC.Set("foo", "bar")
	getR := multiC.Get("foo")

	reqs, err := multiC.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 2)

	_, err = setR.Reply()
	c.Check(err, IsNil)

	v, err := getR.Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestExecOnEmptyQueue(c *C) {
	reqs, err := t.client.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 0)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestEchoFromGoroutines(c *C) {
	for i := int64(0); i < 1000; i++ {
		go func() {
			msg := "echo" + strconv.FormatInt(i, 10)
			echo, err := t.client.Echo(msg).Reply()
			c.Check(err, IsNil)
			c.Check(echo, Equals, msg)
		}()
	}
}

func (t *RedisTest) TestPipeliningFromGoroutines(c *C) {
	multiClient := t.client.Multi()

	for i := int64(0); i < 1000; i += 2 {
		go func() {
			msg1 := "echo" + strconv.FormatInt(i, 10)
			msg2 := "echo" + strconv.FormatInt(i+1, 10)

			echo1Req := multiClient.Echo(msg1)
			echo2Req := multiClient.Echo(msg2)

			reqs, err := multiClient.RunQueued()
			c.Check(reqs, HasLen, 2)
			c.Check(err, IsNil)

			echo1, err := echo1Req.Reply()
			c.Check(err, IsNil)
			c.Check(echo1, Equals, msg1)

			echo2, err := echo2Req.Reply()
			c.Check(err, IsNil)
			c.Check(echo2, Equals, msg2)
		}()
	}
}

func (t *RedisTest) TestIncrFromGoroutines(c *C) {
	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			_, err := t.client.Incr("TestIncrFromGoroutinesKey").Reply()
			c.Check(err, IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	n, err := t.client.Get("TestIncrFromGoroutinesKey").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, "20000")
}

func (t *RedisTest) TestIncrPipeliningFromGoroutines(c *C) {
	multiClient := t.client.Multi()

	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			multiClient.Incr("TestIncrPipeliningFromGoroutinesKey")
			wg.Done()
		}()
	}
	wg.Wait()

	reqs, err := multiClient.RunQueued()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 20000)

	n, err := t.client.Get("TestIncrPipeliningFromGoroutinesKey").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, "20000")
}

func (t *RedisTest) TestIncrTransaction(c *C) {
	multiClient := t.client.Multi()

	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			multiClient.Incr("TestIncrTransactionKey")
			wg.Done()
		}()
	}
	wg.Wait()

	reqs, err := multiClient.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 20000)

	n, err := t.client.Get("TestIncrTransactionKey").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, "20000")
}

//------------------------------------------------------------------------------

func (t *RedisTest) BenchmarkRedisPing(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		pong, err := t.client.Ping().Reply()
		c.Check(err, IsNil)
		c.Check(pong, Equals, "PONG")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Ping().Reply()
	}
}

func (t *RedisTest) BenchmarkRedisSet(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		ok, err := t.client.Set("foo", "bar").Reply()
		c.Check(err, IsNil)
		c.Check(ok, Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Set("foo", "bar").Reply()
	}
}

func (t *RedisTest) BenchmarkRedisGetNil(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		v, err := t.client.Get("foo").Reply()
		c.Check(err, Equals, redis.Nil)
		c.Check(v, Equals, "")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo").Reply()
	}
}

func (t *RedisTest) BenchmarkRedisGet(c *C) {
	c.StopTimer()

	_, err := t.client.Set("foo", "bar").Reply()
	c.Check(err, IsNil)

	for i := 0; i < 10; i++ {
		v, err := t.client.Get("foo").Reply()
		c.Check(err, IsNil)
		c.Check(v, Equals, "bar")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo").Reply()
	}
}

func (t *RedisTest) BenchmarkRedisWriteRead(c *C) {
	c.StopTimer()

	req := []byte("PING\r\n")
	conn, _, err := t.client.ConnPool.Get()
	c.Check(err, IsNil)

	for i := 0; i < 10; i++ {
		err := t.client.WriteRead(req, conn)
		c.Check(err, IsNil)
		c.Check(conn.Rd.Bytes(), DeepEquals, []byte("+PONG\r\n"))
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.WriteRead(req, conn)
	}

	c.StopTimer()
	t.client.ConnPool.Add(conn)
	c.StartTimer()
}
