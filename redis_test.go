package redis_test

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	. "launchpad.net/gocheck"

	"github.com/vmihailenco/bufreader"
	"github.com/vmihailenco/redis"
)

//------------------------------------------------------------------------------

type RedisTest struct {
	redisC *redis.Client
}

var _ = Suite(&RedisTest{})

func Test(t *testing.T) { TestingT(t) }

//------------------------------------------------------------------------------

func (t *RedisTest) SetUpTest(c *C) {
	connect := func() (io.ReadWriter, error) {
		return net.Dial("tcp", "localhost:6379")
	}

	t.redisC = redis.NewClient(connect, nil)
	t.redisC.Flushdb()
}

func (t *RedisTest) TearDownTest(c *C) {
	t.redisC.Flushdb()
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAuth(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestEcho(c *C) {
	echo, err := t.redisC.Echo("hello").Reply()
	c.Check(err, IsNil)
	c.Check(echo, Equals, "hello")
}

func (t *RedisTest) TestPing(c *C) {
	pong, err := t.redisC.Ping().Reply()
	c.Check(err, IsNil)
	c.Check(pong, Equals, "PONG")
}

func (t *RedisTest) TestQuit(c *C) {
	ok, err := t.redisC.Quit().Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	pong, err := t.redisC.Ping().Reply()
	c.Check(err, IsNil)
	c.Check(pong, Equals, "PONG")
}

func (t *RedisTest) TestSelect(c *C) {
	ok, err := t.redisC.Select(1).Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestDel(c *C) {
	n, err := t.redisC.Del("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestDump(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestExists(c *C) {
	exists, err := t.redisC.Exists("foo").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, false)
}

func (t *RedisTest) TestExpire(c *C) {
	isSet, err := t.redisC.Expire("foo", 0).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) TestExpireAt(c *C) {
	isSet, err := t.redisC.ExpireAt("foo", 0).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) TestKeys(c *C) {
	t.redisC.Set("foo1", "")
	t.redisC.Set("foo2", "")

	keys, err := t.redisC.Keys("*").Reply()
	c.Check(err, IsNil)
	c.Check(keys, HasLen, 2)
	c.Check(keys[0], Equals, "foo1")
	c.Check(keys[1], Equals, "foo2")
}

func (t *RedisTest) TestMigrate(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestMove(c *C) {
	isMoved, err := t.redisC.Move("foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, false)

	t.redisC.Set("foo", "bar")

	isMoved, err = t.redisC.Move("foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, true)

	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")

	t.redisC.Select(1)

	v, err = t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestObject(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	n, err := t.redisC.ObjectRefCount("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	enc, err := t.redisC.ObjectEncoding("foo").Reply()
	c.Check(err, IsNil)
	c.Check(enc, Equals, "raw")

	n, err = t.redisC.ObjectIdleTime("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestPersist(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	isPersisted, err := t.redisC.Persist("foo").Reply()
	c.Check(err, IsNil)
	c.Check(isPersisted, Equals, false)

	t.redisC.Expire("foo", 10)

	isPersisted, err = t.redisC.Persist("foo").Reply()
	c.Check(err, IsNil)
	c.Check(isPersisted, Equals, true)
}

func (t *RedisTest) TestPexpire(c *C) {
	c.Skip("not implemented")
	isSet, err := t.redisC.Pexpire("foo", 0).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) TestPexpireAt(c *C) {
	c.Skip("not implemented")
	isSet, err := t.redisC.PexpireAt("foo", time.Now().UnixNano()*100+60).Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) TestPTTL(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestRandomKey(c *C) {
	key, err := t.redisC.RandomKey().Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(key, Equals, "")

	t.redisC.Set("foo", "bar").Reply()

	key, err = t.redisC.RandomKey().Reply()
	c.Check(err, IsNil)
	c.Check(key, Equals, "foo")
}

func (t *RedisTest) TestRename(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	status, err := t.redisC.Rename("foo", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(status, Equals, "OK")

	v, err := t.redisC.Get("foo1").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestRenameNX(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	renamed, err := t.redisC.RenameNX("foo", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(renamed, Equals, true)

	v, err := t.redisC.Get("foo1").Reply()
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
	ttl, err := t.redisC.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(-1))

	t.redisC.Set("foo", "bar").Reply()
	t.redisC.Expire("foo", 60)

	ttl, err = t.redisC.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(60))
}

func (t *RedisTest) Type(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	type_, err := t.redisC.Type("foo").Reply()
	c.Check(err, IsNil)
	c.Check(type_, Equals, "string")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAppend(c *C) {
	l, err := t.redisC.Append("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(l, Equals, int64(3))
}

func (t *RedisTest) TestDecr(c *C) {
	n, err := t.redisC.Decr("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-1))
}

func (t *RedisTest) TestDecrBy(c *C) {
	n, err := t.redisC.DecrBy("foo", 10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-10))
}

func (t *RedisTest) TestGet(c *C) {
	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestSetGetBig(c *C) {
	v, err := t.redisC.GetBit("foo", 5).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(0))

	v, err = t.redisC.SetBit("foo", 5, 1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(0))

	v, err = t.redisC.GetBit("foo", 5).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(1))
}

func (t *RedisTest) TestGetRange(c *C) {
	t.redisC.Set("foo", "hello")

	v, err := t.redisC.GetRange("foo", 0, 1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "he")
}

func (t *RedisTest) TestGetSet(c *C) {
	t.redisC.Set("foo", "bar")

	v, err := t.redisC.GetSet("foo", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	v, err = t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar2")
}

func (t *RedisTest) TestIncr(c *C) {
	n, err := t.redisC.Incr("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))
}

func (t *RedisTest) TestIncrBy(c *C) {
	n, err := t.redisC.IncrBy("foo", 10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(10))
}

func (t *RedisTest) TestMsetMget(c *C) {
	ok, err := t.redisC.MSet("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.redisC.MGet("foo1", "foo2").Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"bar1", "bar2"})
}

func (t *RedisTest) MSetNX(c *C) {
	isSet, err := t.redisC.MSetNX("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.redisC.MSetNX("foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)
}

func (t *RedisTest) PSetEx(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSetGet(c *C) {
	ok, err := t.redisC.Set("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestSetEx(c *C) {
	ok, err := t.redisC.SetEx("foo", 10, "bar").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	ttl, err := t.redisC.TTL("foo").Reply()
	c.Check(err, IsNil)
	c.Check(ttl, Equals, int64(10))
}

func (t *RedisTest) TestSetNx(c *C) {
	isSet, err := t.redisC.SetNx("foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.redisC.SetNx("foo", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)

	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestSetRange(c *C) {
	t.redisC.Set("foo", "Hello World").Reply()

	n, err := t.redisC.SetRange("foo", 6, "Redis").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(11))

	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "Hello Redis")
}

func (t *RedisTest) TestStrLen(c *C) {
	t.redisC.Set("foo", "bar").Reply()

	n, err := t.redisC.StrLen("foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	n, err = t.redisC.StrLen("_").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestHDel(c *C) {
	t.redisC.HSet("myhash", "foo", "bar").Reply()

	n, err := t.redisC.HDel("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.HDel("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))
}

func (t *RedisTest) TestHExists(c *C) {
	t.redisC.HSet("myhash", "foo", "bar").Reply()

	n, err := t.redisC.HExists("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, true)

	n, err = t.redisC.HExists("myhash", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, false)
}

func (t *RedisTest) TestHGet(c *C) {
	t.redisC.HSet("myhash", "foo", "bar").Reply()

	v, err := t.redisC.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	v, err = t.redisC.HGet("myhash", "foo1").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestHGetAll(c *C) {
	t.redisC.HSet("myhash", "foo1", "bar1").Reply()
	t.redisC.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.redisC.HGetAll("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 4)
	c.Check(values, DeepEquals, []interface{}{"foo1", "bar1", "foo2", "bar2"})
}

func (t *RedisTest) TestHIncrBy(c *C) {
	t.redisC.HSet("myhash", "foo", "5").Reply()

	n, err := t.redisC.HIncrBy("myhash", "foo", 1).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(6))

	n, err = t.redisC.HIncrBy("myhash", "foo", -1).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(5))

	n, err = t.redisC.HIncrBy("myhash", "foo", -10).Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(-5))
}

func (t *RedisTest) TestHIncrByFloat(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestHKeys(c *C) {
	t.redisC.HSet("myhash", "foo1", "bar1").Reply()
	t.redisC.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.redisC.HKeys("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"foo1", "foo2"})
}

func (t *RedisTest) TestHLen(c *C) {
	t.redisC.HSet("myhash", "foo1", "bar1").Reply()
	t.redisC.HSet("myhash", "foo2", "bar2").Reply()

	n, err := t.redisC.HLen("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestHMGet(c *C) {
	t.redisC.HSet("myhash", "foo1", "bar1").Reply()
	t.redisC.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.redisC.HMGet("myhash", "foo1", "foo2", "_").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
	c.Check(values, DeepEquals, []interface{}{"bar1", "bar2", nil})
}

func (t *RedisTest) TestHMSet(c *C) {
	ok, err := t.redisC.HMSet("myhash", "foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	v1, err := t.redisC.HGet("myhash", "foo1").Reply()
	c.Check(err, IsNil)
	c.Check(v1, Equals, "bar1")

	v2, err := t.redisC.HGet("myhash", "foo2").Reply()
	c.Check(err, IsNil)
	c.Check(v2, Equals, "bar2")
}

func (t *RedisTest) TestHSet(c *C) {
	isNew, err := t.redisC.HSet("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isNew, Equals, true)

	v, err := t.redisC.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestHSetNX(c *C) {
	isSet, err := t.redisC.HSetNX("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, true)

	isSet, err = t.redisC.HSetNX("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)
	c.Check(isSet, Equals, false)

	v, err := t.redisC.HGet("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

func (t *RedisTest) TestHVals(c *C) {
	t.redisC.HSet("myhash", "foo1", "bar1").Reply()
	t.redisC.HSet("myhash", "foo2", "bar2").Reply()

	values, err := t.redisC.HVals("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"bar1", "bar2"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestBLPop(c *C) {
	t.redisC.RPush("list1", "a", "b", "c").Reply()

	values, err := t.redisC.BLPop(0, "list1", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"list1", "a"})
}

func (t *RedisTest) TestBrPop(c *C) {
	t.redisC.RPush("list1", "a", "b", "c").Reply()

	values, err := t.redisC.BRPop(0, "list1", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"list1", "c"})
}

func (t *RedisTest) TestBRPopLPush(c *C) {
	t.redisC.RPush("list1", "a", "b", "c").Reply()

	v, err := t.redisC.BRPopLPush("list1", "list2", 0).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "c")
}

func (t *RedisTest) TestLIndex(c *C) {
	t.redisC.LPush("list", "World")
	t.redisC.LPush("list", "Hello")

	v, err := t.redisC.LIndex("list", 0).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "Hello")

	v, err = t.redisC.LIndex("list", -1).Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "World")

	v, err = t.redisC.LIndex("list", 3).Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")
}

func (t *RedisTest) TestLInsert(c *C) {
	t.redisC.RPush("list", "Hello").Reply()
	t.redisC.RPush("list", "World").Reply()

	n, err := t.redisC.LInsert("list", "BEFORE", "World", "There").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(3))

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
	c.Check(values, DeepEquals, []interface{}{"Hello", "There", "World"})
}

func (t *RedisTest) TestLLen(c *C) {
	t.redisC.LPush("list", "World").Reply()
	t.redisC.LPush("list", "Hello").Reply()

	n, err := t.redisC.LLen("list").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))
}

func (t *RedisTest) TestLPop(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	v, err := t.redisC.LPop("list").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "one")

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestLPush(c *C) {
	t.redisC.LPush("list", "World").Reply()
	t.redisC.LPush("list", "Hello").Reply()

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestLPushX(c *C) {
	t.redisC.LPush("list", "World").Reply()

	n, err := t.redisC.LPushX("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.redisC.LPushX("list2", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})

	values, err = t.redisC.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 0)
}

func (t *RedisTest) TestLRange(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	values, err := t.redisC.LRange("list", 0, 0).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 1)
	c.Check(values, DeepEquals, []interface{}{"one"})

	values, err = t.redisC.LRange("list", -3, 2).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.redisC.LRange("list", -100, 100).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
	c.Check(values, DeepEquals, []interface{}{"one", "two", "three"})

	values, err = t.redisC.LRange("list", 5, 10).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 0)
}

func (t *RedisTest) TestLRem(c *C) {
	t.redisC.RPush("list", "hello").Reply()
	t.redisC.RPush("list", "hello").Reply()
	t.redisC.RPush("list", "foo").Reply()
	t.redisC.RPush("list", "hello").Reply()

	n, err := t.redisC.LRem("list", -2, "hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"hello", "foo"})
}

func (t *RedisTest) TestLSet(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	ok, err := t.redisC.LSet("list", 0, "four").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	ok, err = t.redisC.LSet("list", -2, "five").Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
	c.Check(values, DeepEquals, []interface{}{"four", "five", "three"})
}

func (t *RedisTest) TestLTrim(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	ok, err := t.redisC.LTrim("list", 1, -1).Reply()
	c.Check(err, IsNil)
	c.Check(ok, Equals, "OK")

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestRPop(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	v, err := t.redisC.RPop("list").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "three")

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"one", "two"})
}

func (t *RedisTest) TestRPopLPush(c *C) {
	t.redisC.RPush("list", "one").Reply()
	t.redisC.RPush("list", "two").Reply()
	t.redisC.RPush("list", "three").Reply()

	v, err := t.redisC.RPopLPush("list", "list2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "three")

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"one", "two"})

	values, err = t.redisC.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 1)
	c.Check(values, DeepEquals, []interface{}{"three"})
}

func (t *RedisTest) TestRPush(c *C) {
	n, err := t.redisC.RPush("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.RPush("list", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestRPushX(c *C) {
	n, err := t.redisC.RPush("list", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.RPushX("list", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	n, err = t.redisC.RPushX("list2", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.redisC.LRange("list", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"Hello", "World"})

	values, err = t.redisC.LRange("list2", 0, -1).Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 0)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestSAdd(c *C) {
	n, err := t.redisC.SAdd("set", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	members, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(members, HasLen, 2)
	c.Check(members[0], Equals, "World")
	c.Check(members[1], Equals, "Hello")
}

func (t *RedisTest) TestSCard(c *C) {
	n, err := t.redisC.SAdd("set", "Hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.SAdd("set", "World").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	card, err := t.redisC.SCard("set").Reply()
	c.Check(err, IsNil)
	c.Check(card, Equals, int64(2))
}

func (t *RedisTest) TestSDiff(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	values, err := t.redisC.SDiff("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSDiffStore(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	n, err := t.redisC.SDiffStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(2))

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSInter(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	values, err := t.redisC.SInter("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 1)
	c.Check(values, DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestSInterStore(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	n, err := t.redisC.SInterStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 1)
	c.Check(values, DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestIsMember(c *C) {
	t.redisC.SAdd("set", "one").Reply()

	isMember, err := t.redisC.SIsMember("set", "one").Reply()
	c.Check(err, IsNil)
	c.Check(isMember, Equals, true)

	isMember, err = t.redisC.SIsMember("set", "two").Reply()
	c.Check(err, IsNil)
	c.Check(isMember, Equals, false)
}

func (t *RedisTest) TestSMembers(c *C) {
	t.redisC.SAdd("set", "Hello").Reply()
	t.redisC.SAdd("set", "World").Reply()

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSMove(c *C) {
	t.redisC.SAdd("set1", "one").Reply()
	t.redisC.SAdd("set1", "two").Reply()

	t.redisC.SAdd("set2", "three").Reply()

	isMoved, err := t.redisC.SMove("set1", "set2", "two").Reply()
	c.Check(err, IsNil)
	c.Check(isMoved, Equals, true)

	values, err := t.redisC.SMembers("set1").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 1)
	c.Check(values, DeepEquals, []interface{}{"one"})

	values, err = t.redisC.SMembers("set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSPop(c *C) {
	t.redisC.SAdd("set", "one").Reply()
	t.redisC.SAdd("set", "two").Reply()
	t.redisC.SAdd("set", "three").Reply()

	v, err := t.redisC.SPop("set").Reply()
	c.Check(err, IsNil)
	c.Check(v, Not(Equals), "")

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
}

func (t *RedisTest) TestSRandMember(c *C) {
	t.redisC.SAdd("set", "one").Reply()
	t.redisC.SAdd("set", "two").Reply()
	t.redisC.SAdd("set", "three").Reply()

	v, err := t.redisC.SRandMember("set").Reply()
	c.Check(err, IsNil)
	c.Check(v, Not(Equals), "")

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 3)
}

func (t *RedisTest) TestSRem(c *C) {
	t.redisC.SAdd("set", "one").Reply()
	t.redisC.SAdd("set", "two").Reply()
	t.redisC.SAdd("set", "three").Reply()

	n, err := t.redisC.SRem("set", "one").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.SRem("set", "four").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(0))

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 2)
	c.Check(values, DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSUnion(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	values, err := t.redisC.SUnion("set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 5)
}

func (t *RedisTest) TestSUnionStore(c *C) {
	t.redisC.SAdd("set1", "a").Reply()
	t.redisC.SAdd("set1", "b").Reply()
	t.redisC.SAdd("set1", "c").Reply()

	t.redisC.SAdd("set2", "c").Reply()
	t.redisC.SAdd("set2", "d").Reply()
	t.redisC.SAdd("set2", "e").Reply()

	n, err := t.redisC.SUnionStore("set", "set1", "set2").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(5))

	values, err := t.redisC.SMembers("set").Reply()
	c.Check(err, IsNil)
	c.Check(values, HasLen, 5)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestPubSub(c *C) {
	pubsub := t.redisC.PubSubClient()

	ch, err := pubsub.Subscribe("mychannel")
	c.Check(err, IsNil)
	c.Check(ch, Not(Equals), nil)

	ch, err = pubsub.Subscribe("mychannel2")
	c.Check(err, IsNil)
	c.Check(ch, Not(Equals), nil)

	n, err := t.redisC.Publish("mychannel", "hello").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	n, err = t.redisC.Publish("mychannel2", "hello2").Reply()
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

func (t *RedisTest) TestDiscard(c *C) {
	multiC := t.redisC.Multi()

	multiC.Set("foo1", "bar1")
	multiC.Discard()
	multiC.Set("foo2", "bar2")

	reqs, err := multiC.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 1)

	v, err := t.redisC.Get("foo1").Reply()
	c.Check(err, Equals, redis.Nil)
	c.Check(v, Equals, "")

	v, err = t.redisC.Get("foo2").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar2")
}

func (t *RedisTest) TestMultiExec(c *C) {
	multiC := t.redisC.Multi()

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

//------------------------------------------------------------------------------

func (t *RedisTest) TestConcAccess(c *C) {
	for i := int64(0); i < 99; i++ {
		go func() {
			msg := "echo" + strconv.FormatInt(i, 10)
			echo, err := t.redisC.Echo(msg).Reply()
			c.Check(err, IsNil)
			c.Check(echo, Equals, msg)
		}()
	}
}

//------------------------------------------------------------------------------

func (t *RedisTest) BenchmarkRedisPing(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		pong, err := t.redisC.Ping().Reply()
		c.Check(err, IsNil)
		c.Check(pong, Equals, "PONG")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.redisC.Ping().Reply()
	}
}

func (t *RedisTest) BenchmarkRedisSet(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		ok, err := t.redisC.Set("foo", "bar").Reply()
		c.Check(err, IsNil)
		c.Check(ok, Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.redisC.Set("foo", "bar").Reply()
	}
}

func (t *RedisTest) BenchmarkRedisGet(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		v, err := t.redisC.Get("foo").Reply()
		c.Check(err, Equals, redis.Nil)
		c.Check(v, Equals, "")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.redisC.Get("foo").Reply()
	}
}

func (t *RedisTest) BenchmarkRedisWriteRead(c *C) {
	c.StopTimer()

	req := []byte("PING\r\n")
	rd := bufreader.NewSizedReader(1024)

	for i := 0; i < 10; i++ {
		err := t.redisC.WriteRead(req, rd)
		c.Check(err, IsNil)
		c.Check(rd.Bytes(), DeepEquals, []byte("+PONG\r\n"))
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.redisC.WriteRead(req, rd)
	}
}
