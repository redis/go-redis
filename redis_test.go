package redis_test

import (
	"io"
	"net"
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
	t.client = redis.NewTCPClient(":6379", "", -1)
	c.Check(t.client.Flushdb().Err(), IsNil)

	t.multiClient = t.client.Multi()
}

func (t *RedisTest) TearDownTest(c *C) {
	c.Check(t.client.Flushdb().Err(), IsNil)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestInitConn(c *C) {
	openConn := func() (io.ReadWriter, error) {
		return net.Dial("tcp", ":6379")
	}

	isInitConnCalled := false
	initConn := func(client *redis.Client) error {
		isInitConnCalled = true
		return nil
	}

	client := redis.NewClient(openConn, nil, initConn)
	ping := client.Ping()
	c.Check(ping.Err(), IsNil)
	c.Check(ping.Val(), Equals, "PONG")
	c.Check(isInitConnCalled, Equals, true)
}

func (t *RedisTest) TestRunWithouthCheckingErrVal(c *C) {
	set := t.client.Set("foo", "bar")

	get := t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")

	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAuth(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestEcho(c *C) {
	echo := t.client.Echo("hello")
	c.Check(echo.Err(), IsNil)
	c.Check(echo.Val(), Equals, "hello")
}

func (t *RedisTest) TestPing(c *C) {
	ping := t.client.Ping()
	c.Check(ping.Err(), IsNil)
	c.Check(ping.Val(), Equals, "PONG")
}

func (t *RedisTest) TestSelect(c *C) {
	sel := t.client.Select(1)
	c.Check(sel.Err(), IsNil)
	c.Check(sel.Val(), Equals, "OK")
}

// //------------------------------------------------------------------------------

func (t *RedisTest) TestDel(c *C) {
	t.client.Set("key1", "Hello")
	t.client.Set("key2", "World")

	del := t.client.Del("key1", "key2", "key3")
	c.Check(del.Err(), IsNil)
	c.Check(del.Val(), Equals, int64(2))
}

func (t *RedisTest) TestDump(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestExists(c *C) {
	t.client.Set("key1", "Hello")

	exists := t.client.Exists("key1")
	c.Check(exists.Err(), IsNil)
	c.Check(exists.Val(), Equals, true)

	exists = t.client.Exists("key2")
	c.Check(exists.Err(), IsNil)
	c.Check(exists.Val(), Equals, false)
}

func (t *RedisTest) TestExpire(c *C) {
	t.client.Set("key", "Hello")

	expire := t.client.Expire("key", 10)
	c.Check(expire.Err(), IsNil)
	c.Check(expire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(10))

	set := t.client.Set("key", "Hello World")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	ttl = t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestExpireAt(c *C) {
	t.client.Set("key", "Hello")

	exists := t.client.Exists("key")
	c.Check(exists.Err(), IsNil)
	c.Check(exists.Val(), Equals, true)

	expireAt := t.client.ExpireAt("key", 1293840000)
	c.Check(expireAt.Err(), IsNil)
	c.Check(expireAt.Val(), Equals, true)

	exists = t.client.Exists("key")
	c.Check(exists.Err(), IsNil)
	c.Check(exists.Val(), Equals, false)
}

func (t *RedisTest) TestKeys(c *C) {
	t.client.MSet("one", "1", "two", "2", "three", "3", "four", "4")

	keys := t.client.Keys("*o*")
	c.Check(keys.Err(), IsNil)
	c.Check(keys.Val(), DeepEquals, []interface{}{"four", "two", "one"})

	keys = t.client.Keys("t??")
	c.Check(keys.Err(), IsNil)
	c.Check(keys.Val(), DeepEquals, []interface{}{"two"})

	keys = t.client.Keys("*")
	c.Check(keys.Err(), IsNil)
	c.Check(keys.Val(), DeepEquals, []interface{}{"four", "three", "two", "one"})
}

func (t *RedisTest) TestMigrate(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestMove(c *C) {
	move := t.client.Move("foo", 1)
	c.Check(move.Err(), IsNil)
	c.Check(move.Val(), Equals, false)

	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	move = t.client.Move("foo", 1)
	c.Check(move.Err(), IsNil)
	c.Check(move.Val(), Equals, true)

	get := t.client.Get("foo")
	c.Check(get.Err(), Equals, redis.Nil)
	c.Check(get.Val(), Equals, "")

	sel := t.client.Select(1)
	c.Check(sel.Err(), IsNil)
	c.Check(sel.Val(), Equals, "OK")

	get = t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestObject(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	refCount := t.client.ObjectRefCount("foo")
	c.Check(refCount.Err(), IsNil)
	c.Check(refCount.Val(), Equals, int64(1))

	enc := t.client.ObjectEncoding("foo")
	c.Check(enc.Err(), IsNil)
	c.Check(enc.Val(), Equals, "raw")

	idleTime := t.client.ObjectIdleTime("foo")
	c.Check(idleTime.Err(), IsNil)
	c.Check(idleTime.Val(), Equals, int64(0))
}

func (t *RedisTest) TestPersist(c *C) {
	set := t.client.Set("key", "Hello")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	expire := t.client.Expire("key", 10)
	c.Check(expire.Err(), IsNil)
	c.Check(expire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(10))

	persist := t.client.Persist("key")
	c.Check(persist.Err(), IsNil)
	c.Check(persist.Val(), Equals, true)

	ttl = t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestPExpire(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	pexpire := t.client.PExpire("key", 1500)
	c.Check(pexpire.Err(), IsNil)
	c.Check(pexpire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, 1)

	pttl := t.client.PTTL("key")
	c.Check(pttl.Err(), IsNil)
	c.Check(pttl.Val(), Equals, 1500)
}

func (t *RedisTest) TestPExpireAt(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	pExpireAt := t.client.PExpireAt("key", 1555555555005)
	c.Check(pExpireAt.Err(), IsNil)
	c.Check(pExpireAt.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, 211915059)

	pttl := t.client.PTTL("key")
	c.Check(pttl.Err(), IsNil)
	c.Check(pttl.Val(), Equals, int64(211915059461))
}

func (t *RedisTest) TestPTTL(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	expire := t.client.Expire("key", 1)
	c.Check(expire.Err(), IsNil)
	c.Check(set.Val(), Equals, true)

	pttl := t.client.PTTL("key")
	c.Check(pttl.Err(), IsNil)
	c.Check(pttl.Val(), Equals, int64(999))
}

func (t *RedisTest) TestRandomKey(c *C) {
	randomKey := t.client.RandomKey()
	c.Check(randomKey.Err(), Equals, redis.Nil)
	c.Check(randomKey.Val(), Equals, "")

	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	randomKey = t.client.RandomKey()
	c.Check(randomKey.Err(), IsNil)
	c.Check(randomKey.Val(), Equals, "foo")
}

func (t *RedisTest) TestRename(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	status := t.client.Rename("foo", "foo1")
	c.Check(status.Err(), IsNil)
	c.Check(status.Val(), Equals, "OK")

	get := t.client.Get("foo1")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestRenameNX(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	renameNX := t.client.RenameNX("foo", "foo1")
	c.Check(renameNX.Err(), IsNil)
	c.Check(renameNX.Val(), Equals, true)

	get := t.client.Get("foo1")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestRestore(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSort(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestTTL(c *C) {
	ttl := t.client.TTL("foo")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(-1))

	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	expire := t.client.Expire("foo", 60)
	c.Check(expire.Err(), IsNil)
	c.Check(expire.Val(), Equals, true)

	ttl = t.client.TTL("foo")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(60))
}

func (t *RedisTest) Type(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	type_ := t.client.Type("foo")
	c.Check(type_.Err(), IsNil)
	c.Check(type_.Val(), Equals, "string")
}

// //------------------------------------------------------------------------------

func (t *RedisTest) TestAppend(c *C) {
	append := t.client.Append("foo", "bar")
	c.Check(append.Err(), IsNil)
	c.Check(append.Val(), Equals, int64(3))
}

func (t *RedisTest) TestDecr(c *C) {
	decr := t.client.Decr("foo")
	c.Check(decr.Err(), IsNil)
	c.Check(decr.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestDecrBy(c *C) {
	decrBy := t.client.DecrBy("foo", 10)
	c.Check(decrBy.Err(), IsNil)
	c.Check(decrBy.Val(), Equals, int64(-10))
}

func (t *RedisTest) TestGet(c *C) {
	get := t.client.Get("foo")
	c.Check(get.Err(), Equals, redis.Nil)
	c.Check(get.Val(), Equals, "")
}

func (t *RedisTest) TestSetGetBig(c *C) {
	getBit := t.client.GetBit("foo", 5)
	c.Check(getBit.Err(), IsNil)
	c.Check(getBit.Val(), Equals, int64(0))

	setBit := t.client.SetBit("foo", 5, 1)
	c.Check(setBit.Err(), IsNil)
	c.Check(setBit.Val(), Equals, int64(0))

	getBit = t.client.GetBit("foo", 5)
	c.Check(getBit.Err(), IsNil)
	c.Check(getBit.Val(), Equals, int64(1))
}

func (t *RedisTest) TestGetRange(c *C) {
	set := t.client.Set("foo", "hello")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	getRange := t.client.GetRange("foo", 0, 1)
	c.Check(getRange.Err(), IsNil)
	c.Check(getRange.Val(), Equals, "he")
}

func (t *RedisTest) TestGetSet(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	getSet := t.client.GetSet("foo", "bar2")
	c.Check(getSet.Err(), IsNil)
	c.Check(getSet.Val(), Equals, "bar")

	get := t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar2")
}

func (t *RedisTest) TestIncr(c *C) {
	incr := t.client.Incr("foo")
	c.Check(incr.Err(), IsNil)
	c.Check(incr.Val(), Equals, int64(1))
}

func (t *RedisTest) TestIncrBy(c *C) {
	incrBy := t.client.IncrBy("foo", 10)
	c.Check(incrBy.Err(), IsNil)
	c.Check(incrBy.Val(), Equals, int64(10))
}

func (t *RedisTest) TestMsetMget(c *C) {
	mSet := t.client.MSet("foo1", "bar1", "foo2", "bar2")
	c.Check(mSet.Err(), IsNil)
	c.Check(mSet.Val(), Equals, "OK")

	mGet := t.client.MGet("foo1", "foo2")
	c.Check(mGet.Err(), IsNil)
	c.Check(mGet.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
}

func (t *RedisTest) MSetNX(c *C) {
	mSetNX := t.client.MSetNX("foo1", "bar1", "foo2", "bar2")
	c.Check(mSetNX.Err(), IsNil)
	c.Check(mSetNX.Val(), Equals, true)

	mSetNX = t.client.MSetNX("foo1", "bar1", "foo2", "bar2")
	c.Check(mSetNX.Err(), IsNil)
	c.Check(mSetNX.Val(), Equals, false)
}

func (t *RedisTest) PSetEx(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSetGet(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	get := t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestSetEx(c *C) {
	setEx := t.client.SetEx("foo", 10, "bar")
	c.Check(setEx.Err(), IsNil)
	c.Check(setEx.Val(), Equals, "OK")

	ttl := t.client.TTL("foo")
	c.Check(ttl.Err(), IsNil)
	c.Check(ttl.Val(), Equals, int64(10))
}

func (t *RedisTest) TestSetNX(c *C) {
	setNX := t.client.SetNX("foo", "bar")
	c.Check(setNX.Err(), IsNil)
	c.Check(setNX.Val(), Equals, true)

	setNX = t.client.SetNX("foo", "bar2")
	c.Check(setNX.Err(), IsNil)
	c.Check(setNX.Val(), Equals, false)

	get := t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestSetRange(c *C) {
	set := t.client.Set("foo", "Hello World")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	range_ := t.client.SetRange("foo", 6, "Redis")
	c.Check(range_.Err(), IsNil)
	c.Check(range_.Val(), Equals, int64(11))

	get := t.client.Get("foo")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "Hello Redis")
}

func (t *RedisTest) TestStrLen(c *C) {
	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	strLen := t.client.StrLen("foo")
	c.Check(strLen.Err(), IsNil)
	c.Check(strLen.Val(), Equals, int64(3))

	strLen = t.client.StrLen("_")
	c.Check(strLen.Err(), IsNil)
	c.Check(strLen.Val(), Equals, int64(0))
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestHDel(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Check(hSet.Err(), IsNil)

	hDel := t.client.HDel("myhash", "foo")
	c.Check(hDel.Err(), IsNil)
	c.Check(hDel.Val(), Equals, int64(1))

	hDel = t.client.HDel("myhash", "foo")
	c.Check(hDel.Err(), IsNil)
	c.Check(hDel.Val(), Equals, int64(0))
}

func (t *RedisTest) TestHExists(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Check(hSet.Err(), IsNil)

	hExists := t.client.HExists("myhash", "foo")
	c.Check(hExists.Err(), IsNil)
	c.Check(hExists.Val(), Equals, true)

	hExists = t.client.HExists("myhash", "foo1")
	c.Check(hExists.Err(), IsNil)
	c.Check(hExists.Val(), Equals, false)
}

func (t *RedisTest) TestHGet(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Check(hSet.Err(), IsNil)

	hGet := t.client.HGet("myhash", "foo")
	c.Check(hGet.Err(), IsNil)
	c.Check(hGet.Val(), Equals, "bar")

	hGet = t.client.HGet("myhash", "foo1")
	c.Check(hGet.Err(), Equals, redis.Nil)
	c.Check(hGet.Val(), Equals, "")
}

func (t *RedisTest) TestHGetAll(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Check(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Check(hSet.Err(), IsNil)

	hGetAll := t.client.HGetAll("myhash")
	c.Check(hGetAll.Err(), IsNil)
	c.Check(hGetAll.Val(), DeepEquals, []interface{}{"foo1", "bar1", "foo2", "bar2"})
}

func (t *RedisTest) TestHIncrBy(c *C) {
	hSet := t.client.HSet("myhash", "foo", "5")
	c.Check(hSet.Err(), IsNil)

	hIncrBy := t.client.HIncrBy("myhash", "foo", 1)
	c.Check(hIncrBy.Err(), IsNil)
	c.Check(hIncrBy.Val(), Equals, int64(6))

	hIncrBy = t.client.HIncrBy("myhash", "foo", -1)
	c.Check(hIncrBy.Err(), IsNil)
	c.Check(hIncrBy.Val(), Equals, int64(5))

	hIncrBy = t.client.HIncrBy("myhash", "foo", -10)
	c.Check(hIncrBy.Err(), IsNil)
	c.Check(hIncrBy.Val(), Equals, int64(-5))
}

func (t *RedisTest) TestHIncrByFloat(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestHKeys(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Check(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Check(hSet.Err(), IsNil)

	hKeys := t.client.HKeys("myhash")
	c.Check(hKeys.Err(), IsNil)
	c.Check(hKeys.Val(), DeepEquals, []interface{}{"foo1", "foo2"})
}

func (t *RedisTest) TestHLen(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Check(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Check(hSet.Err(), IsNil)

	hLen := t.client.HLen("myhash")
	c.Check(hLen.Err(), IsNil)
	c.Check(hLen.Val(), Equals, int64(2))
}

func (t *RedisTest) TestHMGet(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Check(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Check(hSet.Err(), IsNil)

	hMGet := t.client.HMGet("myhash", "foo1", "foo2", "_")
	c.Check(hMGet.Err(), IsNil)
	c.Check(hMGet.Val(), DeepEquals, []interface{}{"bar1", "bar2", nil})
}

func (t *RedisTest) TestHMSet(c *C) {
	hMSet := t.client.HMSet("myhash", "foo1", "bar1", "foo2", "bar2")
	c.Check(hMSet.Err(), IsNil)
	c.Check(hMSet.Val(), Equals, "OK")

	hGet := t.client.HGet("myhash", "foo1")
	c.Check(hGet.Err(), IsNil)
	c.Check(hGet.Val(), Equals, "bar1")

	hGet = t.client.HGet("myhash", "foo2")
	c.Check(hGet.Err(), IsNil)
	c.Check(hGet.Val(), Equals, "bar2")
}

func (t *RedisTest) TestHSet(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Check(hSet.Err(), IsNil)
	c.Check(hSet.Val(), Equals, true)

	hGet := t.client.HGet("myhash", "foo")
	c.Check(hGet.Err(), IsNil)
	c.Check(hGet.Val(), Equals, "bar")
}

func (t *RedisTest) TestHSetNX(c *C) {
	hSetNX := t.client.HSetNX("myhash", "foo", "bar")
	c.Check(hSetNX.Err(), IsNil)
	c.Check(hSetNX.Val(), Equals, true)

	hSetNX = t.client.HSetNX("myhash", "foo", "bar")
	c.Check(hSetNX.Err(), IsNil)
	c.Check(hSetNX.Val(), Equals, false)

	hGet := t.client.HGet("myhash", "foo")
	c.Check(hGet.Err(), IsNil)
	c.Check(hGet.Val(), Equals, "bar")
}

func (t *RedisTest) TestHVals(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Check(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Check(hSet.Err(), IsNil)

	hVals := t.client.HVals("myhash")
	c.Check(hVals.Err(), IsNil)
	c.Check(hVals.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestBLPop(c *C) {
	rPush := t.client.RPush("list1", "a", "b", "c")
	c.Check(rPush.Err(), IsNil)

	bLPop := t.client.BLPop(0, "list1", "list2")
	c.Check(bLPop.Err(), IsNil)
	c.Check(bLPop.Val(), DeepEquals, []interface{}{"list1", "a"})
}

func (t *RedisTest) TestBLPopBlocks(c *C) {
	started := make(chan bool)
	done := make(chan bool)
	go func() {
		started <- true
		bLPop := t.client.BLPop(0, "list")
		c.Check(bLPop.Err(), IsNil)
		c.Check(bLPop.Val(), DeepEquals, []interface{}{"list", "a"})
		done <- true
	}()
	<-started

	select {
	case <-done:
		c.Error("BLPop is not blocked")
	case <-time.After(time.Second):
		// ok
	}

	rPush := t.client.RPush("list", "a")
	c.Check(rPush.Err(), IsNil)

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		c.Error("BLPop is still blocked")
		// ok
	}
}

func (t *RedisTest) TestBRPop(c *C) {
	rPush := t.client.RPush("list1", "a", "b", "c")
	c.Check(rPush.Err(), IsNil)

	bRPop := t.client.BRPop(0, "list1", "list2")
	c.Check(bRPop.Err(), IsNil)
	c.Check(bRPop.Val(), DeepEquals, []interface{}{"list1", "c"})
}

func (t *RedisTest) TestBRPopBlocks(c *C) {
	started := make(chan bool)
	done := make(chan bool)
	go func() {
		started <- true
		bRPop := t.client.BRPop(0, "list")
		c.Check(bRPop.Err(), IsNil)
		c.Check(bRPop.Val(), DeepEquals, []interface{}{"list", "a"})
		done <- true
	}()
	<-started

	select {
	case <-done:
		c.Error("BRPop is not blocked")
	case <-time.After(time.Second):
		// ok
	}

	rPush := t.client.RPush("list", "a")
	c.Check(rPush.Err(), IsNil)

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		c.Error("BRPop is still blocked")
		// ok
	}
}

func (t *RedisTest) TestBRPopLPush(c *C) {
	rPush := t.client.RPush("list1", "a", "b", "c")
	c.Check(rPush.Err(), IsNil)

	bRPopLPush := t.client.BRPopLPush("list1", "list2", 0)
	c.Check(bRPopLPush.Err(), IsNil)
	c.Check(bRPopLPush.Val(), Equals, "c")
}

func (t *RedisTest) TestLIndex(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Check(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Check(lPush.Err(), IsNil)

	lIndex := t.client.LIndex("list", 0)
	c.Check(lIndex.Err(), IsNil)
	c.Check(lIndex.Val(), Equals, "Hello")

	lIndex = t.client.LIndex("list", -1)
	c.Check(lIndex.Err(), IsNil)
	c.Check(lIndex.Val(), Equals, "World")

	lIndex = t.client.LIndex("list", 3)
	c.Check(lIndex.Err(), Equals, redis.Nil)
	c.Check(lIndex.Val(), Equals, "")
}

func (t *RedisTest) TestLInsert(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "World")
	c.Check(rPush.Err(), IsNil)

	lInsert := t.client.LInsert("list", "BEFORE", "World", "There")
	c.Check(lInsert.Err(), IsNil)
	c.Check(lInsert.Val(), Equals, int64(3))

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"Hello", "There", "World"})
}

func (t *RedisTest) TestLLen(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Check(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Check(lPush.Err(), IsNil)

	lLen := t.client.LLen("list")
	c.Check(lLen.Err(), IsNil)
	c.Check(lLen.Val(), Equals, int64(2))
}

func (t *RedisTest) TestLPop(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	lPop := t.client.LPop("list")
	c.Check(lPop.Err(), IsNil)
	c.Check(lPop.Val(), Equals, "one")

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestLPush(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Check(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Check(lPush.Err(), IsNil)

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestLPushX(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Check(lPush.Err(), IsNil)

	lPushX := t.client.LPushX("list", "Hello")
	c.Check(lPushX.Err(), IsNil)
	c.Check(lPushX.Val(), Equals, int64(2))

	lPushX = t.client.LPushX("list2", "Hello")
	c.Check(lPushX.Err(), IsNil)
	c.Check(lPushX.Val(), Equals, int64(0))

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRange(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	lRange := t.client.LRange("list", 0, 0)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"one"})

	lRange = t.client.LRange("list", -3, 2)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	lRange = t.client.LRange("list", -100, 100)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	lRange = t.client.LRange("list", 5, 10)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRem(c *C) {
	rPush := t.client.RPush("list", "hello")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "hello")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "foo")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "hello")
	c.Check(rPush.Err(), IsNil)

	lRem := t.client.LRem("list", -2, "hello")
	c.Check(lRem.Err(), IsNil)
	c.Check(lRem.Val(), Equals, int64(2))

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"hello", "foo"})
}

func (t *RedisTest) TestLSet(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	lSet := t.client.LSet("list", 0, "four")
	c.Check(lSet.Err(), IsNil)
	c.Check(lSet.Val(), Equals, "OK")

	lSet = t.client.LSet("list", -2, "five")
	c.Check(lSet.Err(), IsNil)
	c.Check(lSet.Val(), Equals, "OK")

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"four", "five", "three"})
}

func (t *RedisTest) TestLTrim(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	lTrim := t.client.LTrim("list", 1, -1)
	c.Check(lTrim.Err(), IsNil)
	c.Check(lTrim.Val(), Equals, "OK")

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestRPop(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	rPop := t.client.RPop("list")
	c.Check(rPop.Err(), IsNil)
	c.Check(rPop.Val(), Equals, "three")

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"one", "two"})
}

func (t *RedisTest) TestRPopLPush(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Check(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Check(rPush.Err(), IsNil)

	rPopLPush := t.client.RPopLPush("list", "list2")
	c.Check(rPopLPush.Err(), IsNil)
	c.Check(rPopLPush.Val(), Equals, "three")

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"one", "two"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"three"})
}

func (t *RedisTest) TestRPush(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Check(rPush.Err(), IsNil)
	c.Check(rPush.Val(), Equals, int64(1))

	rPush = t.client.RPush("list", "World")
	c.Check(rPush.Err(), IsNil)
	c.Check(rPush.Val(), Equals, int64(2))

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestRPushX(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Check(rPush.Err(), IsNil)
	c.Check(rPush.Val(), Equals, int64(1))

	rPushX := t.client.RPushX("list", "World")
	c.Check(rPushX.Err(), IsNil)
	c.Check(rPushX.Val(), Equals, int64(2))

	rPushX = t.client.RPushX("list2", "World")
	c.Check(rPushX.Err(), IsNil)
	c.Check(rPushX.Val(), Equals, int64(0))

	lRange := t.client.LRange("list", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Check(lRange.Err(), IsNil)
	c.Check(lRange.Val(), DeepEquals, []interface{}{})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestSAdd(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Check(sAdd.Err(), IsNil)
	c.Check(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Check(sAdd.Err(), IsNil)
	c.Check(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Check(sAdd.Err(), IsNil)
	c.Check(sAdd.Val(), Equals, int64(0))

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSCard(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Check(sAdd.Err(), IsNil)
	c.Check(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Check(sAdd.Err(), IsNil)
	c.Check(sAdd.Val(), Equals, int64(1))

	sCard := t.client.SCard("set")
	c.Check(sCard.Err(), IsNil)
	c.Check(sCard.Val(), Equals, int64(2))
}

func (t *RedisTest) TestSDiff(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sDiff := t.client.SDiff("set1", "set2")
	c.Check(sDiff.Err(), IsNil)
	c.Check(sDiff.Val(), DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSDiffStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sDiffStore := t.client.SDiffStore("set", "set1", "set2")
	c.Check(sDiffStore.Err(), IsNil)
	c.Check(sDiffStore.Val(), Equals, int64(2))

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSInter(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sInter := t.client.SInter("set1", "set2")
	c.Check(sInter.Err(), IsNil)
	c.Check(sInter.Val(), DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestSInterStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sInterStore := t.client.SInterStore("set", "set1", "set2")
	c.Check(sInterStore.Err(), IsNil)
	c.Check(sInterStore.Val(), Equals, int64(1))

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestIsMember(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Check(sAdd.Err(), IsNil)

	sIsMember := t.client.SIsMember("set", "one")
	c.Check(sIsMember.Err(), IsNil)
	c.Check(sIsMember.Val(), Equals, true)

	sIsMember = t.client.SIsMember("set", "two")
	c.Check(sIsMember.Err(), IsNil)
	c.Check(sIsMember.Val(), Equals, false)
}

func (t *RedisTest) TestSMembers(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "World")
	c.Check(sAdd.Err(), IsNil)

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSMove(c *C) {
	sAdd := t.client.SAdd("set1", "one")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "two")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "three")
	c.Check(sAdd.Err(), IsNil)

	sMove := t.client.SMove("set1", "set2", "two")
	c.Check(sMove.Err(), IsNil)
	c.Check(sMove.Val(), Equals, true)

	sMembers := t.client.SMembers("set1")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"one"})

	sMembers = t.client.SMembers("set2")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSPop(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Check(sAdd.Err(), IsNil)

	sPop := t.client.SPop("set")
	c.Check(sPop.Err(), IsNil)
	c.Check(sPop.Val(), Not(Equals), "")

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), HasLen, 2)
}

func (t *RedisTest) TestSRandMember(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Check(sAdd.Err(), IsNil)

	sRandMember := t.client.SRandMember("set")
	c.Check(sRandMember.Err(), IsNil)
	c.Check(sRandMember.Val(), Not(Equals), "")

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), HasLen, 3)
}

func (t *RedisTest) TestSRem(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Check(sAdd.Err(), IsNil)

	sRem := t.client.SRem("set", "one")
	c.Check(sRem.Err(), IsNil)
	c.Check(sRem.Val(), Equals, int64(1))

	sRem = t.client.SRem("set", "four")
	c.Check(sRem.Err(), IsNil)
	c.Check(sRem.Val(), Equals, int64(0))

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSUnion(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sUnion := t.client.SUnion("set1", "set2")
	c.Check(sUnion.Err(), IsNil)
	c.Check(sUnion.Val(), HasLen, 5)
}

func (t *RedisTest) TestSUnionStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Check(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Check(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Check(sAdd.Err(), IsNil)

	sUnionStore := t.client.SUnionStore("set", "set1", "set2")
	c.Check(sUnionStore.Err(), IsNil)
	c.Check(sUnionStore.Val(), Equals, int64(5))

	sMembers := t.client.SMembers("set")
	c.Check(sMembers.Err(), IsNil)
	c.Check(sMembers.Val(), HasLen, 5)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestZAdd(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	c.Check(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(1, "uno"))
	c.Check(zAdd.Err(), IsNil)
	c.Check(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	c.Check(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "two"))
	c.Check(zAdd.Err(), IsNil)
	c.Check(zAdd.Val(), Equals, int64(0))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"one", "1", "uno", "1", "two", "3"})
}

func (t *RedisTest) TestZCard(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)

	zCard := t.client.ZCard("zset")
	c.Check(zCard.Err(), IsNil)
	c.Check(zCard.Val(), Equals, int64(2))
}

func (t *RedisTest) TestZCount(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zCount := t.client.ZCount("zset", "-inf", "+inf")
	c.Check(zCount.Err(), IsNil)
	c.Check(zCount.Val(), Equals, int64(3))

	zCount = t.client.ZCount("zset", "(1", "3")
	c.Check(zCount.Err(), IsNil)
	c.Check(zCount.Val(), Equals, int64(2))
}

func (t *RedisTest) TestZIncrBy(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)

	zIncrBy := t.client.ZIncrBy("zset", 2, "one")
	c.Check(zIncrBy.Err(), IsNil)
	c.Check(zIncrBy.Val(), Equals, float64(3))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"two", "2", "one", "3"})
}

func (t *RedisTest) TestZInterStore(c *C) {
	zAdd := t.client.ZAdd("zset1", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset1", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)

	zAdd = t.client.ZAdd("zset2", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset3", redis.NewZMember(3, "two"))
	c.Check(zAdd.Err(), IsNil)

	zInterStore := t.client.ZInterStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	)
	c.Check(zInterStore.Err(), IsNil)
	c.Check(zInterStore.Val(), Equals, int64(2))

	zRange := t.client.ZRange("out", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"one", "5", "two", "10"})
}

func (t *RedisTest) TestZRange(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRange := t.client.ZRange("zset", 0, -1, false)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	zRange = t.client.ZRange("zset", 2, 3, false)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"three"})

	zRange = t.client.ZRange("zset", -2, -1, false)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestZRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRangeByScore := t.client.ZRangeByScore("zset", "-inf", "+inf", false, nil)
	c.Check(zRangeByScore.Err(), IsNil)
	c.Check(zRangeByScore.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	zRangeByScore = t.client.ZRangeByScore("zset", "1", "2", false, nil)
	c.Check(zRangeByScore.Err(), IsNil)
	c.Check(zRangeByScore.Val(), DeepEquals, []interface{}{"one", "two"})

	zRangeByScore = t.client.ZRangeByScore("zset", "(1", "2", false, nil)
	c.Check(zRangeByScore.Err(), IsNil)
	c.Check(zRangeByScore.Val(), DeepEquals, []interface{}{"two"})

	zRangeByScore = t.client.ZRangeByScore("zset", "(1", "(2", false, nil)
	c.Check(zRangeByScore.Err(), IsNil)
	c.Check(zRangeByScore.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRank := t.client.ZRank("zset", "three")
	c.Check(zRank.Err(), IsNil)
	c.Check(zRank.Val(), Equals, int64(2))

	zRank = t.client.ZRank("zset", "four")
	c.Check(zRank.Err(), Equals, redis.Nil)
	c.Check(zRank.Val(), Equals, int64(0))
}

func (t *RedisTest) TestZRem(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRem := t.client.ZRem("zset", "two")
	c.Check(zRem.Err(), IsNil)
	c.Check(zRem.Val(), Equals, int64(1))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"one", "1", "three", "3"})
}

func (t *RedisTest) TestZRemRangeByRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRemRangeByRank := t.client.ZRemRangeByRank("zset", 0, 1)
	c.Check(zRemRangeByRank.Err(), IsNil)
	c.Check(zRemRangeByRank.Val(), Equals, int64(2))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"three", "3"})
}

func (t *RedisTest) TestZRemRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRemRangeByScore := t.client.ZRemRangeByScore("zset", "-inf", "(2")
	c.Check(zRemRangeByScore.Err(), IsNil)
	c.Check(zRemRangeByScore.Val(), Equals, int64(1))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"two", "2", "three", "3"})
}

func (t *RedisTest) TestZRevRange(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRevRange := t.client.ZRevRange("zset", "0", "-1", false)
	c.Check(zRevRange.Err(), IsNil)
	c.Check(zRevRange.Val(), DeepEquals, []interface{}{"three", "two", "one"})

	zRevRange = t.client.ZRevRange("zset", "2", "3", false)
	c.Check(zRevRange.Err(), IsNil)
	c.Check(zRevRange.Val(), DeepEquals, []interface{}{"one"})

	zRevRange = t.client.ZRevRange("zset", "-2", "-1", false)
	c.Check(zRevRange.Err(), IsNil)
	c.Check(zRevRange.Val(), DeepEquals, []interface{}{"two", "one"})
}

func (t *RedisTest) TestZRevRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRevRangeByScore := t.client.ZRevRangeByScore("zset", "+inf", "-inf", false, nil)
	c.Check(zRevRangeByScore.Err(), IsNil)
	c.Check(zRevRangeByScore.Val(), DeepEquals, []interface{}{"three", "two", "one"})

	zRevRangeByScore = t.client.ZRevRangeByScore("zset", "2", "(1", false, nil)
	c.Check(zRevRangeByScore.Err(), IsNil)
	c.Check(zRevRangeByScore.Val(), DeepEquals, []interface{}{"two"})

	zRevRangeByScore = t.client.ZRevRangeByScore("zset", "(2", "(1", false, nil)
	c.Check(zRevRangeByScore.Err(), IsNil)
	c.Check(zRevRangeByScore.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRevRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zRevRank := t.client.ZRevRank("zset", "one")
	c.Check(zRevRank.Err(), IsNil)
	c.Check(zRevRank.Val(), Equals, int64(2))

	zRevRank = t.client.ZRevRank("zset", "four")
	c.Check(zRevRank.Err(), Equals, redis.Nil)
	c.Check(zRevRank.Val(), Equals, int64(0))
}

func (t *RedisTest) TestZScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1.001, "one"))
	c.Check(zAdd.Err(), IsNil)

	zScore := t.client.ZScore("zset", "one")
	c.Check(zScore.Err(), IsNil)
	c.Check(zScore.Val(), Equals, float64(1.001))
}

func (t *RedisTest) TestZUnionStore(c *C) {
	zAdd := t.client.ZAdd("zset1", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset1", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)

	zAdd = t.client.ZAdd("zset2", redis.NewZMember(1, "one"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(2, "two"))
	c.Check(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(3, "three"))
	c.Check(zAdd.Err(), IsNil)

	zUnionStore := t.client.ZUnionStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	)
	c.Check(zUnionStore.Err(), IsNil)
	c.Check(zUnionStore.Val(), Equals, int64(3))

	zRange := t.client.ZRange("out", 0, -1, true)
	c.Check(zRange.Err(), IsNil)
	c.Check(zRange.Val(), DeepEquals, []interface{}{"one", "5", "three", "9", "two", "10"})
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

	pub := t.client.Publish("mychannel", "hello")
	c.Check(pub.Err(), IsNil)
	c.Check(pub.Val(), Equals, int64(1))

	pub = t.client.Publish("mychannel2", "hello2")
	c.Check(pub.Err(), IsNil)
	c.Check(pub.Val(), Equals, int64(1))

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
	set := t.client.Set("foo2", "bar2")
	c.Check(set.Err(), IsNil)
	c.Check(set.Val(), Equals, "OK")

	setReq := t.multiClient.Set("foo1", "bar1")
	getReq := t.multiClient.Get("foo2")

	reqs, err := t.multiClient.RunQueued()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 2)

	c.Check(setReq.Err(), IsNil)
	c.Check(setReq.Val(), Equals, "OK")

	c.Check(getReq.Err(), IsNil)
	c.Check(getReq.Val(), Equals, "bar2")
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

	get := t.client.Get("foo1")
	c.Check(get.Err(), Equals, redis.Nil)
	c.Check(get.Val(), Equals, "")

	get = t.client.Get("foo2")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "bar2")
}

func (t *RedisTest) TestMultiExec(c *C) {
	multiC := t.client.Multi()

	setR := multiC.Set("foo", "bar")
	getR := multiC.Get("foo")

	reqs, err := multiC.Exec()
	c.Check(err, IsNil)
	c.Check(reqs, HasLen, 2)

	c.Check(setR.Err(), IsNil)
	c.Check(setR.Val(), Equals, "OK")

	c.Check(getR.Err(), IsNil)
	c.Check(getR.Val(), Equals, "bar")
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
			echo := t.client.Echo(msg)
			c.Check(echo.Err(), IsNil)
			c.Check(echo.Val(), Equals, msg)
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

			c.Check(echo1Req.Err(), IsNil)
			c.Check(echo1Req.Val(), Equals, msg1)

			c.Check(echo2Req.Err(), IsNil)
			c.Check(echo2Req.Val(), Equals, msg2)
		}()
	}
}

func (t *RedisTest) TestIncrFromGoroutines(c *C) {
	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			incr := t.client.Incr("TestIncrFromGoroutinesKey")
			c.Check(incr.Err(), IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	get := t.client.Get("TestIncrFromGoroutinesKey")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "20000")
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
	for _, req := range reqs {
		if req.Err() != nil {
			c.Errorf("got %v, expected nil", req.Err())
		}
	}

	get := t.client.Get("TestIncrPipeliningFromGoroutinesKey")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "20000")
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
	for _, req := range reqs {
		if req.Err() != nil {
			c.Errorf("got %v, expected nil", req.Err())
		}
	}

	get := t.client.Get("TestIncrTransactionKey")
	c.Check(get.Err(), IsNil)
	c.Check(get.Val(), Equals, "20000")
}

//------------------------------------------------------------------------------

func (t *RedisTest) BenchmarkRedisPing(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		ping := t.client.Ping()
		c.Check(ping.Err(), IsNil)
		c.Check(ping.Val(), Equals, "PONG")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Ping()
	}
}

func (t *RedisTest) BenchmarkRedisSet(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		set := t.client.Set("foo", "bar")
		c.Check(set.Err(), IsNil)
		c.Check(set.Val(), Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Set("foo", "bar")
	}
}

func (t *RedisTest) BenchmarkRedisGetNil(c *C) {
	c.StopTimer()

	for i := 0; i < 10; i++ {
		get := t.client.Get("foo")
		c.Check(get.Err(), Equals, redis.Nil)
		c.Check(get.Val(), Equals, "")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo")
	}
}

func (t *RedisTest) BenchmarkRedisGet(c *C) {
	c.StopTimer()

	set := t.client.Set("foo", "bar")
	c.Check(set.Err(), IsNil)

	for i := 0; i < 10; i++ {
		get := t.client.Get("foo")
		c.Check(get.Err(), IsNil)
		c.Check(get.Val(), Equals, "bar")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo")
	}
}

func (t *RedisTest) BenchmarkRedisMGet(c *C) {
	c.StopTimer()

	mSet := t.client.MSet("foo1", "bar1", "foo2", "bar2")
	c.Check(mSet.Err(), IsNil)

	for i := 0; i < 10; i++ {
		mGet := t.client.MGet("foo1", "foo2")
		c.Check(mGet.Err(), IsNil)
		c.Check(mGet.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.MGet("foo1", "foo2")
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
