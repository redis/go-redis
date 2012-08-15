package redis_test

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	. "launchpad.net/gocheck"

	"github.com/vmihailenco/redis"
)

const redisAddr = ":6379"

//------------------------------------------------------------------------------

type RedisTest struct {
	openedConnsCount, closedConnsCount int64
	client                             *redis.Client
}

var _ = Suite(&RedisTest{})

func Test(t *testing.T) { TestingT(t) }

//------------------------------------------------------------------------------

func (t *RedisTest) SetUpTest(c *C) {
	t.openedConnsCount = 0
	openConn := func() (io.ReadWriteCloser, error) {
		t.openedConnsCount++
		return net.Dial("tcp", redisAddr)
	}
	t.closedConnsCount = 0
	closeConn := func(conn io.ReadWriteCloser) error {
		t.closedConnsCount++
		return nil
	}
	t.client = redis.NewClient(openConn, closeConn, nil)
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 10
	c.Assert(t.client.FlushDb().Err(), IsNil)
}

func (t *RedisTest) TearDownTest(c *C) {
	c.Assert(t.client.FlushDb().Err(), IsNil)
	c.Assert(t.client.Close(), IsNil)
	//	c.Assert(t.openedConnsCount, Equals, t.closedConnsCount)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestInitConn(c *C) {
	openConn := func() (io.ReadWriteCloser, error) {
		return net.Dial("tcp", redisAddr)
	}

	isInitConnCalled := false
	initConn := func(client *redis.Client) error {
		isInitConnCalled = true
		return nil
	}

	client := redis.NewClient(openConn, nil, initConn)
	defer func() {
		c.Assert(client.Close(), IsNil)
	}()

	ping := client.Ping()
	c.Assert(ping.Err(), IsNil)
	c.Assert(ping.Val(), Equals, "PONG")
	c.Assert(isInitConnCalled, Equals, true)
}

func (t *RedisTest) TestRunWithouthCheckingErrVal(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")

	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")
}

func (t *RedisTest) TestGetSpecChars(c *C) {
	set := t.client.Set("foo", "bar1\r\nbar2\r\n")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar1\r\nbar2\r\n")
}

func (t *RedisTest) TestGetBigVal(c *C) {
	val := string(bytes.Repeat([]byte{'*'}, 2<<16))

	set := t.client.Set("foo", val)
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, val)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestConnPoolMaxCap(c *C) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			ping := t.client.Ping()
			c.Assert(ping.Err(), IsNil)
			c.Assert(ping.Val(), Equals, "PONG")
			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.client.Close(), IsNil)
	c.Assert(t.openedConnsCount, Equals, int64(10))
	c.Assert(t.closedConnsCount, Equals, int64(10))
}

func (t *RedisTest) TestConnPoolMaxCapOnPipelineClient(c *C) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			pipeline, err := t.client.PipelineClient()
			c.Assert(err, IsNil)

			ping := pipeline.Ping()
			reqs, err := pipeline.RunQueued()
			c.Assert(err, IsNil)
			c.Assert(reqs, HasLen, 1)
			c.Assert(ping.Err(), IsNil)
			c.Assert(ping.Val(), Equals, "PONG")

			c.Assert(pipeline.Close(), IsNil)

			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.client.Close(), IsNil)
	c.Assert(t.openedConnsCount, Equals, int64(10))
	c.Assert(t.closedConnsCount, Equals, int64(10))
}

func (t *RedisTest) TestConnPoolMaxCapOnMultiClient(c *C) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			multi, err := t.client.MultiClient()
			c.Assert(err, IsNil)

			var ping *redis.StatusReq
			reqs, err := multi.Exec(func() {
				ping = multi.Ping()
			})
			c.Assert(err, IsNil)
			c.Assert(reqs, HasLen, 1)
			c.Assert(ping.Err(), IsNil)
			c.Assert(ping.Val(), Equals, "PONG")

			c.Assert(multi.Close(), IsNil)

			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.client.Close(), IsNil)
	c.Assert(t.openedConnsCount, Equals, int64(10))
	c.Assert(t.closedConnsCount, Equals, int64(10))
}

func (t *RedisTest) TestConnPoolMaxCapOnPubSubClient(c *C) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			pubsub, err := t.client.PubSubClient()
			c.Assert(err, IsNil)

			_, err = pubsub.Subscribe()
			c.Assert(err, IsNil)

			c.Assert(pubsub.Close(), IsNil)

			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.client.Close(), IsNil)
	c.Assert(t.openedConnsCount, Equals, int64(1000))
	c.Assert(t.closedConnsCount, Equals, int64(1000))
}

func (t *RedisTest) TestConnPoolRemovesBrokenConn(c *C) {
	conn, err := net.Dial("tcp", redisAddr)
	c.Assert(err, IsNil)
	c.Assert(conn.Close(), IsNil)

	client := redis.NewTCPClient(redisAddr, "", -1)
	client.ConnPool.(*redis.MultiConnPool).MaxCap = 1
	defer func() {
		c.Assert(client.Close(), IsNil)
	}()

	c.Assert(client.ConnPool.Add(redis.NewConn(conn)), IsNil)

	ping := client.Ping()
	c.Assert(ping.Err().Error(), Equals, "use of closed network connection")
	c.Assert(ping.Val(), Equals, "")

	ping = client.Ping()
	c.Assert(ping.Err(), IsNil)
	c.Assert(ping.Val(), Equals, "PONG")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestAuth(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestEcho(c *C) {
	echo := t.client.Echo("hello")
	c.Assert(echo.Err(), IsNil)
	c.Assert(echo.Val(), Equals, "hello")
}

func (t *RedisTest) TestPing(c *C) {
	ping := t.client.Ping()
	c.Assert(ping.Err(), IsNil)
	c.Assert(ping.Val(), Equals, "PONG")
}

func (t *RedisTest) TestSelect(c *C) {
	sel := t.client.Select(1)
	c.Assert(sel.Err(), IsNil)
	c.Assert(sel.Val(), Equals, "OK")
}

// //------------------------------------------------------------------------------

func (t *RedisTest) TestDel(c *C) {
	t.client.Set("key1", "Hello")
	t.client.Set("key2", "World")

	del := t.client.Del("key1", "key2", "key3")
	c.Assert(del.Err(), IsNil)
	c.Assert(del.Val(), Equals, int64(2))
}

func (t *RedisTest) TestDump(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestExists(c *C) {
	t.client.Set("key1", "Hello")

	exists := t.client.Exists("key1")
	c.Assert(exists.Err(), IsNil)
	c.Assert(exists.Val(), Equals, true)

	exists = t.client.Exists("key2")
	c.Assert(exists.Err(), IsNil)
	c.Assert(exists.Val(), Equals, false)
}

func (t *RedisTest) TestExpire(c *C) {
	t.client.Set("key", "Hello")

	expire := t.client.Expire("key", 10)
	c.Assert(expire.Err(), IsNil)
	c.Assert(expire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(10))

	set := t.client.Set("key", "Hello World")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	ttl = t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestExpireAt(c *C) {
	t.client.Set("key", "Hello")

	exists := t.client.Exists("key")
	c.Assert(exists.Err(), IsNil)
	c.Assert(exists.Val(), Equals, true)

	expireAt := t.client.ExpireAt("key", 1293840000)
	c.Assert(expireAt.Err(), IsNil)
	c.Assert(expireAt.Val(), Equals, true)

	exists = t.client.Exists("key")
	c.Assert(exists.Err(), IsNil)
	c.Assert(exists.Val(), Equals, false)
}

func (t *RedisTest) TestKeys(c *C) {
	t.client.MSet("one", "1", "two", "2", "three", "3", "four", "4")

	keys := t.client.Keys("*o*")
	c.Assert(keys.Err(), IsNil)
	c.Assert(keys.Val(), DeepEquals, []interface{}{"four", "two", "one"})

	keys = t.client.Keys("t??")
	c.Assert(keys.Err(), IsNil)
	c.Assert(keys.Val(), DeepEquals, []interface{}{"two"})

	keys = t.client.Keys("*")
	c.Assert(keys.Err(), IsNil)
	c.Assert(keys.Val(), DeepEquals, []interface{}{"four", "three", "two", "one"})
}

func (t *RedisTest) TestMigrate(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestMove(c *C) {
	move := t.client.Move("foo", 1)
	c.Assert(move.Err(), IsNil)
	c.Assert(move.Val(), Equals, false)

	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	move = t.client.Move("foo", 1)
	c.Assert(move.Err(), IsNil)
	c.Assert(move.Val(), Equals, true)

	get := t.client.Get("foo")
	c.Assert(get.Err(), Equals, redis.Nil)
	c.Assert(get.Val(), Equals, "")

	sel := t.client.Select(1)
	c.Assert(sel.Err(), IsNil)
	c.Assert(sel.Val(), Equals, "OK")

	get = t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestObject(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	refCount := t.client.ObjectRefCount("foo")
	c.Assert(refCount.Err(), IsNil)
	c.Assert(refCount.Val(), Equals, int64(1))

	enc := t.client.ObjectEncoding("foo")
	c.Assert(enc.Err(), IsNil)
	c.Assert(enc.Val(), Equals, "raw")

	idleTime := t.client.ObjectIdleTime("foo")
	c.Assert(idleTime.Err(), IsNil)
	c.Assert(idleTime.Val(), Equals, int64(0))
}

func (t *RedisTest) TestPersist(c *C) {
	set := t.client.Set("key", "Hello")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	expire := t.client.Expire("key", 10)
	c.Assert(expire.Err(), IsNil)
	c.Assert(expire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(10))

	persist := t.client.Persist("key")
	c.Assert(persist.Err(), IsNil)
	c.Assert(persist.Val(), Equals, true)

	ttl = t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestPExpire(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	pexpire := t.client.PExpire("key", 1500)
	c.Assert(pexpire.Err(), IsNil)
	c.Assert(pexpire.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, 1)

	pttl := t.client.PTTL("key")
	c.Assert(pttl.Err(), IsNil)
	c.Assert(pttl.Val(), Equals, 1500)
}

func (t *RedisTest) TestPExpireAt(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	pExpireAt := t.client.PExpireAt("key", 1555555555005)
	c.Assert(pExpireAt.Err(), IsNil)
	c.Assert(pExpireAt.Val(), Equals, true)

	ttl := t.client.TTL("key")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, 211915059)

	pttl := t.client.PTTL("key")
	c.Assert(pttl.Err(), IsNil)
	c.Assert(pttl.Val(), Equals, int64(211915059461))
}

func (t *RedisTest) TestPTTL(c *C) {
	c.Skip("not implemented")

	set := t.client.Set("key", "Hello")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	expire := t.client.Expire("key", 1)
	c.Assert(expire.Err(), IsNil)
	c.Assert(set.Val(), Equals, true)

	pttl := t.client.PTTL("key")
	c.Assert(pttl.Err(), IsNil)
	c.Assert(pttl.Val(), Equals, int64(999))
}

func (t *RedisTest) TestRandomKey(c *C) {
	randomKey := t.client.RandomKey()
	c.Assert(randomKey.Err(), Equals, redis.Nil)
	c.Assert(randomKey.Val(), Equals, "")

	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	randomKey = t.client.RandomKey()
	c.Assert(randomKey.Err(), IsNil)
	c.Assert(randomKey.Val(), Equals, "foo")
}

func (t *RedisTest) TestRename(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	status := t.client.Rename("foo", "foo1")
	c.Assert(status.Err(), IsNil)
	c.Assert(status.Val(), Equals, "OK")

	get := t.client.Get("foo1")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestRenameNX(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	renameNX := t.client.RenameNX("foo", "foo1")
	c.Assert(renameNX.Err(), IsNil)
	c.Assert(renameNX.Val(), Equals, true)

	get := t.client.Get("foo1")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestRestore(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSort(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestTTL(c *C) {
	ttl := t.client.TTL("foo")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(-1))

	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	expire := t.client.Expire("foo", 60)
	c.Assert(expire.Err(), IsNil)
	c.Assert(expire.Val(), Equals, true)

	ttl = t.client.TTL("foo")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(60))
}

func (t *RedisTest) Type(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	type_ := t.client.Type("foo")
	c.Assert(type_.Err(), IsNil)
	c.Assert(type_.Val(), Equals, "string")
}

// //------------------------------------------------------------------------------

func (t *RedisTest) TestAppend(c *C) {
	append := t.client.Append("foo", "bar")
	c.Assert(append.Err(), IsNil)
	c.Assert(append.Val(), Equals, int64(3))
}

func (t *RedisTest) TestDecr(c *C) {
	decr := t.client.Decr("foo")
	c.Assert(decr.Err(), IsNil)
	c.Assert(decr.Val(), Equals, int64(-1))
}

func (t *RedisTest) TestDecrBy(c *C) {
	decrBy := t.client.DecrBy("foo", 10)
	c.Assert(decrBy.Err(), IsNil)
	c.Assert(decrBy.Val(), Equals, int64(-10))
}

func (t *RedisTest) TestGet(c *C) {
	get := t.client.Get("foo")
	c.Assert(get.Err(), Equals, redis.Nil)
	c.Assert(get.Val(), Equals, "")
}

func (t *RedisTest) TestSetGetBig(c *C) {
	getBit := t.client.GetBit("foo", 5)
	c.Assert(getBit.Err(), IsNil)
	c.Assert(getBit.Val(), Equals, int64(0))

	setBit := t.client.SetBit("foo", 5, 1)
	c.Assert(setBit.Err(), IsNil)
	c.Assert(setBit.Val(), Equals, int64(0))

	getBit = t.client.GetBit("foo", 5)
	c.Assert(getBit.Err(), IsNil)
	c.Assert(getBit.Val(), Equals, int64(1))
}

func (t *RedisTest) TestGetRange(c *C) {
	set := t.client.Set("foo", "hello")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	getRange := t.client.GetRange("foo", 0, 1)
	c.Assert(getRange.Err(), IsNil)
	c.Assert(getRange.Val(), Equals, "he")
}

func (t *RedisTest) TestGetSet(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	getSet := t.client.GetSet("foo", "bar2")
	c.Assert(getSet.Err(), IsNil)
	c.Assert(getSet.Val(), Equals, "bar")

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar2")
}

func (t *RedisTest) TestIncr(c *C) {
	incr := t.client.Incr("foo")
	c.Assert(incr.Err(), IsNil)
	c.Assert(incr.Val(), Equals, int64(1))
}

func (t *RedisTest) TestIncrBy(c *C) {
	incrBy := t.client.IncrBy("foo", 10)
	c.Assert(incrBy.Err(), IsNil)
	c.Assert(incrBy.Val(), Equals, int64(10))
}

func (t *RedisTest) TestMsetMget(c *C) {
	mSet := t.client.MSet("foo1", "bar1", "foo2", "bar2")
	c.Assert(mSet.Err(), IsNil)
	c.Assert(mSet.Val(), Equals, "OK")

	mGet := t.client.MGet("foo1", "foo2")
	c.Assert(mGet.Err(), IsNil)
	c.Assert(mGet.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
}

func (t *RedisTest) MSetNX(c *C) {
	mSetNX := t.client.MSetNX("foo1", "bar1", "foo2", "bar2")
	c.Assert(mSetNX.Err(), IsNil)
	c.Assert(mSetNX.Val(), Equals, true)

	mSetNX = t.client.MSetNX("foo1", "bar1", "foo2", "bar2")
	c.Assert(mSetNX.Err(), IsNil)
	c.Assert(mSetNX.Val(), Equals, false)
}

func (t *RedisTest) PSetEx(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestSetGet(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestSetEx(c *C) {
	setEx := t.client.SetEx("foo", 10, "bar")
	c.Assert(setEx.Err(), IsNil)
	c.Assert(setEx.Val(), Equals, "OK")

	ttl := t.client.TTL("foo")
	c.Assert(ttl.Err(), IsNil)
	c.Assert(ttl.Val(), Equals, int64(10))
}

func (t *RedisTest) TestSetNX(c *C) {
	setNX := t.client.SetNX("foo", "bar")
	c.Assert(setNX.Err(), IsNil)
	c.Assert(setNX.Val(), Equals, true)

	setNX = t.client.SetNX("foo", "bar2")
	c.Assert(setNX.Err(), IsNil)
	c.Assert(setNX.Val(), Equals, false)

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestSetRange(c *C) {
	set := t.client.Set("foo", "Hello World")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	range_ := t.client.SetRange("foo", 6, "Redis")
	c.Assert(range_.Err(), IsNil)
	c.Assert(range_.Val(), Equals, int64(11))

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "Hello Redis")
}

func (t *RedisTest) TestStrLen(c *C) {
	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	strLen := t.client.StrLen("foo")
	c.Assert(strLen.Err(), IsNil)
	c.Assert(strLen.Val(), Equals, int64(3))

	strLen = t.client.StrLen("_")
	c.Assert(strLen.Err(), IsNil)
	c.Assert(strLen.Val(), Equals, int64(0))
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestHDel(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Assert(hSet.Err(), IsNil)

	hDel := t.client.HDel("myhash", "foo")
	c.Assert(hDel.Err(), IsNil)
	c.Assert(hDel.Val(), Equals, int64(1))

	hDel = t.client.HDel("myhash", "foo")
	c.Assert(hDel.Err(), IsNil)
	c.Assert(hDel.Val(), Equals, int64(0))
}

func (t *RedisTest) TestHExists(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Assert(hSet.Err(), IsNil)

	hExists := t.client.HExists("myhash", "foo")
	c.Assert(hExists.Err(), IsNil)
	c.Assert(hExists.Val(), Equals, true)

	hExists = t.client.HExists("myhash", "foo1")
	c.Assert(hExists.Err(), IsNil)
	c.Assert(hExists.Val(), Equals, false)
}

func (t *RedisTest) TestHGet(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Assert(hSet.Err(), IsNil)

	hGet := t.client.HGet("myhash", "foo")
	c.Assert(hGet.Err(), IsNil)
	c.Assert(hGet.Val(), Equals, "bar")

	hGet = t.client.HGet("myhash", "foo1")
	c.Assert(hGet.Err(), Equals, redis.Nil)
	c.Assert(hGet.Val(), Equals, "")
}

func (t *RedisTest) TestHGetAll(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Assert(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Assert(hSet.Err(), IsNil)

	hGetAll := t.client.HGetAll("myhash")
	c.Assert(hGetAll.Err(), IsNil)
	c.Assert(hGetAll.Val(), DeepEquals, []interface{}{"foo1", "bar1", "foo2", "bar2"})
}

func (t *RedisTest) TestHIncrBy(c *C) {
	hSet := t.client.HSet("myhash", "foo", "5")
	c.Assert(hSet.Err(), IsNil)

	hIncrBy := t.client.HIncrBy("myhash", "foo", 1)
	c.Assert(hIncrBy.Err(), IsNil)
	c.Assert(hIncrBy.Val(), Equals, int64(6))

	hIncrBy = t.client.HIncrBy("myhash", "foo", -1)
	c.Assert(hIncrBy.Err(), IsNil)
	c.Assert(hIncrBy.Val(), Equals, int64(5))

	hIncrBy = t.client.HIncrBy("myhash", "foo", -10)
	c.Assert(hIncrBy.Err(), IsNil)
	c.Assert(hIncrBy.Val(), Equals, int64(-5))
}

func (t *RedisTest) TestHIncrByFloat(c *C) {
	c.Skip("not implemented")
}

func (t *RedisTest) TestHKeys(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Assert(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Assert(hSet.Err(), IsNil)

	hKeys := t.client.HKeys("myhash")
	c.Assert(hKeys.Err(), IsNil)
	c.Assert(hKeys.Val(), DeepEquals, []interface{}{"foo1", "foo2"})
}

func (t *RedisTest) TestHLen(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Assert(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Assert(hSet.Err(), IsNil)

	hLen := t.client.HLen("myhash")
	c.Assert(hLen.Err(), IsNil)
	c.Assert(hLen.Val(), Equals, int64(2))
}

func (t *RedisTest) TestHMGet(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Assert(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Assert(hSet.Err(), IsNil)

	hMGet := t.client.HMGet("myhash", "foo1", "foo2", "_")
	c.Assert(hMGet.Err(), IsNil)
	c.Assert(hMGet.Val(), DeepEquals, []interface{}{"bar1", "bar2", nil})
}

func (t *RedisTest) TestHMSet(c *C) {
	hMSet := t.client.HMSet("myhash", "foo1", "bar1", "foo2", "bar2")
	c.Assert(hMSet.Err(), IsNil)
	c.Assert(hMSet.Val(), Equals, "OK")

	hGet := t.client.HGet("myhash", "foo1")
	c.Assert(hGet.Err(), IsNil)
	c.Assert(hGet.Val(), Equals, "bar1")

	hGet = t.client.HGet("myhash", "foo2")
	c.Assert(hGet.Err(), IsNil)
	c.Assert(hGet.Val(), Equals, "bar2")
}

func (t *RedisTest) TestHSet(c *C) {
	hSet := t.client.HSet("myhash", "foo", "bar")
	c.Assert(hSet.Err(), IsNil)
	c.Assert(hSet.Val(), Equals, true)

	hGet := t.client.HGet("myhash", "foo")
	c.Assert(hGet.Err(), IsNil)
	c.Assert(hGet.Val(), Equals, "bar")
}

func (t *RedisTest) TestHSetNX(c *C) {
	hSetNX := t.client.HSetNX("myhash", "foo", "bar")
	c.Assert(hSetNX.Err(), IsNil)
	c.Assert(hSetNX.Val(), Equals, true)

	hSetNX = t.client.HSetNX("myhash", "foo", "bar")
	c.Assert(hSetNX.Err(), IsNil)
	c.Assert(hSetNX.Val(), Equals, false)

	hGet := t.client.HGet("myhash", "foo")
	c.Assert(hGet.Err(), IsNil)
	c.Assert(hGet.Val(), Equals, "bar")
}

func (t *RedisTest) TestHVals(c *C) {
	hSet := t.client.HSet("myhash", "foo1", "bar1")
	c.Assert(hSet.Err(), IsNil)
	hSet = t.client.HSet("myhash", "foo2", "bar2")
	c.Assert(hSet.Err(), IsNil)

	hVals := t.client.HVals("myhash")
	c.Assert(hVals.Err(), IsNil)
	c.Assert(hVals.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestBLPop(c *C) {
	rPush := t.client.RPush("list1", "a", "b", "c")
	c.Assert(rPush.Err(), IsNil)

	bLPop := t.client.BLPop(0, "list1", "list2")
	c.Assert(bLPop.Err(), IsNil)
	c.Assert(bLPop.Val(), DeepEquals, []interface{}{"list1", "a"})
}

func (t *RedisTest) TestBLPopBlocks(c *C) {
	started := make(chan bool)
	done := make(chan bool)
	go func() {
		started <- true
		bLPop := t.client.BLPop(0, "list")
		c.Assert(bLPop.Err(), IsNil)
		c.Assert(bLPop.Val(), DeepEquals, []interface{}{"list", "a"})
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
	c.Assert(rPush.Err(), IsNil)

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
	c.Assert(rPush.Err(), IsNil)

	bRPop := t.client.BRPop(0, "list1", "list2")
	c.Assert(bRPop.Err(), IsNil)
	c.Assert(bRPop.Val(), DeepEquals, []interface{}{"list1", "c"})
}

func (t *RedisTest) TestBRPopBlocks(c *C) {
	started := make(chan bool)
	done := make(chan bool)
	go func() {
		started <- true
		bRPop := t.client.BRPop(0, "list")
		c.Assert(bRPop.Err(), IsNil)
		c.Assert(bRPop.Val(), DeepEquals, []interface{}{"list", "a"})
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
	c.Assert(rPush.Err(), IsNil)

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
	c.Assert(rPush.Err(), IsNil)

	bRPopLPush := t.client.BRPopLPush("list1", "list2", 0)
	c.Assert(bRPopLPush.Err(), IsNil)
	c.Assert(bRPopLPush.Val(), Equals, "c")
}

func (t *RedisTest) TestLIndex(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Assert(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Assert(lPush.Err(), IsNil)

	lIndex := t.client.LIndex("list", 0)
	c.Assert(lIndex.Err(), IsNil)
	c.Assert(lIndex.Val(), Equals, "Hello")

	lIndex = t.client.LIndex("list", -1)
	c.Assert(lIndex.Err(), IsNil)
	c.Assert(lIndex.Val(), Equals, "World")

	lIndex = t.client.LIndex("list", 3)
	c.Assert(lIndex.Err(), Equals, redis.Nil)
	c.Assert(lIndex.Val(), Equals, "")
}

func (t *RedisTest) TestLInsert(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "World")
	c.Assert(rPush.Err(), IsNil)

	lInsert := t.client.LInsert("list", "BEFORE", "World", "There")
	c.Assert(lInsert.Err(), IsNil)
	c.Assert(lInsert.Val(), Equals, int64(3))

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"Hello", "There", "World"})
}

func (t *RedisTest) TestLLen(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Assert(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Assert(lPush.Err(), IsNil)

	lLen := t.client.LLen("list")
	c.Assert(lLen.Err(), IsNil)
	c.Assert(lLen.Val(), Equals, int64(2))
}

func (t *RedisTest) TestLPop(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	lPop := t.client.LPop("list")
	c.Assert(lPop.Err(), IsNil)
	c.Assert(lPop.Val(), Equals, "one")

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestLPush(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Assert(lPush.Err(), IsNil)
	lPush = t.client.LPush("list", "Hello")
	c.Assert(lPush.Err(), IsNil)

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestLPushX(c *C) {
	lPush := t.client.LPush("list", "World")
	c.Assert(lPush.Err(), IsNil)

	lPushX := t.client.LPushX("list", "Hello")
	c.Assert(lPushX.Err(), IsNil)
	c.Assert(lPushX.Val(), Equals, int64(2))

	lPushX = t.client.LPushX("list2", "Hello")
	c.Assert(lPushX.Err(), IsNil)
	c.Assert(lPushX.Val(), Equals, int64(0))

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRange(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	lRange := t.client.LRange("list", 0, 0)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"one"})

	lRange = t.client.LRange("list", -3, 2)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	lRange = t.client.LRange("list", -100, 100)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	lRange = t.client.LRange("list", 5, 10)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestLRem(c *C) {
	rPush := t.client.RPush("list", "hello")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "hello")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "foo")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "hello")
	c.Assert(rPush.Err(), IsNil)

	lRem := t.client.LRem("list", -2, "hello")
	c.Assert(lRem.Err(), IsNil)
	c.Assert(lRem.Val(), Equals, int64(2))

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"hello", "foo"})
}

func (t *RedisTest) TestLSet(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	lSet := t.client.LSet("list", 0, "four")
	c.Assert(lSet.Err(), IsNil)
	c.Assert(lSet.Val(), Equals, "OK")

	lSet = t.client.LSet("list", -2, "five")
	c.Assert(lSet.Err(), IsNil)
	c.Assert(lSet.Val(), Equals, "OK")

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"four", "five", "three"})
}

func (t *RedisTest) TestLTrim(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	lTrim := t.client.LTrim("list", 1, -1)
	c.Assert(lTrim.Err(), IsNil)
	c.Assert(lTrim.Val(), Equals, "OK")

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestRPop(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	rPop := t.client.RPop("list")
	c.Assert(rPop.Err(), IsNil)
	c.Assert(rPop.Val(), Equals, "three")

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"one", "two"})
}

func (t *RedisTest) TestRPopLPush(c *C) {
	rPush := t.client.RPush("list", "one")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "two")
	c.Assert(rPush.Err(), IsNil)
	rPush = t.client.RPush("list", "three")
	c.Assert(rPush.Err(), IsNil)

	rPopLPush := t.client.RPopLPush("list", "list2")
	c.Assert(rPopLPush.Err(), IsNil)
	c.Assert(rPopLPush.Val(), Equals, "three")

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"one", "two"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"three"})
}

func (t *RedisTest) TestRPush(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Assert(rPush.Err(), IsNil)
	c.Assert(rPush.Val(), Equals, int64(1))

	rPush = t.client.RPush("list", "World")
	c.Assert(rPush.Err(), IsNil)
	c.Assert(rPush.Val(), Equals, int64(2))

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})
}

func (t *RedisTest) TestRPushX(c *C) {
	rPush := t.client.RPush("list", "Hello")
	c.Assert(rPush.Err(), IsNil)
	c.Assert(rPush.Val(), Equals, int64(1))

	rPushX := t.client.RPushX("list", "World")
	c.Assert(rPushX.Err(), IsNil)
	c.Assert(rPushX.Val(), Equals, int64(2))

	rPushX = t.client.RPushX("list2", "World")
	c.Assert(rPushX.Err(), IsNil)
	c.Assert(rPushX.Val(), Equals, int64(0))

	lRange := t.client.LRange("list", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{"Hello", "World"})

	lRange = t.client.LRange("list2", 0, -1)
	c.Assert(lRange.Err(), IsNil)
	c.Assert(lRange.Val(), DeepEquals, []interface{}{})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestSAdd(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Assert(sAdd.Err(), IsNil)
	c.Assert(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Assert(sAdd.Err(), IsNil)
	c.Assert(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Assert(sAdd.Err(), IsNil)
	c.Assert(sAdd.Val(), Equals, int64(0))

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSCard(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Assert(sAdd.Err(), IsNil)
	c.Assert(sAdd.Val(), Equals, int64(1))

	sAdd = t.client.SAdd("set", "World")
	c.Assert(sAdd.Err(), IsNil)
	c.Assert(sAdd.Val(), Equals, int64(1))

	sCard := t.client.SCard("set")
	c.Assert(sCard.Err(), IsNil)
	c.Assert(sCard.Val(), Equals, int64(2))
}

func (t *RedisTest) TestSDiff(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sDiff := t.client.SDiff("set1", "set2")
	c.Assert(sDiff.Err(), IsNil)
	c.Assert(sDiff.Val(), DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSDiffStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sDiffStore := t.client.SDiffStore("set", "set1", "set2")
	c.Assert(sDiffStore.Err(), IsNil)
	c.Assert(sDiffStore.Val(), Equals, int64(2))

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"a", "b"})
}

func (t *RedisTest) TestSInter(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sInter := t.client.SInter("set1", "set2")
	c.Assert(sInter.Err(), IsNil)
	c.Assert(sInter.Val(), DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestSInterStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sInterStore := t.client.SInterStore("set", "set1", "set2")
	c.Assert(sInterStore.Err(), IsNil)
	c.Assert(sInterStore.Val(), Equals, int64(1))

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"c"})
}

func (t *RedisTest) TestIsMember(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Assert(sAdd.Err(), IsNil)

	sIsMember := t.client.SIsMember("set", "one")
	c.Assert(sIsMember.Err(), IsNil)
	c.Assert(sIsMember.Val(), Equals, true)

	sIsMember = t.client.SIsMember("set", "two")
	c.Assert(sIsMember.Err(), IsNil)
	c.Assert(sIsMember.Val(), Equals, false)
}

func (t *RedisTest) TestSMembers(c *C) {
	sAdd := t.client.SAdd("set", "Hello")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "World")
	c.Assert(sAdd.Err(), IsNil)

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"World", "Hello"})
}

func (t *RedisTest) TestSMove(c *C) {
	sAdd := t.client.SAdd("set1", "one")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "two")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "three")
	c.Assert(sAdd.Err(), IsNil)

	sMove := t.client.SMove("set1", "set2", "two")
	c.Assert(sMove.Err(), IsNil)
	c.Assert(sMove.Val(), Equals, true)

	sMembers := t.client.SMembers("set1")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"one"})

	sMembers = t.client.SMembers("set2")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSPop(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Assert(sAdd.Err(), IsNil)

	sPop := t.client.SPop("set")
	c.Assert(sPop.Err(), IsNil)
	c.Assert(sPop.Val(), Not(Equals), "")

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), HasLen, 2)
}

func (t *RedisTest) TestSRandMember(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Assert(sAdd.Err(), IsNil)

	sRandMember := t.client.SRandMember("set")
	c.Assert(sRandMember.Err(), IsNil)
	c.Assert(sRandMember.Val(), Not(Equals), "")

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), HasLen, 3)
}

func (t *RedisTest) TestSRem(c *C) {
	sAdd := t.client.SAdd("set", "one")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "two")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set", "three")
	c.Assert(sAdd.Err(), IsNil)

	sRem := t.client.SRem("set", "one")
	c.Assert(sRem.Err(), IsNil)
	c.Assert(sRem.Val(), Equals, int64(1))

	sRem = t.client.SRem("set", "four")
	c.Assert(sRem.Err(), IsNil)
	c.Assert(sRem.Val(), Equals, int64(0))

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), DeepEquals, []interface{}{"three", "two"})
}

func (t *RedisTest) TestSUnion(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sUnion := t.client.SUnion("set1", "set2")
	c.Assert(sUnion.Err(), IsNil)
	c.Assert(sUnion.Val(), HasLen, 5)
}

func (t *RedisTest) TestSUnionStore(c *C) {
	sAdd := t.client.SAdd("set1", "a")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "b")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set1", "c")
	c.Assert(sAdd.Err(), IsNil)

	sAdd = t.client.SAdd("set2", "c")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "d")
	c.Assert(sAdd.Err(), IsNil)
	sAdd = t.client.SAdd("set2", "e")
	c.Assert(sAdd.Err(), IsNil)

	sUnionStore := t.client.SUnionStore("set", "set1", "set2")
	c.Assert(sUnionStore.Err(), IsNil)
	c.Assert(sUnionStore.Val(), Equals, int64(5))

	sMembers := t.client.SMembers("set")
	c.Assert(sMembers.Err(), IsNil)
	c.Assert(sMembers.Val(), HasLen, 5)
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestZAdd(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	c.Assert(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(1, "uno"))
	c.Assert(zAdd.Err(), IsNil)
	c.Assert(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	c.Assert(zAdd.Val(), Equals, int64(1))

	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "two"))
	c.Assert(zAdd.Err(), IsNil)
	c.Assert(zAdd.Val(), Equals, int64(0))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"one", "1", "uno", "1", "two", "3"})
}

func (t *RedisTest) TestZCard(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)

	zCard := t.client.ZCard("zset")
	c.Assert(zCard.Err(), IsNil)
	c.Assert(zCard.Val(), Equals, int64(2))
}

func (t *RedisTest) TestZCount(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zCount := t.client.ZCount("zset", "-inf", "+inf")
	c.Assert(zCount.Err(), IsNil)
	c.Assert(zCount.Val(), Equals, int64(3))

	zCount = t.client.ZCount("zset", "(1", "3")
	c.Assert(zCount.Err(), IsNil)
	c.Assert(zCount.Val(), Equals, int64(2))
}

func (t *RedisTest) TestZIncrBy(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)

	zIncrBy := t.client.ZIncrBy("zset", 2, "one")
	c.Assert(zIncrBy.Err(), IsNil)
	c.Assert(zIncrBy.Val(), Equals, float64(3))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"two", "2", "one", "3"})
}

func (t *RedisTest) TestZInterStore(c *C) {
	zAdd := t.client.ZAdd("zset1", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset1", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)

	zAdd = t.client.ZAdd("zset2", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset3", redis.NewZMember(3, "two"))
	c.Assert(zAdd.Err(), IsNil)

	zInterStore := t.client.ZInterStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	)
	c.Assert(zInterStore.Err(), IsNil)
	c.Assert(zInterStore.Val(), Equals, int64(2))

	zRange := t.client.ZRange("out", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"one", "5", "two", "10"})
}

func (t *RedisTest) TestZRange(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRange := t.client.ZRange("zset", 0, -1, false)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	zRange = t.client.ZRange("zset", 2, 3, false)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"three"})

	zRange = t.client.ZRange("zset", -2, -1, false)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"two", "three"})
}

func (t *RedisTest) TestZRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRangeByScore := t.client.ZRangeByScore("zset", "-inf", "+inf", false, nil)
	c.Assert(zRangeByScore.Err(), IsNil)
	c.Assert(zRangeByScore.Val(), DeepEquals, []interface{}{"one", "two", "three"})

	zRangeByScore = t.client.ZRangeByScore("zset", "1", "2", false, nil)
	c.Assert(zRangeByScore.Err(), IsNil)
	c.Assert(zRangeByScore.Val(), DeepEquals, []interface{}{"one", "two"})

	zRangeByScore = t.client.ZRangeByScore("zset", "(1", "2", false, nil)
	c.Assert(zRangeByScore.Err(), IsNil)
	c.Assert(zRangeByScore.Val(), DeepEquals, []interface{}{"two"})

	zRangeByScore = t.client.ZRangeByScore("zset", "(1", "(2", false, nil)
	c.Assert(zRangeByScore.Err(), IsNil)
	c.Assert(zRangeByScore.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRank := t.client.ZRank("zset", "three")
	c.Assert(zRank.Err(), IsNil)
	c.Assert(zRank.Val(), Equals, int64(2))

	zRank = t.client.ZRank("zset", "four")
	c.Assert(zRank.Err(), Equals, redis.Nil)
	c.Assert(zRank.Val(), Equals, int64(0))
}

func (t *RedisTest) TestZRem(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRem := t.client.ZRem("zset", "two")
	c.Assert(zRem.Err(), IsNil)
	c.Assert(zRem.Val(), Equals, int64(1))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"one", "1", "three", "3"})
}

func (t *RedisTest) TestZRemRangeByRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRemRangeByRank := t.client.ZRemRangeByRank("zset", 0, 1)
	c.Assert(zRemRangeByRank.Err(), IsNil)
	c.Assert(zRemRangeByRank.Val(), Equals, int64(2))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"three", "3"})
}

func (t *RedisTest) TestZRemRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRemRangeByScore := t.client.ZRemRangeByScore("zset", "-inf", "(2")
	c.Assert(zRemRangeByScore.Err(), IsNil)
	c.Assert(zRemRangeByScore.Val(), Equals, int64(1))

	zRange := t.client.ZRange("zset", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"two", "2", "three", "3"})
}

func (t *RedisTest) TestZRevRange(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRevRange := t.client.ZRevRange("zset", "0", "-1", false)
	c.Assert(zRevRange.Err(), IsNil)
	c.Assert(zRevRange.Val(), DeepEquals, []interface{}{"three", "two", "one"})

	zRevRange = t.client.ZRevRange("zset", "2", "3", false)
	c.Assert(zRevRange.Err(), IsNil)
	c.Assert(zRevRange.Val(), DeepEquals, []interface{}{"one"})

	zRevRange = t.client.ZRevRange("zset", "-2", "-1", false)
	c.Assert(zRevRange.Err(), IsNil)
	c.Assert(zRevRange.Val(), DeepEquals, []interface{}{"two", "one"})
}

func (t *RedisTest) TestZRevRangeByScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRevRangeByScore := t.client.ZRevRangeByScore("zset", "+inf", "-inf", false, nil)
	c.Assert(zRevRangeByScore.Err(), IsNil)
	c.Assert(zRevRangeByScore.Val(), DeepEquals, []interface{}{"three", "two", "one"})

	zRevRangeByScore = t.client.ZRevRangeByScore("zset", "2", "(1", false, nil)
	c.Assert(zRevRangeByScore.Err(), IsNil)
	c.Assert(zRevRangeByScore.Val(), DeepEquals, []interface{}{"two"})

	zRevRangeByScore = t.client.ZRevRangeByScore("zset", "(2", "(1", false, nil)
	c.Assert(zRevRangeByScore.Err(), IsNil)
	c.Assert(zRevRangeByScore.Val(), DeepEquals, []interface{}{})
}

func (t *RedisTest) TestZRevRank(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zRevRank := t.client.ZRevRank("zset", "one")
	c.Assert(zRevRank.Err(), IsNil)
	c.Assert(zRevRank.Val(), Equals, int64(2))

	zRevRank = t.client.ZRevRank("zset", "four")
	c.Assert(zRevRank.Err(), Equals, redis.Nil)
	c.Assert(zRevRank.Val(), Equals, int64(0))
}

func (t *RedisTest) TestZScore(c *C) {
	zAdd := t.client.ZAdd("zset", redis.NewZMember(1.001, "one"))
	c.Assert(zAdd.Err(), IsNil)

	zScore := t.client.ZScore("zset", "one")
	c.Assert(zScore.Err(), IsNil)
	c.Assert(zScore.Val(), Equals, float64(1.001))
}

func (t *RedisTest) TestZUnionStore(c *C) {
	zAdd := t.client.ZAdd("zset1", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset1", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)

	zAdd = t.client.ZAdd("zset2", redis.NewZMember(1, "one"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(2, "two"))
	c.Assert(zAdd.Err(), IsNil)
	zAdd = t.client.ZAdd("zset2", redis.NewZMember(3, "three"))
	c.Assert(zAdd.Err(), IsNil)

	zUnionStore := t.client.ZUnionStore(
		"out",
		2,
		[]string{"zset1", "zset2"},
		[]int64{2, 3},
		"",
	)
	c.Assert(zUnionStore.Err(), IsNil)
	c.Assert(zUnionStore.Val(), Equals, int64(3))

	zRange := t.client.ZRange("out", 0, -1, true)
	c.Assert(zRange.Err(), IsNil)
	c.Assert(zRange.Val(), DeepEquals, []interface{}{"one", "5", "three", "9", "two", "10"})
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestPatternPubSub(c *C) {
	pubsub, err := t.client.PubSubClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pubsub.Close(), IsNil)
	}()

	ch, err := pubsub.PSubscribe("mychannel*")
	c.Assert(err, IsNil)
	c.Assert(ch, Not(IsNil))

	pub := t.client.Publish("mychannel1", "hello")
	c.Assert(pub.Err(), IsNil)
	c.Assert(pub.Val(), Equals, int64(1))

	err = pubsub.PUnsubscribe("mychannel*")
	c.Assert(err, IsNil)

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "psubscribe")
		c.Assert(msg.Channel, Equals, "mychannel*")
		c.Assert(msg.Number, Equals, int64(1))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "pmessage")
		c.Assert(msg.ChannelPattern, Equals, "mychannel*")
		c.Assert(msg.Channel, Equals, "mychannel1")
		c.Assert(msg.Message, Equals, "hello")
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "punsubscribe")
		c.Assert(msg.Channel, Equals, "mychannel*")
		c.Assert(msg.Number, Equals, int64(0))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}
}

func (t *RedisTest) TestPubSub(c *C) {
	pubsub, err := t.client.PubSubClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pubsub.Close(), IsNil)
	}()

	ch, err := pubsub.Subscribe("mychannel")
	c.Assert(err, IsNil)
	c.Assert(ch, Not(IsNil))

	ch2, err := pubsub.Subscribe("mychannel2")
	c.Assert(err, IsNil)
	c.Assert(ch2, Equals, ch)

	pub := t.client.Publish("mychannel", "hello")
	c.Assert(pub.Err(), IsNil)
	c.Assert(pub.Val(), Equals, int64(1))

	pub = t.client.Publish("mychannel2", "hello2")
	c.Assert(pub.Err(), IsNil)
	c.Assert(pub.Val(), Equals, int64(1))

	err = pubsub.Unsubscribe("mychannel")
	c.Assert(err, IsNil)

	err = pubsub.Unsubscribe("mychannel2")
	c.Assert(err, IsNil)

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "subscribe")
		c.Assert(msg.Channel, Equals, "mychannel")
		c.Assert(msg.Number, Equals, int64(1))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "subscribe")
		c.Assert(msg.Channel, Equals, "mychannel2")
		c.Assert(msg.Number, Equals, int64(2))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "message")
		c.Assert(msg.Channel, Equals, "mychannel")
		c.Assert(msg.Message, Equals, "hello")
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "message")
		c.Assert(msg.Channel, Equals, "mychannel2")
		c.Assert(msg.Message, Equals, "hello2")
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "unsubscribe")
		c.Assert(msg.Channel, Equals, "mychannel")
		c.Assert(msg.Number, Equals, int64(1))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}

	select {
	case msg := <-ch:
		c.Assert(msg.Err, Equals, nil)
		c.Assert(msg.Name, Equals, "unsubscribe")
		c.Assert(msg.Channel, Equals, "mychannel2")
		c.Assert(msg.Number, Equals, int64(0))
	case <-time.After(time.Second):
		c.Error("Channel is empty.")
	}
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestPipeline(c *C) {
	set := t.client.Set("foo2", "bar2")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	pipeline, err := t.client.PipelineClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pipeline.Close(), IsNil)
	}()

	setReq := pipeline.Set("foo1", "bar1")
	getReq := pipeline.Get("foo2")

	reqs, err := pipeline.RunQueued()
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 2)

	c.Assert(setReq.Err(), IsNil)
	c.Assert(setReq.Val(), Equals, "OK")

	c.Assert(getReq.Err(), IsNil)
	c.Assert(getReq.Val(), Equals, "bar2")
}

func (t *RedisTest) TestPipelineErrValNotSet(c *C) {
	pipeline, err := t.client.PipelineClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pipeline.Close(), IsNil)
	}()

	get := pipeline.Get("foo")
	c.Assert(get.Err(), ErrorMatches, "redis: value is not set")
}

func (t *RedisTest) TestPipelineRunQueuedOnEmptyQueue(c *C) {
	pipeline, err := t.client.PipelineClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pipeline.Close(), IsNil)
	}()

	reqs, err := pipeline.RunQueued()
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 0)
}

func (t *RedisTest) TestPipelineIncrFromGoroutines(c *C) {
	pipeline, err := t.client.PipelineClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pipeline.Close(), IsNil)
	}()

	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			pipeline.Incr("TestIncrPipeliningFromGoroutinesKey")
			wg.Done()
		}()
	}
	wg.Wait()

	reqs, err := pipeline.RunQueued()
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 20000)
	for _, req := range reqs {
		if req.Err() != nil {
			c.Errorf("got %v, expected nil", req.Err())
		}
	}

	get := t.client.Get("TestIncrPipeliningFromGoroutinesKey")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "20000")
}

func (t *RedisTest) TestPipelineEchoFromGoroutines(c *C) {
	pipeline, err := t.client.PipelineClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(pipeline.Close(), IsNil)
	}()

	wg := &sync.WaitGroup{}
	for i := int64(0); i < 1000; i += 2 {
		wg.Add(1)
		go func() {
			msg1 := "echo" + strconv.FormatInt(i, 10)
			msg2 := "echo" + strconv.FormatInt(i+1, 10)

			echo1 := pipeline.Echo(msg1)
			echo2 := pipeline.Echo(msg2)

			reqs, err := pipeline.RunQueued()
			c.Assert(err, IsNil)
			c.Assert(reqs, HasLen, 2)

			c.Assert(echo1.Err(), IsNil)
			c.Assert(echo1.Val(), Equals, msg1)

			c.Assert(echo2.Err(), IsNil)
			c.Assert(echo2.Val(), Equals, msg2)

			wg.Done()
		}()
	}
	wg.Wait()
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestMultiExec(c *C) {
	multi, err := t.client.MultiClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(multi.Close(), IsNil)
	}()

	var (
		set *redis.StatusReq
		get *redis.BulkReq
	)
	reqs, err := multi.Exec(func() {
		set = multi.Set("foo", "bar")
		get = multi.Get("foo")
	})
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 2)

	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar")
}

func (t *RedisTest) TestMultiExecDiscard(c *C) {
	multi, err := t.client.MultiClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(multi.Close(), IsNil)
	}()

	reqs, err := multi.Exec(func() {
		multi.Set("foo1", "bar1")
		multi.Discard()
		multi.Set("foo2", "bar2")
	})
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 1)

	get := t.client.Get("foo1")
	c.Assert(get.Err(), Equals, redis.Nil)
	c.Assert(get.Val(), Equals, "")

	get = t.client.Get("foo2")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "bar2")
}

func (t *RedisTest) TestMultiExecOnEmptyQueue(c *C) {
	multi, err := t.client.MultiClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(multi.Close(), IsNil)
	}()

	reqs, err := multi.Exec(func() {})
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 0)
}

func (t *RedisTest) TestMultiExecIncrTransaction(c *C) {
	multi, err := t.client.MultiClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(multi.Close(), IsNil)
	}()

	reqs, err := multi.Exec(func() {
		for i := int64(0); i < 20000; i++ {
			multi.Incr("TestIncrTransactionKey")
		}
	})
	c.Assert(err, IsNil)
	c.Assert(reqs, HasLen, 20000)
	for _, req := range reqs {
		if req.Err() != nil {
			c.Errorf("got %v, expected nil", req.Err())
		}
	}

	get := t.client.Get("TestIncrTransactionKey")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "20000")
}

func (t *RedisTest) transactionalIncr(c *C) ([]redis.Req, error) {
	multi, err := t.client.MultiClient()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(multi.Close(), IsNil)
	}()

	watch := multi.Watch("foo")
	c.Assert(watch.Err(), IsNil)
	c.Assert(watch.Val(), Equals, "OK")

	get := multi.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Not(Equals), redis.Nil)

	v, err := strconv.ParseInt(get.Val(), 10, 64)
	c.Assert(err, IsNil)

	reqs, err := multi.Exec(func() {
		multi.Set("foo", strconv.FormatInt(v+1, 10))
	})
	if err == redis.Nil {
		return t.transactionalIncr(c)
	}
	return reqs, err
}

func (t *RedisTest) TestWatchUnwatch(c *C) {
	set := t.client.Set("foo", "0")
	c.Assert(set.Err(), IsNil)
	c.Assert(set.Val(), Equals, "OK")

	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			reqs, err := t.transactionalIncr(c)
			c.Assert(reqs, HasLen, 1)
			c.Assert(err, IsNil)
			c.Assert(reqs[0].Err(), IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	get := t.client.Get("foo")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "1000")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestSyncEchoFromGoroutines(c *C) {
	wg := &sync.WaitGroup{}
	for i := int64(0); i < 1000; i++ {
		wg.Add(1)
		go func() {
			msg := "echo" + strconv.FormatInt(i, 10)
			echo := t.client.Echo(msg)
			c.Assert(echo.Err(), IsNil)
			c.Assert(echo.Val(), Equals, msg)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (t *RedisTest) TestIncrFromGoroutines(c *C) {
	wg := &sync.WaitGroup{}
	for i := int64(0); i < 20000; i++ {
		wg.Add(1)
		go func() {
			incr := t.client.Incr("TestIncrFromGoroutinesKey")
			c.Assert(incr.Err(), IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	get := t.client.Get("TestIncrFromGoroutinesKey")
	c.Assert(get.Err(), IsNil)
	c.Assert(get.Val(), Equals, "20000")
}

//------------------------------------------------------------------------------

func (t *RedisTest) TestCmdBgRewriteAOF(c *C) {
	r := t.client.BgRewriteAOF()
	c.Assert(r.Err(), IsNil)
	c.Assert(r.Val(), Equals, "Background append only file rewriting started")
}

func (t *RedisTest) TestCmdBgSave(c *C) {
	r := t.client.BgSave()
	c.Assert(r.Err(), ErrorMatches, "ERR Can't BGSAVE while AOF log rewriting is in progress")
	c.Assert(r.Val(), Equals, "")
}

func (t *RedisTest) TestCmdClientKill(c *C) {
	r := t.client.ClientKill("1.1.1.1:1111")
	c.Assert(r.Err(), ErrorMatches, "ERR No such client")
	c.Assert(r.Val(), Equals, "")
}

func (t *RedisTest) TestCmdClientList(c *C) {
	r := t.client.ClientList()
	c.Assert(r.Err(), IsNil)
	c.Assert(
		r.Val(),
		Matches,
		"addr=127.0.0.1:[0-9]+ .+\n",
	)
}

func (t *RedisTest) TestCmdConfigGet(c *C) {
	r := t.client.ConfigGet("*")
	c.Assert(r.Err(), IsNil)
	c.Assert(len(r.Val()) > 0, Equals, true)
}

func (t *RedisTest) TestCmdConfigResetStat(c *C) {
	r := t.client.ConfigResetStat()
	c.Assert(r.Err(), IsNil)
	c.Assert(r.Val(), Equals, "OK")
}

func (t *RedisTest) TestCmdConfigSet(c *C) {
	configGet := t.client.ConfigGet("maxmemory")
	c.Assert(configGet.Err(), IsNil)
	c.Assert(configGet.Val(), HasLen, 2)
	c.Assert(configGet.Val()[0].(string), Equals, "maxmemory")

	configSet := t.client.ConfigSet("maxmemory", configGet.Val()[1].(string))
	c.Assert(configSet.Err(), IsNil)
	c.Assert(configSet.Val(), Equals, "OK")
}

func (t *RedisTest) TestCmdDbSize(c *C) {
	dbSize := t.client.DbSize()
	c.Assert(dbSize.Err(), IsNil)
	c.Assert(dbSize.Val(), Equals, int64(0))
}

func (t *RedisTest) TestCmdFlushAll(c *C) {
	// TODO
}

func (t *RedisTest) TestCmdFlushDb(c *C) {
	// TODO
}

func (t *RedisTest) TestCmdInfo(c *C) {
	info := t.client.Info()
	c.Check(info.Err(), IsNil)
	c.Check(info.Val(), Not(Equals), "")
}

func (t *RedisTest) TestCmdLastSave(c *C) {
	lastSave := t.client.LastSave()
	c.Check(lastSave.Err(), IsNil)
	c.Check(lastSave.Val(), Not(Equals), 0)
}

func (t *RedisTest) TestCmdSave(c *C) {
	save := t.client.Save()
	c.Check(save.Err(), IsNil)
	c.Check(save.Val(), Equals, "OK")
}

func (t *RedisTest) TestSlaveOf(c *C) {
	slaveOf := t.client.SlaveOf("localhost", "8888")
	c.Check(slaveOf.Err(), IsNil)
	c.Check(slaveOf.Val(), Equals, "OK")

	slaveOf = t.client.SlaveOf("NO", "ONE")
	c.Check(slaveOf.Err(), IsNil)
	c.Check(slaveOf.Val(), Equals, "OK")
}

func (t *RedisTest) TestTime(c *C) {
	c.Skip("2.6")

	time := t.client.Time()
	c.Check(time.Err(), IsNil)
	c.Check(time.Val(), HasLen, 2)
}

//------------------------------------------------------------------------------

func (t *RedisTest) BenchmarkRedisPing(c *C) {
	c.StopTimer()

	runtime.LockOSThread()
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

	for i := 0; i < 10; i++ {
		ping := t.client.Ping()
		c.Assert(ping.Err(), IsNil)
		c.Assert(ping.Val(), Equals, "PONG")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Ping()
	}
}

func (t *RedisTest) BenchmarkRedisSet(c *C) {
	c.StopTimer()

	runtime.LockOSThread()
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

	for i := 0; i < 10; i++ {
		set := t.client.Set("foo", "bar")
		c.Assert(set.Err(), IsNil)
		c.Assert(set.Val(), Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Set("foo", "bar")
	}
}

func (t *RedisTest) BenchmarkRedisGetNil(c *C) {
	c.StopTimer()

	runtime.LockOSThread()
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

	for i := 0; i < 10; i++ {
		get := t.client.Get("foo")
		c.Assert(get.Err(), Equals, redis.Nil)
		c.Assert(get.Val(), Equals, "")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo")
	}
}

func (t *RedisTest) BenchmarkRedisGet(c *C) {
	c.StopTimer()

	runtime.LockOSThread()
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

	set := t.client.Set("foo", "bar")
	c.Assert(set.Err(), IsNil)

	for i := 0; i < 10; i++ {
		get := t.client.Get("foo")
		c.Assert(get.Err(), IsNil)
		c.Assert(get.Val(), Equals, "bar")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.Get("foo")
	}
}

func (t *RedisTest) BenchmarkRedisMGet(c *C) {
	c.StopTimer()

	runtime.LockOSThread()
	t.client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

	mSet := t.client.MSet("foo1", "bar1", "foo2", "bar2")
	c.Assert(mSet.Err(), IsNil)

	for i := 0; i < 10; i++ {
		mGet := t.client.MGet("foo1", "foo2")
		c.Assert(mGet.Err(), IsNil)
		c.Assert(mGet.Val(), DeepEquals, []interface{}{"bar1", "bar2"})
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.MGet("foo1", "foo2")
	}
}

func (t *RedisTest) BenchmarkRedisWriteRead(c *C) {
	c.StopTimer()

	runtime.LockOSThread()

	conn, _, err := t.client.ConnPool.Get()
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		err := t.client.WriteReq(conn, redis.NewStatusReq("PING"))
		c.Assert(err, IsNil)

		line, _, err := conn.Rd.ReadLine()
		c.Assert(err, IsNil)
		c.Assert(line, DeepEquals, []byte("+PONG"))
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		t.client.WriteReq(conn, redis.NewStatusReq("PING"))
		conn.Rd.ReadLine()
	}

	c.StopTimer()
	t.client.ConnPool.Add(conn)
	c.StartTimer()
}
