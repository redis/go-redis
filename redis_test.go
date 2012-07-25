package redis_test

import (
	"io"
	"net"
	"testing"
	"time"

	. "launchpad.net/gocheck"

	"github.com/togoio/redisgoproxy/redis"
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

func (t *RedisTest) TestPing(c *C) {
	_, err := t.redisC.Ping().Reply()
	c.Check(err, IsNil)
}

func (t *RedisTest) TestSetGet(c *C) {
	key := "foo"
	value := "bar"

	_, err := t.redisC.Set(key, value).Reply()
	c.Check(err, IsNil)

	v, err := t.redisC.Get("foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, value)

	_, err = t.redisC.Get("_").Reply()
	c.Check(err, Equals, redis.Nil)
}

func (t *RedisTest) TestHmsetHmget(c *C) {
	_, err := t.redisC.Hmset("myhash", "foo1", "bar1", "foo2", "bar2").Reply()
	c.Check(err, IsNil)

	pairs, err := t.redisC.Hmget("myhash", "foo1", "foo3", "foo2").Reply()
	c.Check(err, IsNil)
	c.Check(pairs, HasLen, 3)
	c.Check(pairs[0], Equals, "bar1")
	c.Check(pairs[1], Equals, nil)
	c.Check(pairs[2], Equals, "bar2")
}

func (t *RedisTest) TestSet(c *C) {
	n, err := t.redisC.Sadd("myset", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	members, err := t.redisC.Smembers("myset").Reply()
	c.Check(err, IsNil)
	c.Check(members, HasLen, 1)
	c.Check(members[0], Equals, "foo")

	n, err = t.redisC.Srem("myset", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))
}

func (t *RedisTest) TestHsetnx(c *C) {
	wasSet, err := t.redisC.Hsetnx("myhash", "foo1", "bar1").Reply()
	c.Check(err, IsNil)
	c.Check(wasSet, Equals, true)

	wasSet, err = t.redisC.Hsetnx("myhash", "foo1", "bar1").Reply()
	c.Check(err, IsNil)
	c.Check(wasSet, Equals, false)
}

func (t *RedisTest) TestHash(c *C) {
	_, err := t.redisC.Hset("myhash", "foo", "bar").Reply()
	c.Check(err, IsNil)

	v, err := t.redisC.Hget("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")

	exists, err := t.redisC.Hexists("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(exists, Equals, true)

	hlen, err := t.redisC.Hlen("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(hlen, Equals, int64(1))

	res, err := t.redisC.Hgetall("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(res, HasLen, 2)
	c.Check(res[0], Equals, "foo")
	c.Check(res[1], Equals, "bar")

	keys, err := t.redisC.Hkeys("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(keys, HasLen, 1)
	c.Check(keys[0], Equals, "foo")

	vals, err := t.redisC.Hvals("myhash").Reply()
	c.Check(err, IsNil)
	c.Check(vals, HasLen, 1)
	c.Check(vals[0], Equals, "bar")

	n, err := t.redisC.Hdel("myhash", "foo").Reply()
	c.Check(err, IsNil)
	c.Check(n, Equals, int64(1))

	wasSet, err := t.redisC.Hset("myhash", "counter", "0").Reply()
	c.Check(err, IsNil)
	c.Check(wasSet, Equals, true)

	counter, err := t.redisC.Hincrby("myhash", "counter", 1).Reply()
	c.Check(err, IsNil)
	c.Check(counter, Equals, int64(1))
}

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

func (t *RedisTest) TestMultiExec(c *C) {
	multiC := t.redisC.Multi()

	setR := multiC.Set("foo", "bar")
	getR := multiC.Get("foo")

	_, err := multiC.Exec()
	c.Check(err, IsNil)

	_, err = setR.Reply()
	c.Check(err, IsNil)

	v, err := getR.Reply()
	c.Check(err, IsNil)
	c.Check(v, Equals, "bar")
}

//------------------------------------------------------------------------------

func (t *RedisTest) BenchmarkPing(c *C) {
	for i := 0; i < c.N; i++ {
		t.redisC.Ping().Reply()
	}
}

func (t *RedisTest) BenchmarkSet(c *C) {
	for i := 0; i < c.N; i++ {
		t.redisC.Set("foo", "bar").Reply()
	}
}

func (t *RedisTest) BenchmarkGet(c *C) {
	for i := 0; i < c.N; i++ {
		t.redisC.Get("foo").Reply()
	}
}
