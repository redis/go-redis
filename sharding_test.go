package redis_test

import (
	"fmt"

	"gopkg.in/redis.v1"

	. "launchpad.net/gocheck"
)

var (
	shard1 = &redis.Options{
		Addr: redisAddr,
		DB:   10,
	}
	shard2 = &redis.Options{
		Addr: redisAddr,
		DB:   11,
	}
)

type ShardingTest struct {
	shard1, shard2, client *redis.Client
}

var _ = Suite(&ShardingTest{})

func (t *ShardingTest) SetUpTest(c *C) {
	t.shard1 = redis.NewTCPClient(shard1)
	t.shard2 = redis.NewTCPClient(shard2)
	t.client = redis.NewShardingClient(&redis.ShardingOptions{
		Shard: func(cmds []redis.Cmder) (interface{}, error) {
			switch key := cmds[0].Arg(1); key {
			case "key1":
				return shard1, nil
			case "key2":
				return shard2, nil
			default:
				return nil, fmt.Errorf("can't pick shard for %q", key)
			}
		},
	})
}

func (t *ShardingTest) TestSharding(c *C) {
	{
		c.Assert(t.client.Set("key1", "shard1").Err(), IsNil)
		val, err := t.client.Get("key1").Result()
		c.Assert(err, IsNil)
		c.Assert(val, Equals, "shard1")
		c.Assert(val, Equals, t.shard1.Get("key1").Val())
	}

	{
		c.Assert(t.client.Set("key2", "shard2").Err(), IsNil)
		val, err := t.client.Get("key2").Result()
		c.Assert(err, IsNil)
		c.Assert(val, Equals, "shard2")
		c.Assert(val, Equals, t.shard2.Get("key2").Val())
	}

	{
		c.Assert(t.shard1.Get("shard2").Err(), Equals, redis.Nil)
		c.Assert(t.shard2.Get("shard1").Err(), Equals, redis.Nil)
	}
}

func (t *ShardingTest) TestUnknownShard(c *C) {
	err := t.client.Set("key3", "hello").Err()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, `can't pick shard for "key3"`)
}
