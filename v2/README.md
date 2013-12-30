Redis client for Golang
=======================

Supports:

- Redis 2.6 commands except QUIT, MONITOR, SLOWLOG and SYNC.
- Pub/sub.
- Transactions.
- Pipelining.
- Connection pool.
- TLS connections.
- Thread safety.
- Timeouts.

API docs: http://godoc.org/github.com/vmihailenco/redis/v2.
Examples: http://godoc.org/github.com/vmihailenco/redis/v2#pkg-examples.

Installation
------------

Install:

    go get github.com/vmihailenco/redis/v2

Updgrading from previous version
--------------------------------

Type system should catch most changes. But you have to manually change `SetEx`, `PSetEx`, `Expire` and `PExpire` to use `time.Duration` instead of `int64`.

Getting started
---------------

Let's start with connecting to Redis using TCP:

```go
client := redis.NewTCPClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})
defer client.Close()

ping := client.Ping()
fmt.Println(ping.Val(), ping.Err())
// Output: PONG <nil>
```

or using Unix socket:

```go
client := redis.NewUnixClient(&redis.Options{
	Addr: "/tmp/redis.sock",
})
defer client.Close()

ping := client.Ping()
fmt.Println(ping.Val(), ping.Err())
// Output: PONG <nil>
```

Then we can start sending commands:

```go
set := client.Set("foo", "bar")
fmt.Println(set.Err(), set.Val())

get := client.Get("foo")
fmt.Println(get.Err(), get.Val())

// Output: <nil> OK
// <nil> bar
```

We can also pipeline two commands together:

```go
pipeline := client.Pipeline()
set := pipeline.Set("key1", "hello1")
get := pipeline.Get("key2")
cmds, err := pipeline.Exec()
fmt.Println(cmds, err)
fmt.Println(set)
fmt.Println(get)
// Output: [SET key1 hello1: OK GET key2: (nil)] (nil)
// SET key1 hello1: OK
// GET key2: (nil)
```

or:

```go
client := redis.NewTCPClient(&redis.Options{
	Addr: ":6379",
})
defer client.Close()

cmds, err := client.Pipelined(func(c *redis.Pipeline) {
	c.Set("key1", "hello1")
	c.Get("key2")
})
fmt.Println(cmds, err)
// Output: [SET key1 hello1: OK GET key2: (nil)] (nil)
```

We can also send several commands in transaction:

```go
incr := func(tx *redis.Multi) ([]redis.Cmder, error) {
	get := tx.Get("key")
	if err := get.Err(); err != nil && err != redis.Nil {
		return nil, err
	}

	val, _ := strconv.ParseInt(get.Val(), 10, 64)

	return tx.Exec(func() {
		tx.Set("key", strconv.FormatInt(val+1, 10))
	})
}

client := redis.NewTCPClient(&redis.Options{
	Addr: ":6379",
})
defer client.Close()

client.Del("key")

tx := client.Multi()
defer tx.Close()

watch := tx.Watch("key")
_ = watch.Err()

for {
	cmds, err := incr(tx)
	if err == redis.TxFailedErr {
		continue
	} else if err != nil {
		panic(err)
	}
	fmt.Println(cmds, err)
	break
}

// Output: [SET key 1: OK] <nil>
```

To subscribe to the channel:

```go
pubsub := client.PubSub()
defer pubsub.Close()

err := pubsub.Subscribe("mychannel")
_ = err

msg, err := pubsub.Receive()
fmt.Println(msg, err)

pub := client.Publish("mychannel", "hello")
_ = pub.Err()

msg, err = pubsub.Receive()
fmt.Println(msg, err)

// Output: &{subscribe mychannel 1} <nil>
// &{mychannel hello} <nil>
```

To use Lua scripting:

```go
client := redis.NewTCPClient(&redis.Options{
	Addr: ":6379",
})
defer client.Close()

setnx := redis.NewScript(`
    if redis.call("get", KEYS[1]) == false then
        redis.call("set", KEYS[1], ARGV[1])
        return 1
    end
    return 0
`)

run1 := setnx.Run(client, []string{"keynx"}, []string{"foo"})
fmt.Println(run1.Val().(int64), run1.Err())

run2 := setnx.Run(client, []string{"keynx"}, []string{"bar"})
fmt.Println(run2.Val().(int64), run2.Err())

get := client.Get("keynx")
fmt.Println(get)

// Output: 1 <nil>
// 0 <nil>
// GET keynx: foo
```

You can also write custom commands:

```go
Get := func(client *redis.Client, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("GET", key)
	client.Process(cmd)
	return cmd
}

client := redis.NewTCPClient(&redis.Options{
	Addr: ":6379",
})
defer client.Close()

get := Get(client, "key_does_not_exist")
fmt.Printf("%q %s", get.Val(), get.Err())
// Output: "" (nil)
```

Look and feel
-------------

Some corner cases:

    SORT list LIMIT 0 2 ASC
    client.Sort("list", redis.Sort{Offset: 0, Count: 2, Order: "ASC"})

    ZRANGEBYSCORE zset -inf +inf WITHSCORES LIMIT 0 2
    client.ZRangeByScoreWithScores("zset", redis.ZRangeByScore{
        Min: "-inf",
        Max: "+inf",
        Offset: 0,
        Count: 2,
    })

    ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE SUM
    client.ZInterStore("out", redis.ZStore{Weights: []int64{2, 3}}, "zset1", "zset2")

    EVAL "return {KEYS[1],ARGV[1]}" 1 "key" "hello"
    client.Eval("return {KEYS[1],ARGV[1]}", []string{"key"}, []string{"hello"})

Contributing
------------

Configure Redis to allow maximum 10 clients:

    maxclients 10

Run tests:

    go test -gocheck.v

Run benchmarks:

    go test -gocheck.b
