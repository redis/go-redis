Readme
======

Redis client for Golang.

Supports:

- Redis 2.6 commands except QUIT, MONITOR, SLOWLOG and SYNC.
- Pub/sub.
- Transactions.
- Pipelining.
- Connection pool.
- TLS connections.
- Thread safety.

API docs: http://go.pkgdoc.org/github.com/vmihailenco/redis

Installation
------------

Install:

    go get github.com/vmihailenco/redis

Getting started
---------------

Let's start with connecting to Redis:

    password := "" // no password set
    db := -1 // use default DB
    client := redis.NewTCPClient("localhost:6379", password, db)
    defer client.Close()

Then we can start sending commands:

    if err := client.Set("foo", "bar"); err != nil { panic(err) }

    get := client.Get("foo")
    if err := get.Err(); err != nil { panic(err) }
    fmt.Println(get.Val())

We can also pipeline two commands together:

    var set *redis.StatusReq
    var get *redis.StringReq
    reqs, err := client.Pipelined(func(c *redis.PipelineClient)) {
        set = c.Set("key1", "hello1")
        get = c.Get("key2")
    }
    if err != nil { panic(err) }
    if err := set.Err(); err != nil { panic(err) }
    if err := get.Err(); err != nil { panic(err) }
    fmt.Println(get.Val())
    fmt.Println(reqs[0] == set)
    fmt.Println(reqs[1] == get)

or:

    pipeline, err := client.PipelineClient()
    if err != nil {
        panic(err)
    }
    defer pipeline.Close()

    set := pipeline.Set("key1", "hello1")
    get := pipline.Get("key2")

    reqs, err := pipeline.RunQueued()
    if err != nil { panic(err) }

    if err := set.Err(); err != nil { panic(err) }
    if err := get.Err(); err != nil { panic(err) }
    fmt.Println(get.Val())
    fmt.Println(reqs[0] == set)
    fmt.Println(reqs[1] == get)

We can also send several commands in transaction:

    func incrKeyInTransaction(multi *redis.MultiClient) ([]redis.Req, error) {
        get := multi.Get("key")
        if err := get.Err(); err != nil {
            panic(err)
        }

        val, err := strconv.ParseInt(get.Val(), 10, 64)
        if err != nil { panic(err) }

        reqs, err = multi.Exec(func() {
            multi.Set("key", val + 1)
        })
        // Transaction failed. Repeat.
        if err == redis.Nil {
            return incrKeyInTransaction(multi)
        }
        return reqs, err
    }

    multi, err := client.MultiClient()
    if err != nil { panic(err) }
    defer multi.Close()

    watch := multi.Watch("key")
    if err := watch.Err(); err != nil { panic(err) }

    reqs, err := incrKeyInTransaction(multi)
    if err != nil { panic(err) }
    for _, req := range reqs {
        // ...
    }

To subscribe to the channel:

    pubsub, err := client.PubSubClient()
    if err != nil { panic(err) }
    defer pubsub.Close()

    ch, err := pubsub.Subscribe("mychannel")
    if err != nil { panic(err) }

    go func() {
        for msg := range ch {
            if err := msg.Err; err != nil { panic(err) }
            message := msg.Message
        }
    }

You can also write custom commands:

    func Get(client *redis.Client, key string) *redis.StringReq {
        req := redis.NewStringReq("GET", key)
        client.Process(req)
        return req
    }

    get := Get(redisClient, "key")
    if err := get.Err(); err != nil && err != redis.Nil { panic(err) }

Client uses connection pool to send commands. You can change maximum number of connections with:

    client.ConnPool.(*redis.MultiConnPool).MaxCap = 1

Look and feel
-------------

Some corner cases:

    SORT list LIMIT 0 2 ASC
    client.Sort("list", redis.Sort{Offset: 0, Count: 2, Order: "ASC"})

    ZRANGEBYSCORE zset -inf +inf WITHSCORES LIMIT 0 2
    client.ZRangeByScoreWithScores("zset", "-inf", "+inf", 0, 2)

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
