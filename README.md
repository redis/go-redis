Readme
======

Redis client for Golang.

Supports:

- Redis 2.6 commands except QUIT command.
- Pub/sub.
- Transactions.
- Pipelining.
- Connection pool.
- TLS connections.
- Thread safety.

Installation
------------

Install:

    go get github.com/vmihailenco/redis

Getting Client instance
-----------------------

Example 1:

    import "github.com/vmihailenco/redis"


    address := "localhost:6379"
    password := "secret"
    db := 0
    redisClient := redis.NewTCPClient(address, password, db)

Example 2:

    import "github.com/vmihailenco/redis"


    openConn := func() (io.ReadWriteCloser, error) {
        fmt.Println("Connecting...")
        return net.Dial("tcp", ":6379")
    }

    closeConn := func(conn io.ReadWriteCloser) error {
        fmt.Println("Disconnecting...")
        return nil
    }

    initConn := func(client *redis.Client) error {
         auth := client.Auth("key")
         if auth.Err() != nil {
             return auth.Err()
         }

         ping := client.Ping()
         if ping.Err() != nil {
             return ping.Err()
         }

         return nil
    }

    redisClient := redis.NewClient(openConn, closeConn, initConn)

Both `closeConn` and `initConn` functions can be `nil`.

Running commands
----------------

    set := redisClient.Set("key", "hello")
    if set.Err() != nil {
        panic(set.Err())
    }
    ok := set.Val()

    get := redisClient.Get("key")
    if get.Err() != nil && get.Err() != redis.Nil {
        panic(get.Err())
    }
    val := get.Val()

Pipelining
----------

Client has ability to run commands in batches:

    reqs, err := redisClient.Pipelined(func(c *redis.PipelineClient) {
        c.Set("key1", "hello1") // queue command SET
        c.Set("key2", "hello2") // queue command SET
    })
    if err != nil {
        panic(err)
    }
    for _, req := range reqs {
        // ...
    }

Or:

    pipeline, err := redisClient.PipelineClient()
    if err != nil {
        panic(err)
    }
    defer pipeline.Close()

    setReq := pipeline.Set("key1", "hello1") // queue command SET
    getReq := pipeline.Get("key2") // queue command GET

    reqs, err := pipeline.RunQueued() // run queued commands
    if err != nil {
        panic(err)
    }
    for _, req := range reqs {
        // ...
    }

    if setReq.Err() != nil {
        panic(setReq.Err())
    }

    if getReq.Err() != nil && getReq.Err() != redis.Nil {
        panic(getReq.Err())
    }

Multi/Exec
----------

Example:

    func transaction(multi *redis.MultiClient) ([]redis.Req, error) {
        get := multiClient.Get("key")
        if get.Err() != nil {
            panic(get.Err())
        }

        reqs, err = multiClient.Exec(func() {
            multi.Set("key", get.Val() + "1")
        })
        if err == redis.Nil {
            return transaction()
        }

        return reqs, err
    }

    multiClient, err := redisClient.MultiClient()
    if err != nil {
        panic(err)
    }
    defer multiClient.Close()

    watch := multiClient.Watch("key")
    if watch.Err() != nil {
        panic(watch.Err())
    }

    reqs, err := transaction(multiClient)
    if err != nil {
        panic(err)
    }
    for _, req := range reqs {
        // ...
    }

Pub/sub
-------

Publish:

    pub := redisClient.Publish("mychannel", "hello")
    if pub.Err() != nil {
        panic(pub.Err())
    }

Subscribe:

    pubsub, err := redisClient.PubSubClient()
    if err != nil {
        panic(err)
    }
    defer pubsub.Close()

    ch, err := pubsub.Subscribe("mychannel")
    if err != nil {
        panic(err)
    }

    go func() {
        for msg := range ch {
            if msg.Err != nil {
                panic(err)
            }
            message := msg.Message
        }
    }

Thread safety
-------------

Commands are thread safe. Following code is correct:

    for i := 0; i < 1000; i++ {
        go func() {
            redisClient.Incr("key")
        }()
    }

Custom commands
---------------

Example:

    func Get(client *redis.Client, key string) *redis.BulkReq {
        req := redis.NewBulkReq("GET", key)
        client.Process(req)
        return req
    }

    get := Get(redisClient, "key")
    if get.Err() != nil && get.Err() != redis.Nil {
        panic(get.Err())
    }

Connection pool
---------------

Client uses connection pool with default capacity of 10 connections. To change pool capacity:

    redisClient.ConnPool.(*redis.MultiConnPool).MaxCap = 1

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
