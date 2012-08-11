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

Contributing
------------

Configure Redis to allow maximum 10 clients:

    maxclients 10

Run tests:

    go test -gocheck.v

Run benchmarks:

    go test -gocheck.b

Getting Client instance
-----------------------

Example 1:

    import "github.com/vmihailenco/redis"


    address := ":6379"
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
        conn.Close()
        return nil
    }

    initConn := func(client *redis.Client) error {
         auth := client.Auth("foo")
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

    set := redisClient.Set("foo", "bar")
    if set.Err() != nil {
        panic(set.Err())
    }
    ok := set.Val()

    get := redisClient.Get("foo")
    if get.Err() != nil && get.Err() != redis.Nil {
        panic(get.Err())
    }
    val := get.Val()

Pipelining
----------

Client has ability to run commands in batches:

    pipeline, err := redisClient.PipelineClient()
    if err != nil {
        panic(err)
    }
    defer pipeline.Close()

    setReq := pipeline.Set("foo1", "bar1") // queue command SET
    getReq := pipeline.Get("foo2") // queue command GET

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

    multiClient, err := redisClient.MultiClient()
    if err != nil {
        panic(err)
    }
    defer multiClient.Close()

    watch := multiClient.Watch("foo")
    if watch.Err() != nil {
        panic(watch.Err())
    }

    get := multiClient.Get("foo")
    if get.Err() != nil {
        panic(get.Err())
    }

    // Start transaction.
    multiClient.Multi()

    set := multiClient.Set("foo", get.Val() + "1")

    // Commit transaction.
    reqs, err := multiClient.Exec()
    if err == redis.Nil {
        // Repeat transaction.
    } else if err != nil {
        panic(err)
    }
    for _, req := range reqs {
        // ...
    }

    ok := set.Val()

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
            redisClient.Incr("foo")
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

    get := Get(redisClient, "foo")
    if get.Err() != nil && get.Err() != redis.Nil {
        panic(get.Err())
    }

Connection pool
---------------

Client uses connection pool with default capacity of 10 connections. To change pool capacity:

    redisClient.ConnPool.(*redis.MultiConnPool).MaxCap = 1
