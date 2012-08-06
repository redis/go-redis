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

Run:

    go get github.com/vmihailenco/bufreader
    go get github.com/vmihailenco/redis

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


    openConn := func() (io.ReadWriter, error) {
        fmt.Println("Connecting...")
        return net.Dial("tcp", ":6379")
    }

    closeConn := func(conn io.ReadWriter) error {
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

Client has ability to run several commands with one read/write:

    multiClient := redisClient.Multi()

    setReq := multiClient.Set("foo1", "bar1") // queue command SET
    getReq := multiClient.Get("foo2") // queue command GET

    reqs, err := multiClient.RunQueued() // run queued commands
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

    multiClient := redisClient.Multi()

    get1 := multiClient.Get("foo1")
    get2 := multiClient.Get("foo2")
    reqs, err := multiClient.Exec()
    if err != nil {
        panic(err)
    }
    for _, req := range reqs {
        // ...
    }


    if get1.Err() != nil && get1.Err() != redis.Nil {
        panic(get1.Err())
    }
    val1 := get1.Val()

    if get2.Err() != nil && get2.Err() != redis.Nil {
        panic(get2.Err())
    }
    val2 := get2.Val()

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
