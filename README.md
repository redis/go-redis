Readme
======

Redis client for Golang.

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
         _, err := client.Auth("foo").Reply()
         if err != nil {
             return err
         }

         _, err = client.Ping().Reply()
         if err != nil {
             return err
         }

         return nil
    }

    redisClient := redis.NewClient(openConn, closeConn, initConn)

Both `closeConn` and `initConn` functions can be `nil`.

Running commands
----------------

    _, err := redisClient.Set("foo", "bar").Reply()
    if err != nil {
        panic(err)
    }

    value, err := redisClient.Get("foo").Reply()
    if err != nil {
        if err != redis.Nil {
            panic(err)
        }
    }

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

    ok, err := setReq.Reply()
    if err != nil {
        panic(err)
    }

    value, err := getReq.Reply()
    if err != nil {
        if err != redis.Nil {
            panic(err)
        }
    }

Multi/Exec
----------

Example 1:

    multiClient := redisClient.Multi()

    futureGet1 := multiClient.Get("foo1")
    futureGet2 := multiClient.Get("foo2")
    _, err := multiClient.Exec()
    if err != nil {
       panic(err)
    }

    value1, err := futureGet1.Reply()
    if err != nil {
        if err != redis.Nil {
            panic(err)
        }
    }

    value2, err := futureGet2.Reply()
    if err != nil {
        if err != redis.Nil {
            panic(err)
        }
    }

Example 2:

    multiClient := redisClient.Multi()

    multiClient.Get("foo1")
    multiClient.Get("foo2")
    reqs, err := multiClient.Exec()
    if err != nil {
        panic(err)
    }

    for req := range reqs {
        value, err := req.Reply()
        if err != nil {
            if err != redis.Nil {
                panic(err)
            }
        }
    }

Pub/sub
-------

Publish:

    _, err := redisClient.Publish("mychannel", "hello").Reply()
    if err != nil {
        panic(err)
    }

Subscribe:

    pubsub := redisClient.PubSubClient()

    ch, err := pubsub.Subscribe("mychannel")
    if err != nil {
        panic(err)
    }

    go func() {
        for msg := range ch {
            if msg.Err != nil {
                panic(err)
            }
            fmt.Println(msg.Message)
        }
    }

Thread safety
-------------

redis.Client methods are thread safe. Following code is correct:

    for i := 0; i < 1000; i++ {
        go func() {
            redisClient.Incr("foo").Reply()
        }()
    }

Custom commands
---------------

Example:

    func Get(client *redis.Client, key string) *redis.BulkReq {
        req := redis.NewBulkReq("GET", key)
        client.Queue(req)
        return req
    }

    value, err := Get(redisClient, "foo").Reply()
    if err != nil {
        if err != redis.Nil {
            panic(err)
        }
    }

Connection pool
---------------

Client uses connection pool with default capacity of 10 connections. To change pool capacity:

    redisClient.ConnPool.(*redis.MultiConnPool).MaxCap = 1
