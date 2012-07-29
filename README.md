Readme
======

Redis client for Golang.

Usage
-----

Example:

    import "github.com/vmihailenco/redis"


    connect := func() (io.ReadWriter, error) {
        fmt.Println("Connecting...")
        return net.Dial("tcp", "localhost:6379")
    }

    disconnect := func(conn io.ReadWriter) error {
        fmt.Println("Disconnecting...")
        conn.Close()
        return nil
    }

    redisClient = redis.NewClient(connect, disconnect)

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

    setReq := redisClient.Set("foo1", "bar1") // queue command SET
    getReq := redisClient.Get("foo2") // queue command GET

    reqs, err := redisClient.RunQueued() // run queued commands
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

Getting multiClient:

    multiClient := redisClient.Multi()

Or:

    multiClient = redis.NewMultiClient(connect, disconnect)

Working with multiClient:

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

Or:

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
    // pubsub := redis.NewPubSubClient(connect, disconnect)

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

Client is thread safe. Internally sync.Mutex is used to synchronize writes and reads.

Custom commands
---------------

Lazy command:

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

Immediate command:

    func Quit(client *redis.Client) *redis.StatusReq {
        req := redis.NewStatusReq("QUIT")
        client.Run(req)
        client.Close()
        return req
    }

    status, err := Quit(redisClient).Reply()
    if err != nil {
        panic(err)
    }

Connection pool
---------------

Client does not support connection pool.
