Readme
======

Sync Redis client for Golang.

Usage
-----

Example:

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
       panic(err)
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
       panic(err)
    }

    value2, err := futureGet2.Reply()
    if err != nil {
       panic(err)
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
           panic(err)
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

Client is thread safe. Internally sync.Mutex is used to synchronize writes and reads.
