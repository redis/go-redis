/*
Package github.com/vmihailenco/redis implements a Redis client.

Let's start with connecting to Redis using TCP:

    password := ""  // no password set
    db := int64(-1) // use default DB
    client := redis.NewTCPClient("localhost:6379", password, db)
    defer client.Close()

    ping := client.Ping()
    fmt.Println(ping.Err(), ping.Val())
    // Output: <nil> PONG

or using Unix socket:

    client := redis.NewUnixClient("/tmp/redis.sock", "", -1)
    defer client.Close()

    ping := client.Ping()
    fmt.Println(ping.Err(), ping.Val())
    // Output: <nil> PONG

Then we can start sending commands:

    set := client.Set("foo", "bar")
    fmt.Println(set.Err(), set.Val())

    get := client.Get("foo")
    fmt.Println(get.Err(), get.Val())

    // Output: <nil> OK
    // <nil> bar

We can also pipeline two commands together:

    var set *redis.StatusReq
    var get *redis.StringReq
    reqs, err := client.Pipelined(func(c *redis.PipelineClient) {
        set = c.Set("key1", "hello1")
        get = c.Get("key2")
    })
    fmt.Println(err, reqs)
    fmt.Println(set)
    fmt.Println(get)
    // Output: <nil> [SET key1 hello1: OK GET key2: (nil)]
    // SET key1 hello1: OK
    // GET key2: (nil)

or:

    var set *redis.StatusReq
    var get *redis.StringReq
    reqs, err := client.Pipelined(func(c *redis.PipelineClient) {
        set = c.Set("key1", "hello1")
        get = c.Get("key2")
    })
    fmt.Println(err, reqs)
    fmt.Println(set)
    fmt.Println(get)
    // Output: <nil> [SET key1 hello1 GET key2]
    // SET key1 hello1
    // GET key2

We can also send several commands in transaction:

    func transaction(multi *redis.MultiClient) ([]redis.Req, error) {
        get := multi.Get("key")
        if err := get.Err(); err != nil && err != redis.Nil {
            return nil, err
        }

        val, _ := strconv.ParseInt(get.Val(), 10, 64)

        reqs, err := multi.Exec(func() {
            multi.Set("key", strconv.FormatInt(val+1, 10))
        })
        // Transaction failed. Repeat.
        if err == redis.Nil {
            return transaction(multi)
        }
        return reqs, err
    }

    multi, err := client.MultiClient()
    _ = err
    defer multi.Close()

    watch := multi.Watch("key")
    _ = watch.Err()

    reqs, err := transaction(multi)
    fmt.Println(err, reqs)

    // Output: <nil> [SET key 1: OK]

To subscribe to the channel:

    pubsub, err := client.PubSubClient()
    defer pubsub.Close()

    ch, err := pubsub.Subscribe("mychannel")
    _ = err

    subscribeMsg := <-ch
    fmt.Println(subscribeMsg.Err, subscribeMsg.Name)

    pub := client.Publish("mychannel", "hello")
    _ = pub.Err()

    msg := <-ch
    fmt.Println(msg.Err, msg.Message)

    // Output: <nil> subscribe
    // <nil> hello

You can also write custom commands:

    func Get(client *redis.Client, key string) *redis.StringReq {
        req := redis.NewStringReq("GET", key)
        client.Process(req)
        return req
    }

    get := Get(client, "key_does_not_exist")
    fmt.Println(get.Err(), get.Val())
    // Output: (nil)

Client uses connection pool to send commands. You can change maximum number of connections with:

    client.ConnPool.(*redis.MultiConnPool).MaxCap = 1
*/
package redis
