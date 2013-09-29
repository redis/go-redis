package redis_test

import (
	"fmt"
	"strconv"

	"github.com/vmihailenco/redis/v2"
)

func ExampleTCPClient() {
	client := redis.DialTCP(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleUnixClient() {
	client := redis.DialUnix(&redis.Options{
		Addr: "/tmp/redis.sock",
	})
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleSetGet() {
	client := redis.DialTCP(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	set := client.Set("foo", "bar")
	fmt.Println(set.Err(), set.Val())

	get := client.Get("foo")
	fmt.Println(get.Err(), get.Val())

	// Output: <nil> OK
	// <nil> bar
}

func ExamplePipeline() {
	client := redis.DialTCP(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

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
}

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

func ExampleTransaction() {
	client := redis.DialTCP(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	client.Del("key")

	multi, err := client.MultiClient()
	_ = err
	defer multi.Close()

	watch := multi.Watch("key")
	_ = watch.Err()

	reqs, err := transaction(multi)
	fmt.Println(err, reqs)

	// Output: <nil> [SET key 1: OK]
}

func ExamplePubSub() {
	client := redis.DialTCP(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

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
}

func Get(client *redis.Client, key string) *redis.StringReq {
	req := redis.NewStringReq("GET", key)
	client.Process(req)
	return req
}

func ExampleCustomCommand() {
	client := redis.DialTCP(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	get := Get(client, "key_does_not_exist")
	fmt.Println(get.Err(), get.Val())
	// Output: (nil)
}
