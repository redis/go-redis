package redis_test

import (
	"fmt"
	"strconv"

	"github.com/vmihailenco/redis"
)

func ExampleTCPClient() {
	password := ""  // no password set
	db := int64(-1) // use default DB
	client := redis.NewTCPClient("localhost:6379", password, db)
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleUnixClient() {
	client := redis.NewUnixClient("/tmp/redis.sock", "", -1)
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleSetGet() {
	client := redis.NewTCPClient(":6379", "", -1)
	defer client.Close()

	set := client.Set("foo", "bar")
	fmt.Println(set.Err(), set.Val())

	get := client.Get("foo")
	fmt.Println(get.Err(), get.Val())

	// Output: <nil> OK
	// <nil> bar
}

func ExamplePipeline() {
	client := redis.NewTCPClient(":6379", "", -1)
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
	client := redis.NewTCPClient(":6379", "", -1)
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
	client := redis.NewTCPClient(":6379", "", -1)
	defer client.Close()

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
}

func Get(client *redis.Client, key string) *redis.StringReq {
	req := redis.NewStringReq("GET", key)
	client.Process(req)
	return req
}

func ExampleCustomCommand() {
	client := redis.NewTCPClient(":6379", "", -1)
	defer client.Close()

	get := Get(client, "key_does_not_exist")
	fmt.Println(get.Err(), get.Val())
	// Output: (nil)
}
