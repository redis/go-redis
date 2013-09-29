package redis_test

import (
	"fmt"
	"strconv"

	"github.com/vmihailenco/redis/v2"
)

func ExampleNewTCPClient() {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleNewUnixClient() {
	client := redis.NewUnixClient(&redis.Options{
		Addr: "/tmp/redis.sock",
	})
	defer client.Close()

	ping := client.Ping()
	fmt.Println(ping.Err(), ping.Val())
	// Output: <nil> PONG
}

func ExampleSet() {
	client := redis.NewTCPClient(&redis.Options{
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

func ExamplePipelinePipelined() {
	client := redis.NewTCPClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	cmds, err := client.Pipelined(func(c *redis.Pipeline) {
		c.Set("key1", "hello1")
		c.Get("key2")
	})
	fmt.Println(cmds, err)
	// Output: [SET key1 hello1: OK GET key2: (nil)] (nil)
}

func ExamplePipeline() {
	client := redis.NewTCPClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	pipeline := client.Pipeline()
	set := pipeline.Set("key1", "hello1")
	get := pipeline.Get("key2")
	cmds, err := pipeline.Exec()
	fmt.Println(cmds, err)
	fmt.Println(set)
	fmt.Println(get)
	// Output: [SET key1 hello1: OK GET key2: (nil)] (nil)
	// SET key1 hello1: OK
	// GET key2: (nil)
}

func ExampleMulti() {
	incr := func(tx *redis.Multi) ([]redis.Cmder, error) {
		get := tx.Get("key")
		if err := get.Err(); err != nil && err != redis.Nil {
			return nil, err
		}

		val, _ := strconv.ParseInt(get.Val(), 10, 64)

		return tx.Exec(func() {
			tx.Set("key", strconv.FormatInt(val+1, 10))
		})
	}

	client := redis.NewTCPClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	client.Del("key")

	tx := client.Multi()
	defer tx.Close()

	watch := tx.Watch("key")
	_ = watch.Err()

	for {
		cmds, err := incr(tx)
		if err == redis.Nil {
			// Transaction failed. Repeat.
			continue
		} else if err != nil {
			panic(err)
		}
		fmt.Println(err, cmds)
		break
	}

	// Output: <nil> [SET key 1: OK]
}

func ExamplePubSub() {
	client := redis.NewTCPClient(&redis.Options{
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

func Get(client *redis.Client, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("GET", key)
	client.Process(cmd)
	return cmd
}

func ExampleCustomCommand() {
	client := redis.NewTCPClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	get := Get(client, "key_does_not_exist")
	fmt.Println(get.Err(), get.Val())
	// Output: (nil)
}
