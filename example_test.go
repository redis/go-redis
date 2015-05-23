package redis_test

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"gopkg.in/redis.v3"
)

var client *redis.Client

func init() {
	client = redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	client.FlushDb()
}

func ExampleNewClient() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleNewFailoverClient() {
	// See http://redis.io/topics/sentinel for instructions how to
	// setup Redis Sentinel.
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "master",
		SentinelAddrs: []string{":26379"},
	})
	client.Ping()
}

func ExampleNewClusterClient() {
	// See http://redis.io/topics/cluster-tutorial for instructions
	// how to setup Redis Cluster.
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	})
	client.Ping()
}

func ExampleClient() {
	err := client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exists")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exists
}

func ExampleClient_Incr() {
	if err := client.Incr("counter").Err(); err != nil {
		panic(err)
	}

	n, err := client.Get("counter").Int64()
	fmt.Println(n, err)
	// Output: 1 <nil>
}

func ExampleClient_Pipelined() {
	cmds, err := client.Pipelined(func(c *redis.Pipeline) error {
		c.Set("key1", "hello1", 0)
		c.Get("key1")
		return nil
	})
	fmt.Println(err)
	set := cmds[0].(*redis.StatusCmd)
	fmt.Println(set)
	get := cmds[1].(*redis.StringCmd)
	fmt.Println(get)
	// Output: <nil>
	// SET key1 hello1: OK
	// GET key1: hello1
}

func ExamplePipeline() {
	pipeline := client.Pipeline()
	set := pipeline.Set("key1", "hello1", 0)
	get := pipeline.Get("key1")
	cmds, err := pipeline.Exec()
	fmt.Println(cmds, err)
	fmt.Println(set)
	fmt.Println(get)
	// Output: [SET key1 hello1: OK GET key1: hello1] <nil>
	// SET key1 hello1: OK
	// GET key1: hello1
}

func ExampleMulti() {
	incr := func(tx *redis.Multi) ([]redis.Cmder, error) {
		s, err := tx.Get("key").Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		n, _ := strconv.ParseInt(s, 10, 64)

		return tx.Exec(func() error {
			tx.Set("key", strconv.FormatInt(n+1, 10), 0)
			return nil
		})
	}

	client.Del("key")

	tx := client.Multi()
	defer tx.Close()

	watch := tx.Watch("key")
	_ = watch.Err()

	for {
		cmds, err := incr(tx)
		if err == redis.TxFailedErr {
			continue
		} else if err != nil {
			panic(err)
		}
		fmt.Println(cmds, err)
		break
	}

	// Output: [SET key 1: OK] <nil>
}

func ExamplePubSub() {
	pubsub := client.PubSub()
	defer pubsub.Close()

	err := pubsub.Subscribe("mychannel")
	if err != nil {
		panic(err)
	}

	err = client.Publish("mychannel", "hello").Err()
	if err != nil {
		panic(err)
	}

	for {
		msgi, err := pubsub.ReceiveTimeout(100 * time.Millisecond)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				// There are no more messages to process. Stop.
				break
			}
			panic(err)
		}

		switch msg := msgi.(type) {
		case *redis.Subscription:
			fmt.Println(msg.Kind, msg.Channel)
		case *redis.Message:
			fmt.Println(msg.Channel, msg.Payload)
		default:
			panic(fmt.Sprintf("unknown message: %#v", msgi))
		}
	}

	// Output: subscribe mychannel
	// mychannel hello
}

func ExampleScript() {
	setnx := redis.NewScript(`
        if redis.call("get", KEYS[1]) == false then
            redis.call("set", KEYS[1], ARGV[1])
            return 1
        end
        return 0
    `)

	v1, err := setnx.Run(client, []string{"keynx"}, []string{"foo"}).Result()
	fmt.Println(v1.(int64), err)

	v2, err := setnx.Run(client, []string{"keynx"}, []string{"bar"}).Result()
	fmt.Println(v2.(int64), err)

	get := client.Get("keynx")
	fmt.Println(get)

	// Output: 1 <nil>
	// 0 <nil>
	// GET keynx: foo
}

func Example_customCommand() {
	Get := func(client *redis.Client, key string) *redis.StringCmd {
		cmd := redis.NewStringCmd("GET", key)
		client.Process(cmd)
		return cmd
	}

	v, err := Get(client, "key_does_not_exist").Result()
	fmt.Printf("%q %s", v, err)
	// Output: "" redis: nil
}
