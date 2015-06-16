package redis_test

import (
	"fmt"
	"net"
	"strconv"
	"sync"
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

func ExampleNewRing() {
	client := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": ":7000",
			"shard2": ":7001",
			"shard3": ":7002",
		},
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
	var incr *redis.IntCmd
	_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
		incr = pipe.Incr("counter1")
		pipe.Expire("counter1", time.Hour)
		return nil
	})
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExamplePipeline() {
	pipe := client.Pipeline()
	defer pipe.Close()

	incr := pipe.Incr("counter2")
	pipe.Expire("counter2", time.Hour)
	_, err := pipe.Exec()
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExampleMulti() {
	// Transactionally increments key using GET and SET commands.
	incr := func(tx *redis.Multi, key string) error {
		err := tx.Watch(key).Err()
		if err != nil {
			return err
		}

		n, err := tx.Get(key).Int64()
		if err != nil && err != redis.Nil {
			return err
		}

		_, err = tx.Exec(func() error {
			tx.Set(key, strconv.FormatInt(n+1, 10), 0)
			return nil
		})
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx := client.Multi()
			defer tx.Close()

			for {
				err := incr(tx, "counter3")
				if err == redis.TxFailedErr {
					// Retry.
					continue
				} else if err != nil {
					panic(err)
				}
				break
			}
		}()
	}
	wg.Wait()

	n, err := client.Get("counter3").Int64()
	fmt.Println(n, err)
	// Output: 10 <nil>
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
	IncrByXX := redis.NewScript(`
		if redis.call("GET", KEYS[1]) ~= false then
			return redis.call("INCRBY", KEYS[1], ARGV[1])
		end
		return false
	`)

	n, err := IncrByXX.Run(client, []string{"xx_counter"}, []string{"2"}).Result()
	fmt.Println(n, err)

	err = client.Set("xx_counter", "40", 0).Err()
	if err != nil {
		panic(err)
	}

	n, err = IncrByXX.Run(client, []string{"xx_counter"}, []string{"2"}).Result()
	fmt.Println(n, err)

	// Output: <nil> redis: nil
	// 42 <nil>
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
