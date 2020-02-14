package redis_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

var rdb *redis.Client

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:         ":6379",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
}

func ExampleNewClient() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	pong, err := rdb.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleParseURL() {
	opt, err := redis.ParseURL("redis://:qwerty@localhost:6379/1")
	if err != nil {
		panic(err)
	}
	fmt.Println("addr is", opt.Addr)
	fmt.Println("db is", opt.DB)
	fmt.Println("password is", opt.Password)

	// Create client as usually.
	_ = redis.NewClient(opt)

	// Output: addr is localhost:6379
	// db is 1
	// password is qwerty
}

func ExampleNewFailoverClient() {
	// See http://redis.io/topics/sentinel for instructions how to
	// setup Redis Sentinel.
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "master",
		SentinelAddrs: []string{":26379"},
	})
	rdb.Ping()
}

func ExampleNewClusterClient() {
	// See http://redis.io/topics/cluster-tutorial for instructions
	// how to setup Redis Cluster.
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	})
	rdb.Ping()
}

// Following example creates a cluster from 2 master nodes and 2 slave nodes
// without using cluster mode or Redis Sentinel.
func ExampleNewClusterClient_manualSetup() {
	// clusterSlots returns cluster slots information.
	// It can use service like ZooKeeper to maintain configuration information
	// and Cluster.ReloadState to manually trigger state reloading.
	clusterSlots := func() ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// First node with 1 master and 1 slave.
			{
				Start: 0,
				End:   8191,
				Nodes: []redis.ClusterNode{{
					Addr: ":7000", // master
				}, {
					Addr: ":8000", // 1st slave
				}},
			},
			// Second node with 1 master and 1 slave.
			{
				Start: 8192,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: ":7001", // master
				}, {
					Addr: ":8001", // 1st slave
				}},
			},
		}
		return slots, nil
	}

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  clusterSlots,
		RouteRandomly: true,
	})
	rdb.Ping()

	// ReloadState reloads cluster state. It calls ClusterSlots func
	// to get cluster slots information.
	err := rdb.ReloadState()
	if err != nil {
		panic(err)
	}
}

func ExampleNewRing() {
	rdb := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": ":7000",
			"shard2": ":7001",
			"shard3": ":7002",
		},
	})
	rdb.Ping()
}

func ExampleClient() {
	err := rdb.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get("missing_key").Result()
	if err == redis.Nil {
		fmt.Println("missing_key does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("missing_key", val2)
	}
	// Output: key value
	// missing_key does not exist
}

func ExampleConn() {
	conn := rdb.Conn()

	err := conn.ClientSetName("foobar").Err()
	if err != nil {
		panic(err)
	}

	// Open other connections.
	for i := 0; i < 10; i++ {
		go rdb.Ping()
	}

	s, err := conn.ClientGetName().Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(s)
	// Output: foobar
}

func ExampleClient_Set() {
	// Last argument is expiration. Zero means the key has no
	// expiration time.
	err := rdb.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	// key2 will expire in an hour.
	err = rdb.Set("key2", "value", time.Hour).Err()
	if err != nil {
		panic(err)
	}
}

func ExampleClient_Incr() {
	result, err := rdb.Incr("counter").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
	// Output: 1
}

func ExampleClient_BLPop() {
	if err := rdb.RPush("queue", "message").Err(); err != nil {
		panic(err)
	}

	// use `rdb.BLPop(0, "queue")` for infinite waiting time
	result, err := rdb.BLPop(1*time.Second, "queue").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(result[0], result[1])
	// Output: queue message
}

func ExampleClient_Scan() {
	rdb.FlushDB()
	for i := 0; i < 33; i++ {
		err := rdb.Set(fmt.Sprintf("key%d", i), "value", 0).Err()
		if err != nil {
			panic(err)
		}
	}

	var cursor uint64
	var n int
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Scan(cursor, "key*", 10).Result()
		if err != nil {
			panic(err)
		}
		n += len(keys)
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("found %d keys\n", n)
	// Output: found 33 keys
}

func ExampleClient_Pipelined() {
	var incr *redis.IntCmd
	_, err := rdb.Pipelined(func(pipe redis.Pipeliner) error {
		incr = pipe.Incr("pipelined_counter")
		pipe.Expire("pipelined_counter", time.Hour)
		return nil
	})
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExampleClient_Pipeline() {
	pipe := rdb.Pipeline()

	incr := pipe.Incr("pipeline_counter")
	pipe.Expire("pipeline_counter", time.Hour)

	// Execute
	//
	//     INCR pipeline_counter
	//     EXPIRE pipeline_counts 3600
	//
	// using one rdb-server roundtrip.
	_, err := pipe.Exec()
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExampleClient_TxPipelined() {
	var incr *redis.IntCmd
	_, err := rdb.TxPipelined(func(pipe redis.Pipeliner) error {
		incr = pipe.Incr("tx_pipelined_counter")
		pipe.Expire("tx_pipelined_counter", time.Hour)
		return nil
	})
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExampleClient_TxPipeline() {
	pipe := rdb.TxPipeline()

	incr := pipe.Incr("tx_pipeline_counter")
	pipe.Expire("tx_pipeline_counter", time.Hour)

	// Execute
	//
	//     MULTI
	//     INCR pipeline_counter
	//     EXPIRE pipeline_counts 3600
	//     EXEC
	//
	// using one rdb-server roundtrip.
	_, err := pipe.Exec()
	fmt.Println(incr.Val(), err)
	// Output: 1 <nil>
}

func ExampleClient_Watch() {
	const routineCount = 100

	// Transactionally increments key using GET and SET commands.
	increment := func(key string) error {
		txf := func(tx *redis.Tx) error {
			// get current value or zero
			n, err := tx.Get(key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			// actual opperation (local in optimistic lock)
			n++

			// runs only if the watched keys remain unchanged
			_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
				// pipe handles the error case
				pipe.Set(key, n, 0)
				return nil
			})
			return err
		}

		for retries := routineCount; retries > 0; retries-- {
			err := rdb.Watch(txf, key)
			if err != redis.TxFailedErr {
				return err
			}
			// optimistic lock lost
		}
		return errors.New("increment reached maximum number of retries")
	}

	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment error:", err)
			}
		}()
	}
	wg.Wait()

	n, err := rdb.Get("counter3").Int()
	fmt.Println("ended with", n, err)
	// Output: ended with 100 <nil>
}

func ExamplePubSub() {
	pubsub := rdb.Subscribe("mychannel1")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive()
	if err != nil {
		panic(err)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	// Publish a message.
	err = rdb.Publish("mychannel1", "hello").Err()
	if err != nil {
		panic(err)
	}

	time.AfterFunc(time.Second, func() {
		// When pubsub is closed channel is closed too.
		_ = pubsub.Close()
	})

	// Consume messages.
	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)
	}

	// Output: mychannel1 hello
}

func ExamplePubSub_Receive() {
	pubsub := rdb.Subscribe("mychannel2")
	defer pubsub.Close()

	for i := 0; i < 2; i++ {
		// ReceiveTimeout is a low level API. Use ReceiveMessage instead.
		msgi, err := pubsub.ReceiveTimeout(time.Second)
		if err != nil {
			break
		}

		switch msg := msgi.(type) {
		case *redis.Subscription:
			fmt.Println("subscribed to", msg.Channel)

			_, err := rdb.Publish("mychannel2", "hello").Result()
			if err != nil {
				panic(err)
			}
		case *redis.Message:
			fmt.Println("received", msg.Payload, "from", msg.Channel)
		default:
			panic("unreached")
		}
	}

	// sent message to 1 rdb
	// received hello from mychannel2
}

func ExampleScript() {
	IncrByXX := redis.NewScript(`
		if redis.call("GET", KEYS[1]) ~= false then
			return redis.call("INCRBY", KEYS[1], ARGV[1])
		end
		return false
	`)

	n, err := IncrByXX.Run(rdb, []string{"xx_counter"}, 2).Result()
	fmt.Println(n, err)

	err = rdb.Set("xx_counter", "40", 0).Err()
	if err != nil {
		panic(err)
	}

	n, err = IncrByXX.Run(rdb, []string{"xx_counter"}, 2).Result()
	fmt.Println(n, err)

	// Output: <nil> redis: nil
	// 42 <nil>
}

func Example_customCommand() {
	Get := func(rdb *redis.Client, key string) *redis.StringCmd {
		cmd := redis.NewStringCmd("get", key)
		rdb.Process(cmd)
		return cmd
	}

	v, err := Get(rdb, "key_does_not_exist").Result()
	fmt.Printf("%q %s", v, err)
	// Output: "" redis: nil
}

func Example_customCommand2() {
	v, err := rdb.Do("get", "key_does_not_exist").Text()
	fmt.Printf("%q %s", v, err)
	// Output: "" redis: nil
}

func ExampleScanIterator() {
	iter := rdb.Scan(0, "", 0).Iterator()
	for iter.Next() {
		fmt.Println(iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func ExampleScanCmd_Iterator() {
	iter := rdb.Scan(0, "", 0).Iterator()
	for iter.Next() {
		fmt.Println(iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func ExampleNewUniversalClient_simple() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	defer rdb.Close()

	rdb.Ping()
}

func ExampleNewUniversalClient_failover() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		MasterName: "master",
		Addrs:      []string{":26379"},
	})
	defer rdb.Close()

	rdb.Ping()
}

func ExampleNewUniversalClient_cluster() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	})
	defer rdb.Close()

	rdb.Ping()
}
