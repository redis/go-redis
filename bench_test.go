package redis_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func benchmarkRedisClient(ctx context.Context, poolSize int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
	})
	if err := client.FlushDB(ctx).Err(); err != nil {
		panic(err)
	}
	return client
}

func BenchmarkRedisPing(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedisClient(ctx, 10)
	defer rdb.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := rdb.Ping(ctx).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetGoroutines(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedisClient(ctx, 10)
	defer rdb.Close()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := rdb.Set(ctx, "hello", "world", 0).Err()
				if err != nil {
					panic(err)
				}
			}()
		}

		wg.Wait()
	}
}

func BenchmarkRedisGetNil(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Get(ctx, "key").Err(); err != redis.Nil {
				b.Fatal(err)
			}
		}
	})
}

type setStringBenchmark struct {
	poolSize  int
	valueSize int
}

func (bm setStringBenchmark) String() string {
	return fmt.Sprintf("pool=%d value=%d", bm.poolSize, bm.valueSize)
}

func BenchmarkRedisSetString(b *testing.B) {
	benchmarks := []setStringBenchmark{
		{10, 64},
		{10, 1024},
		{10, 64 * 1024},
		{10, 1024 * 1024},
		{10, 10 * 1024 * 1024},

		{100, 64},
		{100, 1024},
		{100, 64 * 1024},
		{100, 1024 * 1024},
		{100, 10 * 1024 * 1024},
	}
	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			ctx := context.Background()
			client := benchmarkRedisClient(ctx, bm.poolSize)
			defer client.Close()

			value := strings.Repeat("1", bm.valueSize)

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := client.Set(ctx, "key", value, 0).Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisSetGetBytes(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'1'}, 10000)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set(ctx, "key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}

			got, err := client.Get(ctx, "key").Bytes()
			if err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
			}
		}
	})
}

func BenchmarkRedisMGet(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	if err := client.MSet(ctx, "key1", "hello1", "key2", "hello2").Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.MGet(ctx, "key1", "key2").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetExpire(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set(ctx, "key", "hello", 0).Err(); err != nil {
				b.Fatal(err)
			}
			if err := client.Expire(ctx, "key", time.Second).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPipeline(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "hello", 0)
				pipe.Expire(ctx, "key", time.Second)
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkZAdd(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.ZAdd(ctx, "key", redis.Z{
				Score:  float64(1),
				Member: "hello",
			}).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkXRead(b *testing.B) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	args := redis.XAddArgs{
		Stream: "1",
		ID:     "*",
		Values: map[string]string{"uno": "dos"},
	}

	lenStreams := 16
	streams := make([]string, 0, lenStreams)
	for i := 0; i < lenStreams; i++ {
		streams = append(streams, strconv.Itoa(i))
	}
	for i := 0; i < lenStreams; i++ {
		streams = append(streams, "0")
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.XAdd(ctx, &args)

			err := client.XRead(ctx, &redis.XReadArgs{
				Streams: streams,
				Count:   1,
				Block:   time.Second,
			}).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

//------------------------------------------------------------------------------

func newClusterScenario() *clusterScenario {
	return &clusterScenario{
		ports:     []string{"16600", "16601", "16602", "16603", "16604", "16605"},
		nodeIDs:   make([]string, 6),
		processes: make(map[string]*redisProcess, 6),
		clients:   make(map[string]*redis.Client, 6),
	}
}

func BenchmarkClusterPing(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	cluster := newClusterScenario()
	if err := startCluster(ctx, cluster); err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()

	client := cluster.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.Ping(ctx).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClusterDoInt(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	cluster := newClusterScenario()
	if err := startCluster(ctx, cluster); err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()

	client := cluster.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.Do(ctx, "SET", 10, 10).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClusterSetString(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	cluster := newClusterScenario()
	if err := startCluster(ctx, cluster); err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()

	client := cluster.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	value := string(bytes.Repeat([]byte{'1'}, 10000))

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.Set(ctx, "key", value, 0).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExecRingSetAddrsCmd(b *testing.B) {
	const (
		ringShard1Name = "ringShardOne"
		ringShard2Name = "ringShardTwo"
	)

	for _, port := range []string{ringShard1Port, ringShard2Port} {
		if _, err := startRedis(port); err != nil {
			b.Fatal(err)
		}
	}

	b.Cleanup(func() {
		for _, p := range processes {
			if err := p.Close(); err != nil {
				b.Errorf("Failed to stop redis process: %v", err)
			}
		}
		processes = nil
	})

	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"ringShardOne": ":" + ringShard1Port,
		},
		NewClient: func(opt *redis.Options) *redis.Client {
			// Simulate slow shard creation
			time.Sleep(100 * time.Millisecond)
			return redis.NewClient(opt)
		},
	})
	defer ring.Close()

	if _, err := ring.Ping(context.Background()).Result(); err != nil {
		b.Fatal(err)
	}

	// Continuously update addresses by adding and removing one address
	updatesDone := make(chan struct{})
	defer func() { close(updatesDone) }()
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-ticker.C:
				if i%2 == 0 {
					ring.SetAddrs(map[string]string{
						ringShard1Name: ":" + ringShard1Port,
					})
				} else {
					ring.SetAddrs(map[string]string{
						ringShard1Name: ":" + ringShard1Port,
						ringShard2Name: ":" + ringShard2Port,
					})
				}
			case <-updatesDone:
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ring.Ping(context.Background()).Result(); err != nil {
			if err == redis.ErrClosed {
				// The shard client could be closed while ping command is in progress
				continue
			} else {
				b.Fatal(err)
			}
		}
	}
}
