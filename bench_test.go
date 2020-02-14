package redis_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
)

func benchmarkRedisClient(poolSize int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
	})
	if err := client.FlushDB().Err(); err != nil {
		panic(err)
	}
	return client
}

func BenchmarkRedisPing(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Ping().Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisGetNil(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Get("key").Err(); err != redis.Nil {
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
			client := benchmarkRedisClient(bm.poolSize)
			defer client.Close()

			value := strings.Repeat("1", bm.valueSize)

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := client.Set("key", value, 0).Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisSetGetBytes(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	value := bytes.Repeat([]byte{'1'}, 10000)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}

			got, err := client.Get("key").Bytes()
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
	client := benchmarkRedisClient(10)
	defer client.Close()

	if err := client.MSet("key1", "hello1", "key2", "hello2").Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.MGet("key1", "key2").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetExpire(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", "hello", 0).Err(); err != nil {
				b.Fatal(err)
			}
			if err := client.Expire("key", time.Second).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPipeline(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.Set("key", "hello", 0)
				pipe.Expire("key", time.Second)
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkZAdd(b *testing.B) {
	client := benchmarkRedisClient(10)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.ZAdd("key", &redis.Z{
				Score:  float64(1),
				Member: "hello",
			}).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

var clientSink *redis.Client

func BenchmarkWithContext(b *testing.B) {
	rdb := benchmarkRedisClient(10)
	defer rdb.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		clientSink = rdb.WithContext(ctx)
	}
}

var ringSink *redis.Ring

func BenchmarkRingWithContext(b *testing.B) {
	rdb := redis.NewRing(&redis.RingOptions{})
	defer rdb.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ringSink = rdb.WithContext(ctx)
	}
}

//------------------------------------------------------------------------------

func newClusterScenario() *clusterScenario {
	return &clusterScenario{
		ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
		nodeIDs:   make([]string, 6),
		processes: make(map[string]*redisProcess, 6),
		clients:   make(map[string]*redis.Client, 6),
	}
}

func BenchmarkClusterPing(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	cluster := newClusterScenario()
	if err := startCluster(cluster); err != nil {
		b.Fatal(err)
	}
	defer stopCluster(cluster)

	client := cluster.newClusterClient(redisClusterOptions())
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.Ping().Err()
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

	cluster := newClusterScenario()
	if err := startCluster(cluster); err != nil {
		b.Fatal(err)
	}
	defer stopCluster(cluster)

	client := cluster.newClusterClient(redisClusterOptions())
	defer client.Close()

	value := string(bytes.Repeat([]byte{'1'}, 10000))

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := client.Set("key", value, 0).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClusterReloadState(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	cluster := newClusterScenario()
	if err := startCluster(cluster); err != nil {
		b.Fatal(err)
	}
	defer stopCluster(cluster)

	client := cluster.newClusterClient(redisClusterOptions())
	defer client.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.ReloadState()
		if err != nil {
			b.Fatal(err)
		}
	}
}

var clusterSink *redis.ClusterClient

func BenchmarkClusterWithContext(b *testing.B) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{})
	defer rdb.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		clusterSink = rdb.WithContext(ctx)
	}
}
