package redis_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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

// benchmarkRedisGet runs the classic Get-into-string path so it can be
// compared head-to-head with benchmarkRedisGetToBuffer for the same value
// size. The key is populated once up front and read in a parallel loop.
func benchmarkRedisGet(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'x'}, size)
	if err := client.Set(ctx, "key", value, 0).Err(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, err := client.Get(ctx, "key").Bytes()
			if err != nil {
				b.Fatal(err)
			}
			if len(got) != size {
				b.Fatalf("got len %d, want %d", len(got), size)
			}
		}
	})
}

// benchmarkRedisGetToBuffer reads the value into a per-goroutine reusable
// buffer via GetToBuffer, exercising the zero-copy receive path.
func benchmarkRedisGetToBuffer(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'x'}, size)
	if err := client.Set(ctx, "key", value, 0).Err(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, size)
		for pb.Next() {
			n, err := client.GetToBuffer(ctx, "key", buf).Result()
			if err != nil {
				b.Fatal(err)
			}
			if n != size {
				b.Fatalf("n = %d, want %d", n, size)
			}
		}
	})
}

// benchmarkRedisSetGetBuffer round-trips a payload through SetFromBuffer
// and GetToBuffer, exercising both zero-copy paths on a shared buffer.
func benchmarkRedisSetGetBuffer(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'x'}, size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, size)
		for pb.Next() {
			if err := client.SetFromBuffer(ctx, "key", value).Err(); err != nil {
				b.Fatal(err)
			}
			n, err := client.GetToBuffer(ctx, "key", buf).Result()
			if err != nil {
				b.Fatal(err)
			}
			if n != size {
				b.Fatalf("n = %d, want %d", n, size)
			}
		}
	})
}

// benchmarkRedisSet runs Set with a []byte value, the closest existing
// peer of SetFromBuffer for direct comparison.
func benchmarkRedisSet(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'x'}, size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set(ctx, "key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchmarkRedisSetFromBuffer is the zero-copy counterpart of benchmarkRedisSet.
func benchmarkRedisSetFromBuffer(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'x'}, size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.SetFromBuffer(ctx, "key", value).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchmarkRedisGetRawSocket bypasses go-redis entirely. It opens a raw TCP
// connection per goroutine, sends GET key directly, and reads the bulk-string
// reply into a pre-allocated buffer via io.ReadFull on the net.Conn (with a
// small bufio.Reader used only to parse the header line). It is the floor for
// what a Go client can extract from this network path — no command struct,
// no hooks, no pool. If GetToBuffer is close to this number, our code path
// is already near-optimal.
func benchmarkRedisGetRawSocket(b *testing.B, size int) {
	ctx := context.Background()
	client := benchmarkRedisClient(ctx, 1)
	defer client.Close()
	if err := client.Set(ctx, "key", bytes.Repeat([]byte{'x'}, size), 0).Err(); err != nil {
		b.Fatal(err)
	}

	req := []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		rd := bufio.NewReaderSize(conn, 32*1024)
		buf := make([]byte, size)

		for pb.Next() {
			if _, err := conn.Write(req); err != nil {
				b.Fatal(err)
			}
			// Header: $<len>\r\n
			hdr, err := rd.ReadSlice('\n')
			if err != nil {
				b.Fatal(err)
			}
			if hdr[0] != '$' {
				b.Fatalf("unexpected reply prefix %q", hdr)
			}
			n, err := strconv.Atoi(string(hdr[1 : len(hdr)-2]))
			if err != nil || n != size {
				b.Fatalf("bad header %q (n=%d, err=%v)", hdr, n, err)
			}
			// Payload + trailing \r\n.
			if _, err := io.ReadFull(rd, buf[:n]); err != nil {
				b.Fatal(err)
			}
			var crlf [2]byte
			if _, err := io.ReadFull(rd, crlf[:]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// CI-friendly benchmark wrappers. Sizes are capped at 64 KiB so the matrix
// (5 Redis versions × 3 Go versions, full docker-compose stack on a 7 GiB
// runner) stays well under the test-binary timeout and runner memory.
// Larger payload behaviour is covered by integration tests
// ("should SetFromBuffer and GetToBuffer with 10 MiB payload") and the
// detailed report in docs/zero-copy-buffer-benchmarks.md. To reproduce
// the 1 MiB / larger numbers locally, invoke the benchmark* helpers
// directly with the size you want.
func BenchmarkRedisGetRawSocket_4KiB(b *testing.B)   { benchmarkRedisGetRawSocket(b, 4*1024) }
func BenchmarkRedisGetRawSocket_64KiB(b *testing.B)  { benchmarkRedisGetRawSocket(b, 64*1024) }
func BenchmarkRedisGet_4KiB(b *testing.B)            { benchmarkRedisGet(b, 4*1024) }
func BenchmarkRedisGet_64KiB(b *testing.B)           { benchmarkRedisGet(b, 64*1024) }
func BenchmarkRedisGetToBuffer_4KiB(b *testing.B)    { benchmarkRedisGetToBuffer(b, 4*1024) }
func BenchmarkRedisGetToBuffer_64KiB(b *testing.B)   { benchmarkRedisGetToBuffer(b, 64*1024) }
func BenchmarkRedisSet_4KiB(b *testing.B)            { benchmarkRedisSet(b, 4*1024) }
func BenchmarkRedisSet_64KiB(b *testing.B)           { benchmarkRedisSet(b, 64*1024) }
func BenchmarkRedisSetFromBuffer_4KiB(b *testing.B)  { benchmarkRedisSetFromBuffer(b, 4*1024) }
func BenchmarkRedisSetFromBuffer_64KiB(b *testing.B) { benchmarkRedisSetFromBuffer(b, 64*1024) }
func BenchmarkRedisSetGetBuffer_4KiB(b *testing.B)   { benchmarkRedisSetGetBuffer(b, 4*1024) }
func BenchmarkRedisSetGetBuffer_64KiB(b *testing.B)  { benchmarkRedisSetGetBuffer(b, 64*1024) }

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
		ports:   []string{"16600", "16601", "16602", "16603", "16604", "16605"},
		nodeIDs: make([]string, 6),
		clients: make(map[string]*redis.Client, 6),
	}
}

var clusterBench *clusterScenario

func BenchmarkClusterPing(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	if clusterBench == nil {
		clusterBench = newClusterScenario()
		if err := configureClusterTopology(ctx, clusterBench); err != nil {
			b.Fatal(err)
		}
	}

	client := clusterBench.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	b.Run("cluster ping", func(b *testing.B) {
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := client.Ping(ctx).Err()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkClusterDoInt(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	if clusterBench == nil {
		clusterBench = newClusterScenario()
		if err := configureClusterTopology(ctx, clusterBench); err != nil {
			b.Fatal(err)
		}
	}

	client := clusterBench.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	b.Run("cluster do set int", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := client.Do(ctx, "SET", 10, 10).Err()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkClusterSetString(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	ctx := context.Background()
	if clusterBench == nil {
		clusterBench = newClusterScenario()
		if err := configureClusterTopology(ctx, clusterBench); err != nil {
			b.Fatal(err)
		}
	}

	client := clusterBench.newClusterClient(ctx, redisClusterOptions())
	defer client.Close()

	value := string(bytes.Repeat([]byte{'1'}, 10000))

	b.Run("cluster set string", func(b *testing.B) {
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

func BenchmarkExecRingSetAddrsCmd(b *testing.B) {
	const (
		ringShard1Name = "ringShardOne"
		ringShard2Name = "ringShardTwo"
	)

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
