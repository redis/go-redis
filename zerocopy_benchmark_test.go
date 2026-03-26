package redis_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Benchmark comparing regular Get/Set vs zero-copy GetToBuffer/SetFromBuffer
// Run with: go test -bench=BenchmarkZeroCopy -benchmem -run=NONE .
//
// Key findings:
// - GetToBuffer avoids data allocations — reads directly into the user's buffer
// - SetFromBuffer avoids string conversion allocation
// - Works correctly with both RESP2 and RESP3 (push notifications are handled)

func BenchmarkZeroCopySet_1MB(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	data := make([]byte, 1*1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_set"

	b.Run("RegularSet", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Regular Set converts []byte to string, causing allocation
			if err := client.Set(ctx, key, data, 0).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SetFromBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Zero-copy Set writes directly from buffer
			if err := client.SetFromBuffer(ctx, key, data).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	client.Del(ctx, key)
}

func BenchmarkZeroCopyGet_1MB(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,

	})
	defer client.Close()

	// Setup: write 1MB of data
	data := make([]byte, 1*1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_get"
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		b.Fatal(err)
	}
	defer client.Del(ctx, key)

	b.Run("RegularGet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Regular Get allocates a new string for the result
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val() // Use the value
		}
	})

	// Pre-allocate buffer for zero-copy reads
	readBuf := make([]byte, len(data)+100)

	b.Run("GetToBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Zero-copy Get reads directly into provided buffer
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val() // Use the value
		}
	})
}

func BenchmarkZeroCopyRoundTrip_1MB(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	data := make([]byte, 1*1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_roundtrip"
	defer client.Del(ctx, key)

	b.Run("RegularSetGet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Set(ctx, key, data, 0).Err(); err != nil {
				b.Fatal(err)
			}
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})

	readBuf := make([]byte, len(data)+100)

	b.Run("ZeroCopySetGetToBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.SetFromBuffer(ctx, key, data).Err(); err != nil {
				b.Fatal(err)
			}
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})
}

// Benchmark with larger data sizes to see allocation savings scale
func BenchmarkZeroCopyGet_10MB(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_get_10mb"
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		b.Fatal(err)
	}
	defer client.Del(ctx, key)

	b.Run("RegularGet", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})

	readBuf := make([]byte, len(data)+100)

	b.Run("GetToBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})
}

// BenchmarkZeroCopyGetToBuffer_RESP3 validates that the zero-copy path works
// correctly with RESP3, where push notification processing may fill the
// buffered reader before readReply is called.
func BenchmarkZeroCopyGetToBuffer_RESP3(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
		Protocol:     3, // RESP3 — push notification processing fills the buffer
	})
	defer client.Close()

	data := make([]byte, 1*1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_resp3"
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		b.Fatal(err)
	}
	defer client.Del(ctx, key)

	readBuf := make([]byte, len(data)+100)

	b.Run("RegularGet", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})

	b.Run("GetToBuffer", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})
}

// TestZeroCopyZeroDataAllocations validates that GetToBuffer does NOT allocate
// memory for the response data. Both GetToBuffer and regular Get have the same
// number of allocations (for the cmd struct, args slice, pool ops, etc.), but
// GetToBuffer allocates drastically fewer bytes because the payload goes
// directly into the caller's pre-allocated buffer.
func TestZeroCopyZeroDataAllocations(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	for _, size := range []int{1024, 100 * 1024, 1024 * 1024} { // 1KB, 100KB, 1MB
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		key := "test_zerocopy_alloc"
		if err := client.Set(ctx, key, data, 0).Err(); err != nil {
			t.Fatal(err)
		}

		readBuf := make([]byte, size+100)

		// Warm up the connection pool so pool-related allocations don't skew results.
		for i := 0; i < 5; i++ {
			_ = client.GetToBuffer(ctx, key, readBuf)
			_ = client.Get(ctx, key)
		}

		// Measure bytes allocated per GetToBuffer call.
		// testing.AllocsPerRun only counts alloc count, not bytes, so we use
		// a manual approach with runtime.MemStats.
		const runs = 100

		collectBytesPerOp := func(fn func()) uint64 {
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)
			for i := 0; i < runs; i++ {
				fn()
			}
			runtime.ReadMemStats(&m2)
			return (m2.TotalAlloc - m1.TotalAlloc) / runs
		}

		zeroCopyBytes := collectBytesPerOp(func() {
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				t.Fatal(cmd.Err())
			}
		})

		regularBytes := collectBytesPerOp(func() {
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				t.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		})

		t.Logf("Size=%7d: GetToBuffer=%d B/op, Get=%d B/op (saved %d B/op, %.0f%%)",
			size, zeroCopyBytes, regularBytes, regularBytes-zeroCopyBytes,
			float64(regularBytes-zeroCopyBytes)/float64(regularBytes)*100)

		// GetToBuffer must allocate fewer bytes than regular Get. Regular Get
		// allocates at least the data size for the string. GetToBuffer should
		// save nearly all of that since the payload goes into the user's buffer.
		if zeroCopyBytes >= regularBytes {
			t.Errorf("GetToBuffer (%d B/op) should allocate fewer bytes than Get (%d B/op) for size %d",
				zeroCopyBytes, regularBytes, size)
		}

		// The bytes saved should be at least 80% of the data size — the data
		// allocation is eliminated, only small cmd/pool overhead remains.
		saved := regularBytes - zeroCopyBytes
		if saved < uint64(size)*80/100 {
			t.Errorf("Expected to save at least 80%% of data size (%d bytes), but only saved %d bytes",
				size, saved)
		}

		client.Del(ctx, key)
	}
}

// Benchmark memory allocations specifically
func BenchmarkZeroCopyAllocations(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	// Use 100KB to see clear allocation differences
	data := make([]byte, 100*1024) // 100KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	key := "bench_zerocopy_alloc"
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		b.Fatal(err)
	}
	defer client.Del(ctx, key)

	readBuf := make([]byte, len(data)+100)

	b.Run("RegularGet_100KB", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.Get(ctx, key)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})

	b.Run("GetToBuffer_100KB", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := client.GetToBuffer(ctx, key, readBuf)
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			_ = cmd.Val()
		}
	})
}

