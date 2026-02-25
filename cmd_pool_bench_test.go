package redis

import (
	"context"
	"testing"
	"time"
)

// Benchmark pooled vs non-pooled command implementations

func BenchmarkStringCommands(b *testing.B) {
	ctx := context.Background()
	client := NewClient(&Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Warm up the connection
	client.Set(ctx, "bench:warmup", "value", 0)

	b.Run("Get", func(b *testing.B) {
		client.Set(ctx, "bench:get", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.Get(ctx, "bench:get")
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.Set(ctx, "bench:set", "value", 0)
		}
	})

	b.Run("Incr", func(b *testing.B) {
		client.Del(ctx, "bench:incr")
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.Incr(ctx, "bench:incr")
		}
	})

	b.Run("Decr", func(b *testing.B) {
		client.Set(ctx, "bench:decr", "1000000", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.Decr(ctx, "bench:decr")
		}
	})

	b.Run("IncrBy", func(b *testing.B) {
		client.Del(ctx, "bench:incrby")
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.IncrBy(ctx, "bench:incrby", 10)
		}
	})

	b.Run("DecrBy", func(b *testing.B) {
		client.Set(ctx, "bench:decrby", "1000000", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.DecrBy(ctx, "bench:decrby", 10)
		}
	})

	b.Run("IncrByFloat", func(b *testing.B) {
		client.Del(ctx, "bench:incrbyfloat")
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.IncrByFloat(ctx, "bench:incrbyfloat", 1.5)
		}
	})

	b.Run("GetRange", func(b *testing.B) {
		client.Set(ctx, "bench:getrange", "Hello, World!", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.GetRange(ctx, "bench:getrange", 0, 4)
		}
	})

	b.Run("GetDel", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.Set(ctx, "bench:getdel", "value", 0)
			_, _ = client.GetDel(ctx, "bench:getdel")
		}
	})

	b.Run("GetEx", func(b *testing.B) {
		client.Set(ctx, "bench:getex", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.GetEx(ctx, "bench:getex", 10*time.Second)
		}
	})

	b.Run("MGet", func(b *testing.B) {
		client.MSet(ctx, "bench:mget1", "val1", "bench:mget2", "val2", "bench:mget3", "val3")
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.MGet(ctx, "bench:mget1", "bench:mget2", "bench:mget3")
		}
	})

	b.Run("MSet", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.MSet(ctx, "bench:mset1", "val1", "bench:mset2", "val2", "bench:mset3", "val3")
		}
	})

	b.Run("Digest", func(b *testing.B) {
		client.Set(ctx, "bench:digest", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = client.Digest(ctx, "bench:digest")
		}
	})
}

func BenchmarkCommandAllocation(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStringCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
			putStringCmd(cmd)
		}
	})

	b.Run("StringCmd_New", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = NewStringCmd(ctx, "get", "key")
		}
	})

	b.Run("IntCmd_Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getIntCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
			putIntCmd(cmd)
		}
	})

	b.Run("IntCmd_New", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = NewIntCmd(ctx, "incr", "key")
		}
	})

	b.Run("BoolCmd_Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getBoolCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"setnx", "key", "value"}}
			putBoolCmd(cmd)
		}
	})

	b.Run("BoolCmd_New", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = NewBoolCmd(ctx, "setnx", "key", "value")
		}
	})

	b.Run("StatusCmd_Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStatusCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
			putStatusCmd(cmd)
		}
	})

	b.Run("StatusCmd_New", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = NewStatusCmd(ctx, "set", "key", "value")
		}
	})

	b.Run("SliceCmd_Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getSliceCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"mget", "key1", "key2", "key3"}}
			putSliceCmd(cmd)
		}
	})

	b.Run("SliceCmd_New", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = NewSliceCmd(ctx, "mget", "key1", "key2", "key3")
		}
	})
}

func BenchmarkConcurrentCommands(b *testing.B) {
	ctx := context.Background()
	client := NewClient(&Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	b.Run("Get_Concurrent", func(b *testing.B) {
		client.Set(ctx, "bench:concurrent", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = client.Get(ctx, "bench:concurrent")
			}
		})
	})

	b.Run("Set_Concurrent", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = client.Set(ctx, "bench:concurrent:set", "value", 0)
			}
		})
	})

	b.Run("Incr_Concurrent", func(b *testing.B) {
		client.Del(ctx, "bench:concurrent:incr")
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = client.Incr(ctx, "bench:concurrent:incr")
			}
		})
	})

	b.Run("MGet_Concurrent", func(b *testing.B) {
		client.MSet(ctx, "bench:c1", "v1", "bench:c2", "v2", "bench:c3", "v3")
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = client.MGet(ctx, "bench:c1", "bench:c2", "bench:c3")
			}
		})
	})
}

func BenchmarkMemoryPressure(b *testing.B) {
	ctx := context.Background()
	client := NewClient(&Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Simulate high-throughput scenario
	b.Run("HighThroughput_1000ops", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				_, _ = client.Get(ctx, "bench:throughput")
				_, _ = client.Set(ctx, "bench:throughput", "value", 0)
				_, _ = client.Incr(ctx, "bench:counter")
			}
		}
	})
}

