package redis

import (
	"context"
	"testing"
)

// Standalone benchmarks that don't require Redis connection
// These test the pooling mechanism directly

func BenchmarkCmdPooling(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStringCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
			cmd.val = "value"
			_ = cmd.Val()
			putStringCmd(cmd)
		}
	})

	b.Run("StringCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewStringCmd(ctx, "get", "key")
			cmd.val = "value"
			_ = cmd.Val()
		}
	})

	b.Run("IntCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getIntCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
			cmd.val = 42
			_ = cmd.Val()
			putIntCmd(cmd)
		}
	})

	b.Run("IntCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewIntCmd(ctx, "incr", "key")
			cmd.val = 42
			_ = cmd.Val()
		}
	})

	b.Run("BoolCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getBoolCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"setnx", "key", "value"}}
			cmd.val = true
			_ = cmd.Val()
			putBoolCmd(cmd)
		}
	})

	b.Run("BoolCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewBoolCmd(ctx, "setnx", "key", "value")
			cmd.val = true
			_ = cmd.Val()
		}
	})

	b.Run("StatusCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStatusCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
			cmd.val = "OK"
			_ = cmd.Val()
			putStatusCmd(cmd)
		}
	})

	b.Run("StatusCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewStatusCmd(ctx, "set", "key", "value")
			cmd.val = "OK"
			_ = cmd.Val()
		}
	})

	b.Run("FloatCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getFloatCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incrbyfloat", "key", "1.5"}}
			cmd.val = 3.14
			_ = cmd.Val()
			putFloatCmd(cmd)
		}
	})

	b.Run("FloatCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewFloatCmd(ctx, "incrbyfloat", "key", "1.5")
			cmd.val = 3.14
			_ = cmd.Val()
		}
	})

	b.Run("SliceCmd_WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getSliceCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"mget", "key1", "key2", "key3"}}
			cmd.val = []interface{}{"val1", "val2", "val3"}
			_ = cmd.Val()
			putSliceCmd(cmd)
		}
	})

	b.Run("SliceCmd_WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewSliceCmd(ctx, "mget", "key1", "key2", "key3")
			cmd.val = []interface{}{"val1", "val2", "val3"}
			_ = cmd.Val()
		}
	})
}

func BenchmarkCmdPooling_Parallel(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_WithPool_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cmd := getStringCmd()
				cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
				cmd.val = "value"
				_ = cmd.Val()
				putStringCmd(cmd)
			}
		})
	})

	b.Run("StringCmd_WithoutPool_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cmd := NewStringCmd(ctx, "get", "key")
				cmd.val = "value"
				_ = cmd.Val()
			}
		})
	})

	b.Run("IntCmd_WithPool_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cmd := getIntCmd()
				cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
				cmd.val = 42
				_ = cmd.Val()
				putIntCmd(cmd)
			}
		})
	})

	b.Run("IntCmd_WithoutPool_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cmd := NewIntCmd(ctx, "incr", "key")
				cmd.val = 42
				_ = cmd.Val()
			}
		})
	})
}

func BenchmarkCmdPooling_HighThroughput(b *testing.B) {
	ctx := context.Background()

	b.Run("Mixed_WithPool_1000ops", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				// Simulate mixed workload
				if j%3 == 0 {
					cmd := getStringCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
					cmd.val = "value"
					_ = cmd.Val()
					putStringCmd(cmd)
				} else if j%3 == 1 {
					cmd := getIntCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
					cmd.val = 42
					_ = cmd.Val()
					putIntCmd(cmd)
				} else {
					cmd := getStatusCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
					cmd.val = "OK"
					_ = cmd.Val()
					putStatusCmd(cmd)
				}
			}
		}
	})

	b.Run("Mixed_WithoutPool_1000ops", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				// Simulate mixed workload
				if j%3 == 0 {
					cmd := NewStringCmd(ctx, "get", "key")
					cmd.val = "value"
					_ = cmd.Val()
				} else if j%3 == 1 {
					cmd := NewIntCmd(ctx, "incr", "key")
					cmd.val = 42
					_ = cmd.Val()
				} else {
					cmd := NewStatusCmd(ctx, "set", "key", "value")
					cmd.val = "OK"
					_ = cmd.Val()
				}
			}
		}
	})
}

