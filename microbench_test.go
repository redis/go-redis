package redis

import (
	"context"
	"testing"
)

// Micro-benchmarks that test the pooling mechanism directly
// These work on the refactored branch only

func BenchmarkStringCmdPooling(b *testing.B) {
	ctx := context.Background()

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStringCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
			cmd.val = "value"
			_ = cmd.Val()
			putStringCmd(cmd)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewStringCmd(ctx, "get", "key")
			cmd.val = "value"
			_ = cmd.Val()
		}
	})
}

func BenchmarkIntCmdPooling(b *testing.B) {
	ctx := context.Background()

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getIntCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
			cmd.val = 42
			_ = cmd.Val()
			putIntCmd(cmd)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewIntCmd(ctx, "incr", "key")
			cmd.val = 42
			_ = cmd.Val()
		}
	})
}

func BenchmarkStatusCmdPooling(b *testing.B) {
	ctx := context.Background()

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := getStatusCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
			cmd.val = "OK"
			_ = cmd.Val()
			putStatusCmd(cmd)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmd := NewStatusCmd(ctx, "set", "key", "value")
			cmd.val = "OK"
			_ = cmd.Val()
		}
	})
}

func BenchmarkMixedWorkload(b *testing.B) {
	ctx := context.Background()

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate mixed workload
			for j := 0; j < 100; j++ {
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

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate mixed workload
			for j := 0; j < 100; j++ {
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

func BenchmarkConcurrentPooling(b *testing.B) {
	ctx := context.Background()

	b.Run("WithPool", func(b *testing.B) {
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

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cmd := NewStringCmd(ctx, "get", "key")
				cmd.val = "value"
				_ = cmd.Val()
			}
		})
	})
}

