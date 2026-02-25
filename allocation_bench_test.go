package redis

import (
	"context"
	"testing"
)

// Benchmark Cmd object allocation on master branch (baseline)
// This measures the cost of creating Cmd objects without pooling

var sink interface{}

func BenchmarkCmdAllocation(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := NewStringCmd(ctx, "get", "key")
			cmd.val = "value"
			result = cmd.Val()
		}
		sink = result
	})

	b.Run("IntCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result int64
		for i := 0; i < b.N; i++ {
			cmd := NewIntCmd(ctx, "incr", "key")
			cmd.val = 42
			result = cmd.Val()
		}
		sink = result
	})

	b.Run("StatusCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := NewStatusCmd(ctx, "set", "key", "value")
			cmd.val = "OK"
			result = cmd.Val()
		}
		sink = result
	})

	b.Run("BoolCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result bool
		for i := 0; i < b.N; i++ {
			cmd := NewBoolCmd(ctx, "setnx", "key", "value")
			cmd.val = true
			result = cmd.Val()
		}
		sink = result
	})

	b.Run("FloatCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result float64
		for i := 0; i < b.N; i++ {
			cmd := NewFloatCmd(ctx, "incrbyfloat", "key", "1.5")
			cmd.val = 3.14
			result = cmd.Val()
		}
		sink = result
	})

	b.Run("SliceCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result []interface{}
		for i := 0; i < b.N; i++ {
			cmd := NewSliceCmd(ctx, "mget", "key1", "key2", "key3")
			cmd.val = []interface{}{"val1", "val2", "val3"}
			result = cmd.Val()
		}
		sink = result
	})
}

func BenchmarkCmdAllocation_Mixed(b *testing.B) {
	ctx := context.Background()

	b.Run("Mixed100ops", func(b *testing.B) {
		b.ReportAllocs()
		var s string
		var intVal int64
		var st string
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				if j%3 == 0 {
					cmd := NewStringCmd(ctx, "get", "key")
					cmd.val = "value"
					s = cmd.Val()
				} else if j%3 == 1 {
					cmd := NewIntCmd(ctx, "incr", "key")
					cmd.val = 42
					intVal = cmd.Val()
				} else {
					cmd := NewStatusCmd(ctx, "set", "key", "value")
					cmd.val = "OK"
					st = cmd.Val()
				}
			}
		}
		sink = s
		sink = intVal
		sink = st
	})
}

func BenchmarkCmdAllocation_Parallel(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var result string
			for pb.Next() {
				cmd := NewStringCmd(ctx, "get", "key")
				cmd.val = "value"
				result = cmd.Val()
			}
			sink = result
		})
	})

	b.Run("IntCmd_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var result int64
			for pb.Next() {
				cmd := NewIntCmd(ctx, "incr", "key")
				cmd.val = 42
				result = cmd.Val()
			}
			sink = result
		})
	})
}

