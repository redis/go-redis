package redis

import (
	"context"
	"testing"
)

// Benchmark Cmd object allocation with pooling (refactored branch)
// This measures the improvement from using sync.Pool

var sinkPooled interface{}

func BenchmarkCmdAllocationPooled(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := getStringCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
			cmd.val = "value"
			result = cmd.Val()
			putStringCmd(cmd)
		}
		sinkPooled = result
	})

	b.Run("IntCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result int64
		for i := 0; i < b.N; i++ {
			cmd := getIntCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
			cmd.val = 42
			result = cmd.Val()
			putIntCmd(cmd)
		}
		sinkPooled = result
	})

	b.Run("StatusCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := getStatusCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
			cmd.val = "OK"
			result = cmd.Val()
			putStatusCmd(cmd)
		}
		sinkPooled = result
	})

	b.Run("BoolCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result bool
		for i := 0; i < b.N; i++ {
			cmd := getBoolCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"setnx", "key", "value"}}
			cmd.val = true
			result = cmd.Val()
			putBoolCmd(cmd)
		}
		sinkPooled = result
	})

	b.Run("FloatCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result float64
		for i := 0; i < b.N; i++ {
			cmd := getFloatCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incrbyfloat", "key", "1.5"}}
			cmd.val = 3.14
			result = cmd.Val()
			putFloatCmd(cmd)
		}
		sinkPooled = result
	})

	b.Run("SliceCmd", func(b *testing.B) {
		b.ReportAllocs()
		var result []interface{}
		for i := 0; i < b.N; i++ {
			cmd := getSliceCmd()
			cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"mget", "key1", "key2", "key3"}}
			cmd.val = []interface{}{"val1", "val2", "val3"}
			result = cmd.Val()
			putSliceCmd(cmd)
		}
		sinkPooled = result
	})
}

func BenchmarkCmdAllocationPooled_Mixed(b *testing.B) {
	ctx := context.Background()

	b.Run("Mixed100ops", func(b *testing.B) {
		b.ReportAllocs()
		var s string
		var intVal int64
		var st string
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				if j%3 == 0 {
					cmd := getStringCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
					cmd.val = "value"
					s = cmd.Val()
					putStringCmd(cmd)
				} else if j%3 == 1 {
					cmd := getIntCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
					cmd.val = 42
					intVal = cmd.Val()
					putIntCmd(cmd)
				} else {
					cmd := getStatusCmd()
					cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"set", "key", "value"}}
					cmd.val = "OK"
					st = cmd.Val()
					putStatusCmd(cmd)
				}
			}
		}
		sinkPooled = s
		sinkPooled = intVal
		sinkPooled = st
	})
}

func BenchmarkCmdAllocationPooled_Parallel(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var result string
			for pb.Next() {
				cmd := getStringCmd()
				cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", "key"}}
				cmd.val = "value"
				result = cmd.Val()
				putStringCmd(cmd)
			}
			sinkPooled = result
		})
	})

	b.Run("IntCmd_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var result int64
			for pb.Next() {
				cmd := getIntCmd()
				cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"incr", "key"}}
				cmd.val = 42
				result = cmd.Val()
				putIntCmd(cmd)
			}
			sinkPooled = result
		})
	})
}

