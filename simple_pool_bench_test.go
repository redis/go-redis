package redis_test

import (
	"context"
	"testing"

	redis "github.com/redis/go-redis/v9"
)

// Simple benchmark comparing pooled vs non-pooled Cmd creation
// This works on the refactored branch

var benchSink interface{}

func BenchmarkPooledVsNonPooled(b *testing.B) {
	ctx := context.Background()

	b.Run("StringCmd_NonPooled", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := redis.NewStringCmd(ctx, "get", "key")
			cmd.SetVal("value")
			result = cmd.Val()
		}
		benchSink = result
	})

	b.Run("IntCmd_NonPooled", func(b *testing.B) {
		b.ReportAllocs()
		var result int64
		for i := 0; i < b.N; i++ {
			cmd := redis.NewIntCmd(ctx, "incr", "key")
			cmd.SetVal(42)
			result = cmd.Val()
		}
		benchSink = result
	})

	b.Run("StatusCmd_NonPooled", func(b *testing.B) {
		b.ReportAllocs()
		var result string
		for i := 0; i < b.N; i++ {
			cmd := redis.NewStatusCmd(ctx, "set", "key", "value")
			cmd.SetVal("OK")
			result = cmd.Val()
		}
		benchSink = result
	})

	b.Run("Mixed100ops_NonPooled", func(b *testing.B) {
		b.ReportAllocs()
		var s string
		var intVal int64
		var st string
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				if j%3 == 0 {
					cmd := redis.NewStringCmd(ctx, "get", "key")
					cmd.SetVal("value")
					s = cmd.Val()
				} else if j%3 == 1 {
					cmd := redis.NewIntCmd(ctx, "incr", "key")
					cmd.SetVal(42)
					intVal = cmd.Val()
				} else {
					cmd := redis.NewStatusCmd(ctx, "set", "key", "value")
					cmd.SetVal("OK")
					st = cmd.Val()
				}
			}
		}
		benchSink = s
		benchSink = intVal
		benchSink = st
	})
}

