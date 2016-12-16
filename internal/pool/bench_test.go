package pool_test

import (
	"errors"
	"testing"
	"time"

	"gopkg.in/redis.v5/internal/pool"
)

func benchmarkPoolGetPut(b *testing.B, poolSize int) {
	connPool := pool.NewConnPool(dummyDialer, poolSize, time.Second, time.Hour, time.Hour)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, _, err := connPool.Get()
			if err != nil {
				b.Fatal(err)
			}
			if err = connPool.Put(cn); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPoolGetPut10Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 10)
}

func BenchmarkPoolGetPut100Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 100)
}

func BenchmarkPoolGetPut1000Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 1000)
}

func benchmarkPoolGetRemove(b *testing.B, poolSize int) {
	connPool := pool.NewConnPool(dummyDialer, poolSize, time.Second, time.Hour, time.Hour)
	removeReason := errors.New("benchmark")

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, _, err := connPool.Get()
			if err != nil {
				b.Fatal(err)
			}
			if err := connPool.Remove(cn, removeReason); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPoolGetRemove10Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 10)
}

func BenchmarkPoolGetRemove100Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 100)
}

func BenchmarkPoolGetRemove1000Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 1000)
}
