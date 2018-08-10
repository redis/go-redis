package pool_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/internal/pool"
)

type poolGetPutBenchmark struct {
	poolSize int
}

func (bm poolGetPutBenchmark) String() string {
	return fmt.Sprintf("pool=%d", bm.poolSize)
}

func BenchmarkPoolGetPut(b *testing.B) {
	benchmarks := []poolGetPutBenchmark{
		{1},
		{2},
		{8},
		{32},
		{64},
		{128},
	}
	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			connPool := pool.NewConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           bm.poolSize,
				PoolTimeout:        time.Second,
				IdleTimeout:        time.Hour,
				IdleCheckFrequency: time.Hour,
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get()
					if err != nil {
						b.Fatal(err)
					}
					connPool.Put(cn)
				}
			})
		})
	}
}

type poolGetRemoveBenchmark struct {
	poolSize int
}

func (bm poolGetRemoveBenchmark) String() string {
	return fmt.Sprintf("pool=%d", bm.poolSize)
}

func BenchmarkPoolGetRemove(b *testing.B) {
	benchmarks := []poolGetRemoveBenchmark{
		{1},
		{2},
		{8},
		{32},
		{64},
		{128},
	}
	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			connPool := pool.NewConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           bm.poolSize,
				PoolTimeout:        time.Second,
				IdleTimeout:        time.Hour,
				IdleCheckFrequency: time.Hour,
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get()
					if err != nil {
						b.Fatal(err)
					}
					connPool.Remove(cn)
				}
			})
		})
	}
}
