package pool_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"gopkg.in/redis.v3/internal/pool"
)

func benchmarkPoolGetPut(b *testing.B, poolSize int) {
	dial := func() (net.Conn, error) {
		return &net.TCPConn{}, nil
	}
	pool := pool.NewConnPool(dial, poolSize, time.Second, 0)
	pool.DialLimiter = nil

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get()
			if err != nil {
				b.Fatalf("no error expected on pool.Get but received: %s", err.Error())
			}
			if err = pool.Put(conn); err != nil {
				b.Fatalf("no error expected on pool.Put but received: %s", err.Error())
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

func benchmarkPoolGetReplace(b *testing.B, poolSize int) {
	dial := func() (net.Conn, error) {
		return &net.TCPConn{}, nil
	}
	pool := pool.NewConnPool(dial, poolSize, time.Second, 0)
	pool.DialLimiter = nil

	removeReason := errors.New("benchmark")

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get()
			if err != nil {
				b.Fatalf("no error expected on pool.Get but received: %s", err.Error())
			}
			if err = pool.Replace(conn, removeReason); err != nil {
				b.Fatalf("no error expected on pool.Remove but received: %s", err.Error())
			}
		}
	})
}

func BenchmarkPoolGetReplace10Conns(b *testing.B) {
	benchmarkPoolGetReplace(b, 10)
}

func BenchmarkPoolGetReplace100Conns(b *testing.B) {
	benchmarkPoolGetReplace(b, 100)
}

func BenchmarkPoolGetReplace1000Conns(b *testing.B) {
	benchmarkPoolGetReplace(b, 1000)
}
