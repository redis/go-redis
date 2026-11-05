package pool

import (
	"context"
	"net"
	"sync"
	"testing"
)

func TestMinIdleCheckConcurrentRaise(t *testing.T) {
	for i := 0; i < 10000; i++ {
		p := NewConnPool(&Options{
			PoolSize: 1, MaxConcurrentDials: 1,
			Dialer: func(context.Context) (net.Conn, error) {
				client, server := net.Pipe()
				_ = server.Close()
				return client, nil
			},
		})
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			p.checkMinIdleConns()
		}()
		go func() {
			defer wg.Done()
			<-start
			p.RaiseMinIdleConns(1)
		}()
		close(start)
		wg.Wait()
		// A refill reserves poolSize synchronously before starting its dial.
		// With no dial/turn contention, the raised floor must be scheduled by
		// one of these callers, without needing another pool operation.
		scheduled := p.poolSize.Load()
		_ = p.Close()
		if scheduled != 1 {
			t.Fatalf("iteration %d: concurrent raise lost its refill request", i)
		}
	}
}
