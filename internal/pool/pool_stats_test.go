package pool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestStatsExcludesPendingIdleDials(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client, server := net.Pipe()
	p := NewConnPool(&Options{
		Dialer: func(context.Context) (net.Conn, error) {
			close(started)
			<-release
			return client, nil
		},
		PoolSize:           1,
		MinIdleConns:       1,
		MaxConcurrentDials: 1,
	})
	t.Cleanup(func() {
		close(release)
		_ = p.Close()
		_ = client.Close()
		_ = server.Close()
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("idle dial did not start")
	}
	if reserved := p.idleConnsLen.Load(); reserved != 1 {
		t.Fatalf("reserved idle connections = %d, want 1", reserved)
	}
	stats := p.Stats()
	if stats.TotalConns != 0 || stats.IdleConns != 0 {
		t.Fatalf("pending dial reported as an open connection: total=%d idle=%d",
			stats.TotalConns, stats.IdleConns)
	}
}

func TestStatsConcurrentSnapshot(t *testing.T) {
	p := &ConnPool{conns: make(map[uint64]*Conn)}
	cn := new(Conn)
	started := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		for {
			select {
			case <-done:
				return
			default:
			}
			// Both stable states contain only idle connections.
			p.connsMu.Lock()
			p.conns[1] = cn
			p.idleConns = append(p.idleConns, cn)
			p.idleConnsLen.Store(1)
			p.connsMu.Unlock()

			p.connsMu.Lock()
			delete(p.conns, 1)
			p.idleConns = p.idleConns[:0]
			p.idleConnsLen.Store(0)
			p.connsMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		close(done)
		wg.Wait()
	})
	<-started

	for i := 0; i < 10000; i++ {
		stats := p.Stats()
		if stats.TotalConns != stats.IdleConns {
			t.Fatalf("inconsistent snapshot: total=%d idle=%d", stats.TotalConns, stats.IdleConns)
		}
	}
}
