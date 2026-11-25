package pool

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// BenchmarkReapTakeAllExpired measures the connsMu-held section of a reaper
// tick when every idle conn has expired (the post-spike shrink). Pool state is
// restored outside the timed region.
func BenchmarkReapTakeAllExpired(b *testing.B) {
	for _, n := range []int{10, 100, 500, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := NewConnPool(&Options{
				PoolSize:           int32(n),
				MaxConcurrentDials: 64,
				DialTimeout:        time.Second,
				PoolTimeout:        time.Second,
				ConnMaxLifetime:    time.Hour,
				Dialer: func(context.Context) (net.Conn, error) {
					c, _ := net.Pipe()
					return c, nil
				},
			})
			defer p.Close()
			ctx := context.Background()
			cns := make([]*Conn, 0, n)
			for i := 0; i < n; i++ {
				cn, err := p.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				cns = append(cns, cn)
			}
			for _, cn := range cns {
				p.Put(ctx, cn)
			}
			past := time.Now().Add(-time.Hour)
			for _, cn := range cns {
				cn.expiresAt = past
			}

			// ns/op includes the untimed-by-us restore below. Toggling
			// b.StartTimer/StopTimer per iteration would call ReadMemStats
			// (stop-the-world) each time and swamp the measurement, so the
			// locked section is timed by hand and reported as ns/locked.
			var total time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.connsMu.Lock()
				p.conns = make(map[uint64]*Conn, n)
				for _, cn := range cns {
					p.conns[cn.GetID()] = cn
					cn.GetStateMachine().Transition(StateIdle) // reaping claims it
				}
				p.idleConns = append(make([]*Conn, 0, n), cns...)
				p.poolSize.Store(int32(n))
				p.idleConnsLen.Store(int32(n))
				p.connsMu.Unlock()

				nowNs := time.Now().UnixNano()
				t0 := time.Now()
				p.connsMu.Lock()
				got := p.takeExpiredIdleLocked(nowNs)
				p.connsMu.Unlock()
				total += time.Since(t0)
				if len(got) != n {
					b.Fatalf("took %d, want %d", len(got), n)
				}
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/locked")
		})
	}
}
