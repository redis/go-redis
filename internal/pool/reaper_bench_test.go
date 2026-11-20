package pool

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// BenchmarkReapScanNoneExpired is the steady-state cost of one reaper tick:
// scan N idle conns under connsMu, nothing to close.
func BenchmarkReapScanNoneExpired(b *testing.B) {
	for _, n := range []int{10, 100, 500, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			p := NewConnPool(&Options{
				PoolSize:           int32(n),
				MaxConcurrentDials: 64,
				DialTimeout:        time.Second,
				PoolTimeout:        time.Second,
				ConnMaxLifetime:    time.Hour,
				ConnMaxIdleTime:    time.Hour,
				Dialer: func(context.Context) (net.Conn, error) {
					c, s := net.Pipe()
					_ = s
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
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.reapExpiredIdle(ctx)
			}
		})
	}
}
