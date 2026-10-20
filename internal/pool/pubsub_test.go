package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPubSubPoolTrackConnClose(t *testing.T) {
	for _, order := range []string{"track-before-close", "close-before-track", "close-during-track", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			p := NewPubSubPool(&Options{}, func(context.Context, string, string) (net.Conn, error) {
				return client, nil
			})
			cn, err := p.NewConn(context.Background(), "tcp", "unused", nil)
			if err != nil {
				t.Fatal(err)
			}
			var cleanupCalls atomic.Int32
			cn.SetOnClose(func() error {
				cleanupCalls.Add(1)
				// Cleanup callbacks must be able to reenter the pool.
				return p.Close()
			})
			var trackErr error
			switch order {
			case "track-before-close":
				trackErr = p.TrackConn(cn)
				if trackErr != nil {
					t.Fatal(trackErr)
				}
				if got := p.Stats().Active; got != 1 {
					t.Fatalf("active connections = %d, want 1", got)
				}
				_ = p.Close()
			case "close-before-track":
				_ = p.Close()
				trackErr = p.TrackConn(cn)
				if !errors.Is(trackErr, ErrClosed) {
					t.Fatalf("TrackConn error = %v, want ErrClosed", trackErr)
				}
			case "concurrent":
				var wg sync.WaitGroup
				start := make(chan struct{})
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					trackErr = p.TrackConn(cn)
				}()
				go func() {
					defer wg.Done()
					<-start
					_ = p.Close()
				}()
				close(start)
				wg.Wait()
			case "close-during-track":
				// Close after registration, before TrackConn returns.
				// The assertions below verify that Close drained the connection.
				SetAllMetricCallbacks(&MetricCallbacks{
					ConnectionCount: func(_ context.Context, delta int, conn *Conn, _ string, _ bool) {
						if conn == cn && delta == 1 {
							_ = p.Close()
						}
					},
				})
				defer SetAllMetricCallbacks(nil)
				trackErr = p.TrackConn(cn)
				if trackErr != nil {
					t.Fatalf("TrackConn error = %v, want nil after registration", trackErr)
				}
			}
			if trackErr != nil && !errors.Is(trackErr, ErrClosed) {
				t.Fatal(trackErr)
			}
			wantUntracked := uint32(1)
			if order == "close-before-track" {
				wantUntracked = 0
			} else if order == "concurrent" && trackErr != nil {
				// Rejection under trackMu never registers the connection.
				wantUntracked = 0
			}
			if got := p.Stats(); *got != (PubSubStats{Created: 1, Untracked: wantUntracked}) {
				t.Errorf("stats = %+v, want Created=1 Active=0 Untracked=%d", got, wantUntracked)
			}
			if got := cleanupCalls.Load(); got != 1 {
				t.Errorf("cleanup calls = %d, want 1", got)
			}
			_ = server.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Errorf("peer read error = %v, want EOF", err)
			}
			// Assert resource cleanup above before manually closing anything.
			// Repeated cleanup must not double-count a tracked connection or
			// underflow Active for a rejected connection.
			p.UntrackConn(cn)
			_ = p.Close()
			_ = cn.Close()
			if got := p.Stats(); *got != (PubSubStats{Created: 1, Untracked: wantUntracked}) {
				t.Errorf("stats after repeated cleanup = %+v, want Created=1 Active=0 Untracked=%d", got, wantUntracked)
			}
			if got := cleanupCalls.Load(); got != 1 {
				t.Errorf("cleanup calls after repeated cleanup = %d, want 1", got)
			}
			if _, err := p.NewConn(context.Background(), "tcp", "unused", nil); !errors.Is(err, ErrClosed) {
				t.Errorf("NewConn after Close = %v, want ErrClosed", err)
			}
		})
	}
}
