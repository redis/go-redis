package pool

import (
	"context"
	"net"
	"sync"
	"testing"
)

// TestConnOnCloseRaceWithInitConn is a regression test for a data race on
// the Conn close hooks (onClose and onCscClose) between a connection being
// (re)initialized and a concurrent Close.
//
// baseClient.initConn installs the close callbacks via SetOnClose (the
// StreamingCredentialsProvider unsubscribe) and SetOnCscClose (the
// client-side-caching eviction hook). initConn runs inside
// SetNetConnAndInitConn while the connection is in the INITIALIZING state.
// Conn.Close transitions to CLOSED from any state, then reads and nils the
// hooks without synchronization. A pool shutdown (or connection removal)
// that closes a connection whose init is still in flight therefore races the
// setters.
func TestConnOnCloseRaceWithInitConn(t *testing.T) {
	iterations := 5000
	if testing.Short() {
		iterations = 1000
	}

	for i := 0; i < iterations; i++ {
		c1, c2 := net.Pipe()

		cn := NewConn(c1)
		cn.SetInitConnFunc(func(ctx context.Context, c *Conn) error {
			// Mirror baseClient.initConn: install the close hooks (the
			// streaming-credentials unsubscribe and the client-side-caching
			// eviction hook), then mark the connection idle so it is ready
			// for use.
			c.SetOnClose(func() error { return nil })
			c.SetOnCscClose(func() error { return nil })
			c.GetStateMachine().Transition(StateIdle)
			return nil
		})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = cn.SetNetConnAndInitConn(context.Background(), c1)
		}()
		go func() {
			defer wg.Done()
			_ = cn.Close()
		}()
		wg.Wait()

		c2.Close()
	}
}
