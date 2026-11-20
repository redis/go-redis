package pool

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"
	"weak"
)

func TestLivePoolsDropCollectedPools(t *testing.T) {
	dialer := func(context.Context) (net.Conn, error) {
		c, _ := net.Pipe()
		return c, nil
	}
	connPool := weak.Make(NewConnPool(&Options{Dialer: dialer, PoolSize: 1, PoolTimeout: time.Second}))
	pubSubPool := weak.Make(NewPubSubPool(&Options{}, nil))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		livePoolsMu.Lock()
		_, connLive := liveConnPools[connPool]
		_, pubSubLive := livePubSubPools[pubSubPool]
		livePoolsMu.Unlock()
		if !connLive && !pubSubLive {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pools collected without Close are still registered")
}
