package pool

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestIsHealthyConnReaderRaceWithSetNetConn is a regression test for a data
// race between isHealthyConn peeking the reply type on cn.rd and a concurrent
// handoff replacing the underlying connection via SetNetConn.
//
// SetNetConn resets cn.rd under readerMu (conn.go). The push-notification peek
// in isHealthyConn read cn.rd without that lock, so a connection popped by Get
// (health-checked before the OnGet state check) races the handoff worker that
// is resetting the same reader.
func TestIsHealthyConnReaderRaceWithSetNetConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// Keep the client socket full of RESP push bytes so connCheck always
	// reports unexpected data and the peek always has a byte to read, which is
	// what drives isHealthyConn into the cn.rd peek branch.
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4096)
		for i := range buf {
			buf[i] = '>' // proto.RespPush
		}
		for {
			if _, err := c.Write(buf); err != nil {
				return
			}
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	cn := NewConn(client)
	p := &ConnPool{cfg: &Options{PushNotificationsEnabled: true}}

	iterations := 5000
	if testing.Short() {
		iterations = 1000
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			cn.SetNetConn(client)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		now := time.Now().UnixNano()
		for i := 0; i < iterations; i++ {
			p.isHealthyConn(cn, now)
		}
	}()
	close(start)
	wg.Wait()
}
