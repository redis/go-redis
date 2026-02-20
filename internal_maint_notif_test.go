package redis

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestInitConnNilMaintNotificationsConfig is a regression test for
// https://github.com/redis/go-redis/issues/3675
//
// initConn previously accessed MaintNotificationsConfig.EndpointType
// unconditionally, even though the preceding line correctly nil-checked
// MaintNotificationsConfig. This caused a nil pointer dereference panic
// and left optLock.RLock held, leading to a subsequent deadlock.
func TestInitConnNilMaintNotificationsConfig(t *testing.T) {
	// Start a minimal TCP server that speaks enough RESP to let
	// initConn get past the HELLO / AUTH / pipeline phases and reach
	// the MaintNotificationsConfig code path.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// mockRedis responds to every RESP command with a Redis-protocol
	// error. This lets initConn fall through HELLO (Redis errors are
	// not fatal when there is no password) and the empty pipeline
	// succeeds trivially.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.HasPrefix(line, "*") {
						_, _ = c.Write([]byte("-ERR unknown command\r\n"))
					}
				}
			}(conn)
		}
	}()

	opt := &Options{
		Addr: ln.Addr().String(),
	}
	opt.init()

	// Force MaintNotificationsConfig to nil after init() to reproduce
	// the scenario from issue #3675.
	opt.MaintNotificationsConfig = nil

	c := &baseClient{
		opt: opt,
	}
	c.initHooks(hooks{
		dial:       c.dial,
		process:    c.process,
		pipeline:   c.processPipeline,
		txPipeline: c.processTxPipeline,
	})

	// Dial a real connection to the mock server.
	netConn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial mock server: %v", err)
	}
	defer netConn.Close()

	cn := pool.NewConn(netConn)
	// Put the connection into INITIALIZING state so initConn proceeds
	// with the full initialization logic.
	cn.GetStateMachine().Transition(pool.StateInitializing)

	// initConn must not panic. Any returned error is acceptable (the
	// mock server does not implement a full Redis protocol), but a
	// nil-pointer panic on MaintNotificationsConfig is the bug we
	// guard against.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("initConn panicked with nil MaintNotificationsConfig: %v", r)
		}
	}()

	_ = c.initConn(context.Background(), cn)
}
