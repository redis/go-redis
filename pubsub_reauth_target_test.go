package redis

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// A re-auth callback is taken from the connection it was scheduled for. If
// the PubSub reconnects before the callback runs, the AUTH must not be sent
// to the replacement connection, which already authenticated when it was
// dialed. See https://github.com/redis/go-redis/issues/4302.
func TestPubSubReAuthSkipsReplacedConnection(t *testing.T) {
	type side struct {
		cn  *pool.Conn
		mu  sync.Mutex
		buf strings.Builder
	}
	newSide := func() (*side, net.Conn) {
		server, client := net.Pipe()
		s := &side{cn: pool.NewConn(client)}
		s.cn.SetResp3(true)
		go func() {
			b := make([]byte, 512)
			for {
				n, err := server.Read(b)
				s.mu.Lock()
				s.buf.Write(b[:n])
				s.mu.Unlock()
				if err != nil {
					return
				}
			}
		}()
		return s, server
	}
	written := func(s *side) string {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.buf.String()
	}

	first, firstServer := newSide()
	second, secondServer := newSide()
	defer firstServer.Close()
	defer secondServer.Close()
	defer second.cn.Close()

	conns := []*pool.Conn{first.cn, second.cn}
	ps := &PubSub{
		opt: &Options{Addr: "stub:6379", Protocol: 3, WriteTimeout: time.Second},
		newConn: func(_ context.Context, _ string, _ []string, onInit ...func(*pool.Conn)) (*pool.Conn, error) {
			if len(conns) == 0 {
				return nil, io.EOF
			}
			cn := conns[0]
			conns = conns[1:]
			for _, fn := range onInit {
				fn(cn)
			}
			return cn, nil
		},
		closeConn: func(cn *pool.Conn) error { return cn.Close() },
	}

	ps.mu.Lock()
	if _, err := ps.conn(context.Background(), nil); err != nil {
		ps.mu.Unlock()
		t.Fatalf("first conn: %v", err)
	}
	ps.mu.Unlock()

	// The listener of the first connection captured its callback ...
	staleReAuth := first.cn.GetPubSubReAuth()
	if staleReAuth == nil {
		t.Fatal("first connection has no re-auth callback")
	}

	// ... and the PubSub reconnected before the callback ran.
	ps.mu.Lock()
	ps.reconnect(context.Background(), nil)
	if ps.cn != second.cn {
		ps.mu.Unlock()
		t.Fatal("reconnect did not switch to the second connection")
	}
	ps.mu.Unlock()

	if err := staleReAuth("user", "old-pass"); err != nil {
		t.Fatalf("stale re-auth: %v", err)
	}

	ps.mu.Lock()
	pending := ps.authPending
	ps.mu.Unlock()
	if pending != 0 {
		t.Fatalf("authPending = %d, want 0", pending)
	}
	// The callback of the current connection still authenticates it.
	if err := second.cn.GetPubSubReAuth()("user", "new-pass"); err != nil {
		t.Fatalf("current re-auth: %v", err)
	}
	var got string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if got = written(second); strings.Contains(got, "new-pass") {
			break
		}
	}
	if !strings.Contains(got, "new-pass") {
		t.Fatalf("AUTH was not sent to the current connection: %q", got)
	}
	// Writes are ordered, so a stale AUTH would show up before the new one.
	if strings.Contains(got, "old-pass") {
		t.Fatalf("stale AUTH was sent to the replacement connection: %q", got)
	}
	ps.mu.Lock()
	pending = ps.authPending
	ps.mu.Unlock()
	if pending != 1 {
		t.Fatalf("authPending = %d, want 1", pending)
	}
}
