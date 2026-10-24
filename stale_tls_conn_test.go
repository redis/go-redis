package redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// staleTLSServer is a minimal RESP3 server over TLS whose closeAll mimics a
// server-side idle timeout: tls.Conn.Close sends close_notify, then FIN.
type staleTLSServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newStaleTLSServer(t *testing.T) *staleTLSServer {
	t.Helper()
	cert, _ := newClusterTLSTestCertificate(t, "127.0.0.1")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	s := &staleTLSServer{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		s.closeAll()
	})
	return s
}

func (s *staleTLSServer) serve(conn net.Conn) {
	rd := proto.NewReader(bufio.NewReader(conn))
	for {
		v, err := rd.ReadReply()
		if err != nil {
			return
		}
		args, _ := v.([]interface{})
		if len(args) == 0 {
			return
		}
		var reply string
		switch name, _ := args[0].(string); strings.ToUpper(name) {
		case "HELLO":
			reply = "%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n"
		case "PING":
			reply = "+PONG\r\n"
		default:
			reply = "+OK\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func (s *staleTLSServer) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.conns = nil
}

// More idle conns closed by the server than MaxRetries must not fail a command:
// the pool health check cannot see a closed TLS conn, and the pre-command drain
// retires each one before the command is written.
func TestServerClosedIdleTLSConnsDoNotExhaustRetries(t *testing.T) {
	srv := newStaleTLSServer(t)
	const stale = 6

	client := NewClient(&Options{
		Addr:                     srv.ln.Addr().String(),
		Protocol:                 3,
		TLSConfig:                &tls.Config{InsecureSkipVerify: true},
		PoolSize:                 stale + 2,
		MaxRetries:               3,
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()
	ctx := context.Background()

	conns := make([]*Conn, stale)
	for i := range conns {
		conns[i] = client.Conn()
		if err := conns[i].Ping(ctx).Err(); err != nil {
			t.Fatalf("warm-up ping: %v", err)
		}
	}
	for _, cn := range conns {
		_ = cn.Close()
	}
	connPool := client.connPool.(*pool.ConnPool)
	if n := connPool.IdleLen(); n != stale {
		t.Fatalf("idle conns = %d, want %d", n, stale)
	}

	srv.closeAll()
	time.Sleep(100 * time.Millisecond)
	// Skip the drain's recent-put shortcut, as for conns idle longer than 5s.
	_ = connPool.Filter(func(cn *pool.Conn) bool {
		cn.SetLastPutAtNs(0)
		return false
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping after server closed %d idle conns: %v", stale, err)
	}
}
