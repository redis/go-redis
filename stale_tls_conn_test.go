package redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// staleTLSServer is a minimal RESP3 server over TLS whose closeAll mimics a
// server-side idle timeout: tls.Conn.Close sends close_notify, then FIN.
type staleTLSServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	// trailingPush sends a push frame after the HELLO reply.
	trailingPush bool
}

func newStaleTLSServer(t *testing.T, trailingPush bool) *staleTLSServer {
	t.Helper()
	cert, _ := newClusterTLSTestCertificate(t, "127.0.0.1")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	s := &staleTLSServer{ln: ln, trailingPush: trailingPush}
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
			if s.trailingPush {
				reply += ">2\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n"
			}
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

// newStaleTLSClient returns a client whose pool holds stale idle conns the
// server has closed, past the drain's recent-put shortcut.
func newStaleTLSClient(t *testing.T, srv *staleTLSServer, stale int, opt *Options) *Client {
	t.Helper()
	opt.Addr = srv.ln.Addr().String()
	opt.Protocol = 3
	opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	opt.PoolSize = stale + 2
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	client := NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
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
	return client
}

// More idle conns closed by the server than MaxRetries must not fail a command:
// the pool health check cannot see a closed TLS conn, and the pre-command drain
// retires each one before the command is written.
func TestServerClosedIdleTLSConnsDoNotExhaustRetries(t *testing.T) {
	const stale = 6
	client := newStaleTLSClient(t, newStaleTLSServer(t, false), stale, &Options{MaxRetries: 3})

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping after server closed %d idle conns: %v", stale, err)
	}
}

// Stale conns met on a retried attempt cost neither extra backoff nor attempts.
func TestServerClosedIdleTLSConnsOnRetryNoExtraBackoff(t *testing.T) {
	const backoff = 100 * time.Millisecond
	client := newStaleTLSClient(t, newStaleTLSServer(t, false), 6, &Options{
		MaxRetries:      3,
		MinRetryBackoff: backoff,
		MaxRetryBackoff: backoff,
	})
	ctx := context.Background()

	cmd := NewStatusCmd(ctx, "ping")
	var state processState
	start := time.Now()
	if err := client.processWithRetry(ctx, cmd, nil, &state, 1); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*backoff {
		t.Errorf("elapsed = %v, want about one backoff (%v)", elapsed, backoff)
	}
	if state.attempts != 2 {
		t.Errorf("attempts = %d, want 2", state.attempts)
	}
}

type failingPushProcessor struct{ push.NotificationProcessor }

func (failingPushProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	return errors.New("push processor failure")
}

// Drain errors other than a closed conn stay on the normal retry budget.
func TestPushProcessorErrorSpendsRetries(t *testing.T) {
	srv := newStaleTLSServer(t, true)
	const maxRetries = 3
	var dials atomic.Int32
	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
	client := NewClient(&Options{
		Addr:     srv.ln.Addr().String(),
		Protocol: 3,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return dialer.DialContext(ctx, network, addr)
		},
		PoolSize:                  10,
		MaxRetries:                maxRetries,
		MinRetryBackoff:           -1,
		DisableIdentity:           true,
		PushNotificationProcessor: failingPushProcessor{push.NewProcessor()},
		MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()

	if err := client.Ping(context.Background()).Err(); err == nil {
		t.Fatal("ping succeeded, want the push processor error")
	}
	if n := dials.Load(); n != maxRetries+1 {
		t.Errorf("dials = %d, want %d", n, maxRetries+1)
	}
}
