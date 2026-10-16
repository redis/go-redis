package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestOnAuthenticationErrRetiresConn covers the error classification in
// baseClient.onAuthenticationErr. A re-auth AUTH rejected by the server replies
// with a plain Redis error, which isBadConn reports as a good connection, so the
// connection used to stay pooled while still holding the superseded credentials.
func TestOnAuthenticationErrRetiresConn(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantPooled bool
	}{
		{
			name: "server rejected the rotated credential",
			err:  proto.RedisError("WRONGPASS invalid username-password pair"),
		},
		{
			name: "no permission for the rotated user",
			err:  proto.RedisError("NOPERM this user has no permissions to run the 'auth' command"),
		},
		{
			name: "transport failure",
			err:  io.ErrUnexpectedEOF,
		},
		{
			name:       "no error",
			err:        nil,
			wantPooled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := startMockRESP2Server(t)
			defer srv.Close()

			client := NewClient(&Options{
				Addr:     srv.Addr(),
				Protocol: 2,
				PoolSize: 1,
			})
			defer client.Close()

			ctx := context.Background()
			if err := client.Ping(ctx).Err(); err != nil {
				t.Fatalf("ping: %v", err)
			}
			cn, err := client.connPool.Get(ctx)
			if err != nil {
				t.Fatalf("get conn: %v", err)
			}
			client.connPool.Put(ctx, cn)
			if got := client.connPool.Len(); got != 1 {
				t.Fatalf("pool size before = %d, want 1", got)
			}

			client.onAuthenticationErr()(cn, tt.err)

			pooled := client.connPool.Len() == 1
			if pooled != tt.wantPooled {
				t.Fatalf("connection pooled after re-auth error = %v, want %v", pooled, tt.wantPooled)
			}
			if !tt.wantPooled && !cn.IsClosed() {
				t.Fatal("connection was removed from the pool but not closed")
			}
		})
	}
}

// TestStreamingReAuthRejectedRetiresConn drives a credential rotation whose AUTH
// the server rejects through the real re-auth pool hook, and asserts the
// connection does not go back into the pool.
func TestStreamingReAuthRejectedRetiresConn(t *testing.T) {
	srv := startAuthRejectingServer(t)
	defer srv.Close()

	provider := &capturingCredentialsProvider{}
	client := NewClient(&Options{
		Addr:                         srv.Addr(),
		Protocol:                     2,
		PoolSize:                     1,
		StreamingCredentialsProvider: provider,
	})
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Check the connection out so the rotation lands while it is in use: the
	// background re-auth is scheduled when it is returned (ReAuthPoolHook.OnPut).
	cn, err := client.connPool.Get(ctx)
	if err != nil {
		t.Fatalf("get conn: %v", err)
	}
	listener := provider.capturedListener()
	if listener == nil {
		t.Fatal("provider was never subscribed")
	}
	listener.OnNext(auth.NewBasicCredentials("u", "rotated"))
	client.connPool.Put(ctx, cn)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.authAttempts() >= 2 && client.connPool.Len() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := srv.authAttempts(); got < 2 {
		t.Fatalf("server saw %d AUTH attempts, want the re-auth to be issued", got)
	}
	if got := client.connPool.Len(); got != 0 {
		t.Fatalf("pool size after rejected re-auth = %d, want 0", got)
	}
}

// authRejectingServer answers the first AUTH with +OK and every later one with a
// WRONGPASS error, so the handshake succeeds and only the re-auth is rejected.
// HELLO is refused to keep the handshake on the RESP2 path.
type authRejectingServer struct {
	ln    net.Listener
	mu    sync.Mutex
	auths int
}

func startAuthRejectingServer(t *testing.T) *authRejectingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := &authRejectingServer{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()
	return s
}

func (s *authRejectingServer) Addr() string { return s.ln.Addr().String() }
func (s *authRejectingServer) Close()       { _ = s.ln.Close() }

func (s *authRejectingServer) authAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.auths
}

func (s *authRejectingServer) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		switch name := strings.ToUpper(args[0]); {
		case name == "HELLO":
			_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
		case name == "AUTH":
			s.mu.Lock()
			s.auths++
			n := s.auths
			s.mu.Unlock()
			if n > 1 {
				_, _ = c.Write([]byte("-WRONGPASS invalid username-password pair\r\n"))
				continue
			}
			_, _ = c.Write([]byte("+OK\r\n"))
		default:
			_, _ = c.Write([]byte("+OK\r\n"))
		}
	}
}

// capturingCredentialsProvider hands out one credential and keeps the listener
// so the test can push a rotation.
type capturingCredentialsProvider struct {
	mu       sync.Mutex
	listener auth.CredentialsListener
}

func (p *capturingCredentialsProvider) Subscribe(
	l auth.CredentialsListener,
) (auth.Credentials, auth.UnsubscribeFunc, error) {
	if l == nil {
		return nil, nil, errors.New("nil listener")
	}
	p.mu.Lock()
	p.listener = l
	p.mu.Unlock()
	return auth.NewBasicCredentials("u", "initial"), func() error { return nil }, nil
}

func (p *capturingCredentialsProvider) capturedListener() auth.CredentialsListener {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listener
}
