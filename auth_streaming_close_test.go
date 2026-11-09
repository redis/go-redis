package redis

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// A close after SingleConnPool.Get but before initPooledConn's IsInited check
// reaches initConn through the handshake wrapper, which has no streaming manager.
func TestInitConnRejectsClosedConnection(t *testing.T) {
	provider := &stubStreamingProvider{}
	client := NewClient(&Options{StreamingCredentialsProvider: provider})
	t.Cleanup(func() { _ = client.Close() })
	clientConn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	cn := pool.NewConn(clientConn)
	single := pool.NewSingleConnPool(client.connPool, cn)
	if _, err := single.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cn.Close(); err != nil {
		t.Fatal(err)
	}
	wrapper := newConn(client.opt, single, nil, nil)
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("closed handshake connection re-entered subscription setup: %v", p)
		}
	}()
	if err := wrapper.initPooledConn(context.Background(), single, cn); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("initialization error = %v, want ErrClosed", err)
	}
	if !cn.IsClosed() {
		t.Fatal("initialization revived a closed connection")
	}
}

// Count every unsubscribe invocation, even when its effect is idempotent.
type closeRaceCredentialsProvider struct {
	entered      chan struct{}
	release      chan struct{}
	listener     auth.CredentialsListener
	credentials  auth.Credentials
	active       atomic.Int32
	unsubscribes atomic.Int32
	helloAuth    atomic.Bool
	onConnect    atomic.Int32
	onHello      func()
	onRead       func()
}

type closeRaceNetConn struct {
	net.Conn
	provider *closeRaceCredentialsProvider
}

func (c closeRaceNetConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.provider.onRead != nil {
		c.provider.onRead()
	}
	return n, err
}

func (p *closeRaceCredentialsProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.listener = listener
	p.active.Add(1)
	close(p.entered)
	<-p.release
	var once sync.Once
	return p.credentials, func() error {
		p.unsubscribes.Add(1)
		once.Do(func() { p.active.Add(-1) })
		return nil
	}, nil
}

func newCloseRaceConnection(t *testing.T, pubsub bool) (*Client, *pool.Conn, *closeRaceCredentialsProvider) {
	t.Helper()
	provider := &closeRaceCredentialsProvider{
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		credentials: auth.NewBasicCredentials("user", "token"),
	}
	clientConn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	client := NewClient(&Options{
		Addr:                         "test",
		StreamingCredentialsProvider: provider,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return closeRaceNetConn{clientConn, provider}, nil
		},
		PoolSize:                 1,
		MaxRetries:               -1,
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		OnConnect: func(ctx context.Context, cn *Conn) error {
			provider.onConnect.Add(1)
			return cn.Ping(ctx).Err()
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	var cn *pool.Conn
	var err error
	if pubsub {
		cn, err = client.pubSubPool.NewConn(context.Background(), "tcp", "test", nil)
	} else {
		cn, err = client.connPool.NewConn(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cn.Close() })
	go func() {
		rd := proto.NewReader(peer)
		for {
			cmd, err := rd.ReadSlice()
			if err != nil {
				return
			}
			reply := "+OK\r\n"
			if cmd[0] == "hello" {
				provider.helloAuth.Store(len(cmd) == 5 && cmd[2] == "auth" && cmd[3] == "user" && cmd[4] == "token")
				if provider.onHello != nil {
					provider.onHello()
				}
				reply = "%0\r\n"
			}
			if _, err := peer.Write([]byte(reply)); err != nil {
				return
			}
		}
	}()
	return client, cn, provider
}

func checkCloseRaceCleanup(t *testing.T, client *Client, cn *pool.Conn, provider *closeRaceCredentialsProvider) {
	t.Helper()
	// Check before another Close or pool removal can mask a missed late callback.
	if !cn.IsClosed() {
		t.Error("initialization revived a closed connection")
	}
	if got := provider.unsubscribes.Load(); got != 1 {
		t.Errorf("unsubscribe calls = %d, want 1", got)
	}
	if got := provider.active.Load(); got != 0 {
		t.Errorf("active subscriptions = %d, want 0", got)
	}
	if !cn.IsPubSub() {
		// Regular connections lose manager state through the pool's OnRemove hook.
		client.connPool.(*pool.ConnPool).RemoveWithoutTurn(context.Background(), cn, pool.ErrClosed)
	}
	if client.streamingCredentialsManager.IsTracking(cn.GetID()) {
		t.Error("closed connection still has streaming credentials state")
	}
	for i := 0; i < 3; i++ {
		_ = cn.Close()
	}
	if got := provider.unsubscribes.Load(); got != 1 {
		t.Errorf("unsubscribe calls after repeated close = %d, want 1", got)
	}
}

func TestStreamingCredentialsCloseDuringSubscribe(t *testing.T) {
	for _, pubsub := range []bool{false, true} {
		name := "regular"
		if pubsub {
			name = "pubsub"
		}
		t.Run(name, func(t *testing.T) {
			client, cn, provider := newCloseRaceConnection(t, pubsub)
			release := sync.OnceFunc(func() { close(provider.release) })
			t.Cleanup(release)
			done := make(chan error, 1)
			go func() { done <- client.initConn(context.Background(), cn) }()
			select {
			case <-provider.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("subscription did not start")
			}
			// Seed pending re-auth state before close, and deliver an already
			// in-flight notification afterwards as well.
			provider.listener.OnNext(provider.credentials)
			_ = cn.Close()
			provider.listener.OnNext(provider.credentials)
			release()
			select {
			case err := <-done:
				if !errors.Is(err, pool.ErrClosed) {
					t.Errorf("initialization error = %v, want ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("initialization did not finish")
			}
			checkCloseRaceCleanup(t, client, cn, provider)
		})
	}
}
