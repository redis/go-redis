package redis

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

// rotatingCredentialsProvider is a StreamingCredentialsProvider whose rotate
// method pushes new credentials to every subscribed listener.
type rotatingCredentialsProvider struct {
	mu          sync.Mutex
	credentials auth.Credentials
	listeners   map[auth.CredentialsListener]struct{}
}

func (p *rotatingCredentialsProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners[listener] = struct{}{}
	return p.credentials, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.listeners, listener)
		return nil
	}, nil
}

func (p *rotatingCredentialsProvider) rotate(credentials auth.Credentials) {
	p.mu.Lock()
	p.credentials = credentials
	listeners := make([]auth.CredentialsListener, 0, len(p.listeners))
	for listener := range p.listeners {
		listeners = append(listeners, listener)
	}
	p.mu.Unlock()
	for _, listener := range listeners {
		listener.OnNext(credentials)
	}
}

// A pub/sub connection never returns to the pool, so the re-auth pool hook
// never sees it. Closing it must still drop the re-auth listener that
// initConn registered, and the re-auth that a credential rotation scheduled
// for it.
func TestPubSubCloseRemovesStreamingCredentialsState(t *testing.T) {
	ctx := context.Background()
	provider := &rotatingCredentialsProvider{
		credentials: auth.NewBasicCredentials("default", "password"),
		listeners:   map[auth.CredentialsListener]struct{}{},
	}
	client := NewClient(&Options{
		Addr:                         internalTestRedisAddr(),
		StreamingCredentialsProvider: provider,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis is not reachable with the test credentials: %v", err)
	}

	const subscriptions = 20
	connIDs := make([]uint64, 0, subscriptions)
	for i := 0; i < subscriptions; i++ {
		pubsub := client.Subscribe(ctx, "streaming-credentials-leak")
		if _, err := pubsub.Receive(ctx); err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
		connIDs = append(connIDs, pubsub.cn.GetID())
		if i%2 == 0 {
			// Rotate while the connection is open, so a re-auth is scheduled
			// for it.
			provider.rotate(auth.NewBasicCredentials("default", "password"))
		}
		if err := pubsub.Close(); err != nil {
			t.Fatalf("close #%d: %v", i, err)
		}
	}

	manager := client.streamingCredentialsManager
	for _, connID := range connIDs {
		if manager.IsTracking(connID) {
			t.Fatalf("closed pub/sub connection %d still has re-auth state", connID)
		}
	}
}

// Closing the client closes its open pub/sub connections through the pub/sub
// pool, not through PubSub.Close. That must drop their re-auth state too.
func TestClientCloseRemovesStreamingCredentialsStateOfOpenPubSubs(t *testing.T) {
	ctx := context.Background()
	provider := &rotatingCredentialsProvider{
		credentials: auth.NewBasicCredentials("default", "password"),
		listeners:   map[auth.CredentialsListener]struct{}{},
	}
	client := NewClient(&Options{
		Addr:                         internalTestRedisAddr(),
		StreamingCredentialsProvider: provider,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis is not reachable with the test credentials: %v", err)
	}

	const subscriptions = 5
	connIDs := make([]uint64, 0, subscriptions)
	for i := 0; i < subscriptions; i++ {
		pubsub := client.Subscribe(ctx, "streaming-credentials-leak")
		if _, err := pubsub.Receive(ctx); err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
		connIDs = append(connIDs, pubsub.cn.GetID())
	}
	provider.rotate(auth.NewBasicCredentials("default", "password"))

	manager := client.streamingCredentialsManager
	for _, connID := range connIDs {
		if !manager.IsTracking(connID) {
			t.Fatalf("open pub/sub connection %d has no re-auth state before Close", connID)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	for _, connID := range connIDs {
		if manager.IsTracking(connID) {
			t.Fatalf("pub/sub connection %d closed by Client.Close still has re-auth state", connID)
		}
	}
}

type retentionProvider struct{ active atomic.Int32 }

func (p *retentionProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.active.Add(1)
	var once sync.Once
	return auth.NewBasicCredentials("default", "password"), func() error {
		once.Do(func() { p.active.Add(-1) })
		return nil
	}, nil
}

// TestPubSubCloseReleasesStreamingState verifies that closing Pub/Sub connections
// releases streaming credentials listener registrations and active subscriptions (Issue #4149).
func TestPubSubCloseReleasesStreamingState(t *testing.T) {
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("redis is not reachable: %v", err)
	}
	ctx := context.Background()
	p := &retentionProvider{}
	c := NewClient(&Options{
		Addr:                         internalTestRedisAddr(),
		StreamingCredentialsProvider: p,
	})
	defer c.Close()

	for range 3 {
		s := c.Subscribe(ctx, "repro-channel")
		if _, err := s.ReceiveTimeout(ctx, time.Second); err != nil {
			s.Close()
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if p.active.Load() != 0 || c.pubSubPool.Stats().Active != 0 {
		t.Fatalf("subscriptions were not closed: active subscriptions = %d, active pubSub conns = %d", p.active.Load(), c.pubSubPool.Stats().Active)
	}
	m := reflect.ValueOf(c.streamingCredentialsManager).Elem()
	retained := m.FieldByName("credentialsListeners").Elem().FieldByName("listeners").Len()
	if retained != 0 {
		t.Fatalf("closed PubSub connections retained in listener registry: %d", retained)
	}
}
