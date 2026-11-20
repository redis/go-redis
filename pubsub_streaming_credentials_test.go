package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

// connRecordingClient is a pubSubClienter that records every pub/sub
// connection its client dials, so a test can inspect the shared
// connection's re-auth state.
type connRecordingClient struct {
	*Client
	mu    sync.Mutex
	conns []*pool.Conn
}

func (r *connRecordingClient) pubsubConnect(ctx context.Context, addr string) (*pool.Conn, error) {
	cn, err := r.Client.pubsubConnect(ctx, addr)
	if err == nil {
		r.mu.Lock()
		r.conns = append(r.conns, cn)
		r.mu.Unlock()
	}
	return cn, err
}

func (r *connRecordingClient) connIDs() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uint64, 0, len(r.conns))
	for _, cn := range r.conns {
		ids = append(ids, cn.GetID())
	}
	return ids
}

// recordPubSubConns rebuilds client's pub/sub manager over a recording
// wrapper. Call before the first Subscribe.
func recordPubSubConns(client *Client) *connRecordingClient {
	r := &connRecordingClient{Client: client}
	_ = client.pubSubManager.Close()
	client.pubSubManager = newPubSubManager(r, client.cloneOpt())
	return r
}

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

	rec := recordPubSubConns(client)
	const subscriptions = 20
	for i := 0; i < subscriptions; i++ {
		pubsub := client.Subscribe(ctx, "streaming-credentials-leak")
		if _, err := pubsub.Receive(ctx); err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
		if i%2 == 0 {
			// Rotate while the connection is open, so a re-auth is scheduled
			// for it.
			provider.rotate(auth.NewBasicCredentials("default", "password"))
		}
		if err := pubsub.Close(); err != nil {
			t.Fatalf("close #%d: %v", i, err)
		}
	}

	// The last Close releases the shared connection once the server
	// confirms the unsubscribe; wait for every dialed one to be closed.
	manager := client.streamingCredentialsManager
	connIDs := rec.connIDs()
	if len(connIDs) == 0 {
		t.Fatal("no pub/sub connection was dialed")
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, connID := range connIDs {
		for manager.IsTracking(connID) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if manager.IsTracking(connID) {
			t.Fatalf("closed pub/sub connection %d still has re-auth state", connID)
		}
	}
}

// Closing the client closes its open pub/sub connections through the pub/sub
// manager, not through PubSub.Close. That must drop their re-auth state too.
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

	rec := recordPubSubConns(client)
	const subscriptions = 5
	for i := 0; i < subscriptions; i++ {
		pubsub := client.Subscribe(ctx, "streaming-credentials-leak")
		if _, err := pubsub.Receive(ctx); err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
	}
	connIDs := rec.connIDs()
	if len(connIDs) == 0 {
		t.Fatal("no pub/sub connection was dialed")
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
