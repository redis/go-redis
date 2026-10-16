package redis

import (
	"context"
	"os"
	"sync"
	"testing"

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
		Addr:                         streamingCredentialsTestAddr(),
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

func streamingCredentialsTestAddr() string {
	if port := os.Getenv("REDIS_PORT"); port != "" {
		return ":" + port
	}
	return ":6379"
}
