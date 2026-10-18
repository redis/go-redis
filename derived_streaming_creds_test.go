package redis

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/auth"
)

type stubStreamingProvider struct {
	mu          sync.Mutex
	credentials auth.Credentials
}

func (s *stubStreamingProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.credentials, func() error { return nil }, nil
}

func TestDerivedClientsCarryStreamingCredentialsManager(t *testing.T) {
	provider := &stubStreamingProvider{
		credentials: auth.NewBasicCredentials("default", "token"),
	}
	c := NewClient(&Options{
		Addr:                         "127.0.0.1:1",
		PoolSize:                     1,
		StreamingCredentialsProvider: provider,
	})
	t.Cleanup(func() { _ = c.Close() })

	if c.streamingCredentialsManager == nil {
		t.Fatal("expected client to create a streamingCredentialsManager")
	}

	conn := c.Conn()
	t.Cleanup(func() { _ = conn.Close() })
	if conn.streamingCredentialsManager == nil {
		t.Fatal("Conn() did not inherit streamingCredentialsManager; cold initConn would panic (#4154)")
	}
	if conn.streamingCredentialsManager != c.streamingCredentialsManager {
		t.Fatal("Conn() should share the parent's streamingCredentialsManager")
	}

	eph := c.EphemeralConn()
	t.Cleanup(func() { _ = eph.Close() })
	if eph.streamingCredentialsManager == nil {
		t.Fatal("EphemeralConn() did not inherit streamingCredentialsManager (#4154)")
	}

	tx := c.newTx()
	t.Cleanup(func() { _ = tx.Close(context.Background()) })
	if tx.streamingCredentialsManager == nil {
		t.Fatal("newTx()/Watch did not inherit streamingCredentialsManager (#4154)")
	}
	if tx.streamingCredentialsManager != c.streamingCredentialsManager {
		t.Fatal("newTx() should share the parent's streamingCredentialsManager")
	}
}
