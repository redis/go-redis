package redis

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
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
	if eph.streamingCredentialsManager != c.streamingCredentialsManager {
		t.Fatal("EphemeralConn() did not inherit the parent's streamingCredentialsManager (#4154)")
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

// TestDerivedClientsColdInitConnDoesNotPanic drives the cold initConn path
// of each derived client against an unresponsive net.Pipe. Before #4154 was
// fixed this panicked with a nil streamingCredentialsManager inside
// Listener; now it gets past Listener and fails the handshake on timeout.
func TestDerivedClientsColdInitConnDoesNotPanic(t *testing.T) {
	provider := &stubStreamingProvider{
		credentials: auth.NewBasicCredentials("default", "token"),
	}
	c := NewClient(&Options{
		Addr:                         "127.0.0.1:1",
		PoolSize:                     1,
		StreamingCredentialsProvider: provider,
		ReadTimeout:                  50 * time.Millisecond,
		WriteTimeout:                 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = c.Close() })

	conn := c.Conn()
	t.Cleanup(func() { _ = conn.Close() })
	eph := c.EphemeralConn()
	t.Cleanup(func() { _ = eph.Close() })
	tx := c.newTx()
	t.Cleanup(func() { _ = tx.Close(context.Background()) })

	for _, tc := range []struct {
		name string
		base *baseClient
	}{
		{name: "Conn", base: &conn.baseClient},
		{name: "EphemeralConn", base: &eph.baseClient},
		{name: "Tx", base: &tx.baseClient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientSide, serverSide := net.Pipe()
			t.Cleanup(func() {
				_ = clientSide.Close()
				_ = serverSide.Close()
			})
			if err := tc.base.initConn(context.Background(), pool.NewConn(clientSide)); err == nil {
				t.Fatal("expected a handshake error from the unresponsive pipe")
			}
		})
	}
}

func TestFailoverClientStreamingCredentials(t *testing.T) {
	provider := &stubStreamingProvider{
		credentials: auth.NewBasicCredentials("default", "token"),
	}
	c := NewFailoverClient(&FailoverOptions{
		MasterName:                   "mymaster",
		SentinelAddrs:                []string{"127.0.0.1:1"},
		PoolSize:                     1,
		StreamingCredentialsProvider: provider,
		ReadTimeout:                  50 * time.Millisecond,
		WriteTimeout:                 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = c.Close() })

	if c.streamingCredentialsManager == nil {
		t.Fatal("expected failover client to create a streamingCredentialsManager")
	}

	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	if err := c.initConn(context.Background(), pool.NewConn(clientSide)); err == nil {
		t.Fatal("expected a handshake error from the unresponsive pipe")
	}

	// The pipeline pool must exist and carry the re-auth hook: removing a
	// conn from it should drop that conn's listener from the manager.
	pp := c.getPipelinePool()
	if pp == nil {
		t.Fatal("expected failover client to build a pipeline pool")
	}
	pipeClient, pipeServer := net.Pipe()
	t.Cleanup(func() { _ = pipeServer.Close() })
	cn := pool.NewConn(pipeClient)
	before, _ := c.streamingCredentialsManager.Listener(cn, nil, nil)
	pp.RemoveWithoutTurn(context.Background(), cn, nil)
	after, _ := c.streamingCredentialsManager.Listener(cn, nil, nil)
	if before == after {
		t.Fatal("pipeline pool has no streaming credentials hook; initStreamingCredentials ran before the pipeline pool was built")
	}
}
