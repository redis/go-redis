package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

// fakeStreamingCredsProvider is a StreamingCredentialsProvider stub: Subscribe
// hands back initial credentials and captures the listener so a test can push
// a rotated credential via push().
type fakeStreamingCredsProvider struct {
	initial auth.Credentials

	mu       sync.Mutex
	listener auth.CredentialsListener
}

func (p *fakeStreamingCredsProvider) Subscribe(l auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.mu.Lock()
	p.listener = l
	p.mu.Unlock()
	return p.initial, func() error { return nil }, nil
}

func (p *fakeStreamingCredsProvider) push(creds auth.Credentials) {
	p.mu.Lock()
	l := p.listener
	p.mu.Unlock()
	if l != nil {
		l.OnNext(creds)
	}
}

// TestBroadcastSidecar_StreamingCredentialsReconnect: the sidecar must
// authenticate with the current streaming credential and, on a rotation,
// re-AUTH IN PLACE on the same connection (preserving the cache/subscription)
// rather than reconnecting.
func TestBroadcastSidecar_StreamingCredentialsReauth(t *testing.T) {
	srv := startFakeSidecarServer(t, true)
	opt := fakeSidecarOptions(srv.addr)
	prov := &fakeStreamingCredsProvider{initial: auth.NewBasicCredentials("user1", "pass1")}
	opt.StreamingCredentialsProvider = prov

	s := newBroadcastSidecar(opt, NewLocalCache(CacheConfig{MaxEntries: 16}), 0)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Shutdown()

	// First handshake uses the initial credential.
	select {
	case got := <-srv.helloAuth:
		if got != [2]string{"user1", "pass1"} {
			t.Fatalf("first HELLO auth = %v, want [user1 pass1]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sidecar never sent an initial HELLO AUTH")
	}

	// Rotate the credential; the sidecar must AUTH in place with the new token.
	prov.push(auth.NewBasicCredentials("user2", "pass2"))

	select {
	case got := <-srv.authCmd:
		if got != [2]string{"user2", "pass2"} {
			t.Fatalf("in-place AUTH = %v, want [user2 pass2]", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sidecar did not re-AUTH in place with the rotated credential")
	}

	// In-place re-auth must NOT reconnect (that would flush the cache).
	if n := srv.numConns(); n != 1 {
		t.Fatalf("rotation must reuse the connection, got %d conns", n)
	}
	if !s.ready.Load() {
		t.Fatal("sidecar must stay ready across an in-place re-auth")
	}
}
