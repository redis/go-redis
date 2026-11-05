package e2e

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
)

// fakeTokenProvider stands in for go-redis-entraid: one provider shared by
// every member, handing out the current token and pushing each new token to
// every subscribed connection.
type fakeTokenProvider struct {
	mu        sync.Mutex
	creds     auth.Credentials
	listeners map[auth.CredentialsListener]struct{}
	calls     atomic.Int64 // credentialsContext calls
}

func newFakeTokenProvider(username, token string) *fakeTokenProvider {
	return &fakeTokenProvider{
		creds:     auth.NewBasicCredentials(username, token),
		listeners: map[auth.CredentialsListener]struct{}{},
	}
}

// Subscribe implements auth.StreamingCredentialsProvider.
func (p *fakeTokenProvider) Subscribe(l auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners[l] = struct{}{}
	return p.creds, func() error {
		p.mu.Lock()
		delete(p.listeners, l)
		p.mu.Unlock()
		return nil
	}, nil
}

// credentialsContext is the CredentialsProviderContext form: it returns the
// current token and pushes nothing.
func (p *fakeTokenProvider) credentialsContext(ctx context.Context) (string, string, error) {
	p.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	u, pw := p.creds.BasicAuth()
	return u, pw, nil
}

// rotate sets a new token and pushes it to every subscriber, as the entraid
// token manager does on refresh.
func (p *fakeTokenProvider) rotate(username, token string) {
	creds := auth.NewBasicCredentials(username, token)
	p.mu.Lock()
	p.creds = creds
	ls := make([]auth.CredentialsListener, 0, len(p.listeners))
	for l := range p.listeners {
		ls = append(ls, l)
	}
	p.mu.Unlock()
	for _, l := range ls {
		l.OnNext(creds)
	}
}

// setTokenSilently changes the token without notifying subscribers.
func (p *fakeTokenProvider) setTokenSilently(username, token string) {
	p.mu.Lock()
	p.creds = auth.NewBasicCredentials(username, token)
	p.mu.Unlock()
}

// aclAdmin opens a direct client to the shared mock backend (through the
// last proxy, which no test faults here) and creates one ACL user per
// username/token pair. Cleanup deletes them.
func aclAdmin(t *testing.T, users map[string]string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: lastMemberAddr(), MaxRetries: -1})
	ctx := context.Background()
	for user, token := range users {
		if err := c.Do(ctx, "ACL", "SETUSER", user, "reset", "on", ">"+token, "~*", "&*", "+@all").Err(); err != nil {
			t.Fatalf("ACL SETUSER %s: %v", user, err)
		}
	}
	t.Cleanup(func() {
		for user := range users {
			_ = c.Do(context.Background(), "ACL", "DELUSER", user).Err()
		}
		_ = c.Close()
	})
	return c
}

// whoamiIs reports whether ACL WHOAMI through mdb returns want n times in a
// row, so a single connection that still has the old user fails the check.
func whoamiIs(ctx context.Context, mdb *redis.MultiDBClient, want string, n int) bool {
	for i := 0; i < n; i++ {
		got, err := mdb.Do(ctx, "ACL", "WHOAMI").Text()
		if err != nil || got != want {
			return false
		}
	}
	return true
}

// clientInfo returns the server-side id and user of the connection that ran
// CLIENT INFO.
func clientInfo(t *testing.T, ctx context.Context, mdb *redis.MultiDBClient) (id, user string) {
	t.Helper()
	info, err := mdb.Do(ctx, "CLIENT", "INFO").Text()
	if err != nil {
		t.Fatalf("CLIENT INFO: %v", err)
	}
	for _, f := range strings.Fields(info) {
		if v, ok := strings.CutPrefix(f, "id="); ok {
			id = v
		} else if v, ok := strings.CutPrefix(f, "user="); ok {
			user = v
		}
	}
	return id, user
}

// serverUsers returns the user of each connection in ids that the server
// still has, read with CLIENT LIST ID through admin.
func serverUsers(t *testing.T, ctx context.Context, admin *redis.Client, ids map[string]bool) map[string]string {
	t.Helper()
	args := []interface{}{"CLIENT", "LIST", "ID"}
	for id := range ids {
		args = append(args, id)
	}
	list, err := admin.Do(ctx, args...).Text()
	if err != nil {
		t.Fatalf("CLIENT LIST ID: %v", err)
	}
	users := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(list), "\n") {
		var id, user string
		for _, f := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(f, "id="); ok {
				id = v
			} else if v, ok := strings.CutPrefix(f, "user="); ok {
				user = v
			}
		}
		if id != "" {
			users[id] = user
		}
	}
	return users
}

const (
	tokenUserA = "e2e-token-a"
	tokenUserB = "e2e-token-b"
)

// TestStreamingCredentialsRotateAndFailover: every member shares one
// streaming provider (entraid-style). A token pushed with OnNext
// re-authenticates the connections already open, revoking the old token does
// not break traffic, and the member taking over after a failover uses the
// newest token.
//
// Mock-only: it creates ACL users on the backend, which a real Active-Active
// database does not allow.
func TestStreamingCredentialsRotateAndFailover(t *testing.T) {
	skipIfRealMode(t, "creates ACL users on the shared backend")
	admin := aclAdmin(t, map[string]string{tokenUserA: "token-a", tokenUserB: "token-b"})
	p := newFakeTokenProvider(tokenUserA, "token-a")

	opts := fiMultiDBOptions()
	for i := range opts.Clients {
		opts.Clients[i].Options.StreamingCredentialsProvider = p
	}
	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	if !whoamiIs(ctx, mdb, tokenUserA, 5) {
		t.Fatal("ACL WHOAMI before rotation is not the provider's first user")
	}
	before := map[string]bool{}
	for i := 0; i < 5; i++ {
		id, user := clientInfo(t, ctx, mdb)
		if user != tokenUserA {
			t.Fatalf("connection %s user = %q before rotation, want %q", id, user, tokenUserA)
		}
		before[id] = true
	}

	// Rotate: the pushed token re-authenticates the open connections. A
	// connection opened after the rotation gets the new token anyway, so the
	// check needs a connection that existed before it. Re-auth runs in the
	// background and the pool may hand out a newer connection afterwards, so
	// read the old connections' users on the server instead of through mdb.
	p.rotate(tokenUserB, "token-b")
	eventually(t, 10*time.Second, "a pre-rotation connection re-authenticated with the rotated token", func() bool {
		for _, user := range serverUsers(t, ctx, admin, before) {
			if user == tokenUserB {
				return true
			}
		}
		return false
	})
	if !whoamiIs(ctx, mdb, tokenUserB, 10) {
		t.Fatal("a connection still uses the old token after the rotation")
	}

	// Revoke the old token. The server drops any connection still on it;
	// traffic must keep working on the new one.
	if err := admin.Do(ctx, "ACL", "DELUSER", tokenUserA).Err(); err != nil {
		t.Fatalf("ACL DELUSER %s: %v", tokenUserA, err)
	}
	eventually(t, 10*time.Second, "traffic after the old token was revoked", func() bool {
		return mdb.Set(ctx, "e2e:creds:streaming", "after-revoke", 0).Err() == nil &&
			whoamiIs(ctx, mdb, tokenUserB, 5)
	})

	// Fail over: the next member authenticates with the newest token.
	triggerNetworkFailure(t, 0, 30*time.Second)
	eventually(t, 15*time.Second, "commands on the new active with the rotated token", func() bool {
		return mdb.ActiveDatabaseID() != 0 &&
			mdb.Set(ctx, "e2e:creds:streaming", "after-failover", 0).Err() == nil &&
			whoamiIs(ctx, mdb, tokenUserB, 5)
	})
}

// TestCredentialsProviderContextAfterTokenChange: every member reads its
// credentials from one CredentialsProviderContext, which pushes nothing.
// After the token changes and the old one is revoked, the server drops the
// old connections; new ones read the new token from the provider, before and
// after a failover.
//
// Mock-only for the same reason as TestStreamingCredentialsRotateAndFailover.
func TestCredentialsProviderContextAfterTokenChange(t *testing.T) {
	skipIfRealMode(t, "creates ACL users on the shared backend")
	admin := aclAdmin(t, map[string]string{tokenUserA: "token-a", tokenUserB: "token-b"})
	p := newFakeTokenProvider(tokenUserA, "token-a")

	opts := fiMultiDBOptions()
	for i := range opts.Clients {
		opts.Clients[i].Options.CredentialsProviderContext = p.credentialsContext
	}
	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	if !whoamiIs(ctx, mdb, tokenUserA, 5) {
		t.Fatal("ACL WHOAMI before the token change is not the provider's first user")
	}
	if p.calls.Load() == 0 {
		t.Fatal("CredentialsProviderContext was never called")
	}

	// Change the token without a push, then revoke the old one: only new
	// connections pick up the change.
	p.setTokenSilently(tokenUserB, "token-b")
	if err := admin.Do(ctx, "ACL", "DELUSER", tokenUserA).Err(); err != nil {
		t.Fatalf("ACL DELUSER %s: %v", tokenUserA, err)
	}
	eventually(t, 15*time.Second, "reconnected with the new token", func() bool {
		return mdb.Set(ctx, "e2e:creds:context", "after-change", 0).Err() == nil &&
			whoamiIs(ctx, mdb, tokenUserB, 5)
	})

	triggerNetworkFailure(t, 0, 30*time.Second)
	eventually(t, 15*time.Second, "commands on the new active with the new token", func() bool {
		return mdb.ActiveDatabaseID() != 0 &&
			mdb.Set(ctx, "e2e:creds:context", "after-failover", 0).Err() == nil &&
			whoamiIs(ctx, mdb, tokenUserB, 5)
	})
}
