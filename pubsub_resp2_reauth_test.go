package redis_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// These tests run against a real Redis server. In RESP2 a subscribed
// connection rejects AUTH ("only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING /
// QUIT / RESET are allowed in this context"), so a credentials rotation must
// move the subscription to a new connection that authenticated with the new
// credentials.

// resp2ReauthAddr is the standalone server of the docker-compose stack.
func resp2ReauthAddr() string {
	return redisStackAddr
}

// resp2ReauthUsers creates three ACL users with full access and returns their
// names and passwords. The users are deleted when the test ends.
func resp2ReauthUsers(t *testing.T) (admin *redis.Client, users [3][2]string) {
	t.Helper()
	if re, _ := strconv.ParseBool(os.Getenv("RE_CLUSTER")); re {
		t.Skip("needs ACL SETUSER on a standalone server")
	}
	admin = redis.NewClient(&redis.Options{Addr: resp2ReauthAddr(), Protocol: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := admin.Ping(ctx).Err(); err != nil {
		_ = admin.Close()
		t.Skipf("redis not reachable at %s: %v", resp2ReauthAddr(), err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	for i := range users {
		name := fmt.Sprintf("resp2reauth_%d_%s", i, suffix)
		pass := "pass_" + name
		if err := admin.Do(ctx, "ACL", "SETUSER", name, "on", ">"+pass, "~*", "&*", "+@all").Err(); err != nil {
			_ = admin.Close()
			t.Skipf("ACL SETUSER failed: %v", err)
		}
		users[i] = [2]string{name, pass}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for _, u := range users {
			_ = admin.Do(ctx, "ACL", "DELUSER", u[0]).Err()
		}
		_ = admin.Close()
	})
	return admin, users
}

// subscribedConns returns, per ACL user, the number of connections that have
// at least one channel subscription.
func subscribedConns(t *testing.T, admin *redis.Client) map[string]int {
	t.Helper()
	list, err := admin.ClientList(context.Background()).Result()
	if err != nil {
		t.Fatalf("CLIENT LIST: %v", err)
	}
	out := map[string]int{}
	for _, line := range strings.Split(list, "\n") {
		var user string
		var sub int
		for _, f := range strings.Fields(line) {
			k, v, ok := strings.Cut(f, "=")
			if !ok {
				continue
			}
			switch k {
			case "user":
				user = v
			case "sub":
				sub, _ = strconv.Atoi(v)
			}
		}
		if sub > 0 {
			out[user]++
		}
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func resp2ReauthClient(provider auth.StreamingCredentialsProvider) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                         resp2ReauthAddr(),
		Protocol:                     2,
		StreamingCredentialsProvider: provider,
		DisableIdentity:              true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
}

// A rotation moves the subscription to a connection that uses the new user,
// and the old connection is closed.
func TestPubSubRESP2RotationMovesSubscriptionRealRedis(t *testing.T) {
	admin, users := resp2ReauthUsers(t)
	provider := newTestStreamingProvider(users[0][0], users[0][1])
	client := resp2ReauthClient(provider)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channel := "resp2reauth_move_" + users[0][0]
	ps := client.Subscribe(ctx, channel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got := subscribedConns(t, admin)[users[0][0]]; got != 1 {
		t.Fatalf("subscribed conns for old user = %d, want 1", got)
	}

	provider.Update(users[1][0], users[1][1])

	if !waitFor(t, 3*time.Second, func() bool {
		c := subscribedConns(t, admin)
		return c[users[1][0]] == 1 && c[users[0][0]] == 0
	}) {
		t.Fatalf("after rotation: subscribed conns = %v, want only %s", subscribedConns(t, admin), users[1][0])
	}

	if err := admin.Publish(ctx, channel, "after").Err(); err != nil {
		t.Fatal(err)
	}
	msg, err := ps.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage after rotation: %v", err)
	}
	if msg.Payload != "after" {
		t.Fatalf("payload = %q, want %q", msg.Payload, "after")
	}
}

// A goroutine blocked in ReceiveMessage while the rotation happens must not
// see an error: the rotation is not a failure of the subscription.
func TestPubSubRESP2RotationBlockedReceiverRealRedis(t *testing.T) {
	admin, users := resp2ReauthUsers(t)
	provider := newTestStreamingProvider(users[0][0], users[0][1])
	client := resp2ReauthClient(provider)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channel := "resp2reauth_blocked_" + users[0][0]
	ps := client.Subscribe(ctx, channel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var (
		mu       sync.Mutex
		errs     []error
		payloads []string
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg, err := ps.ReceiveMessage(ctx)
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			} else {
				payloads = append(payloads, msg.Payload)
			}
			stop := ctx.Err() != nil || (err == nil && msg.Payload == "last")
			mu.Unlock()
			if stop {
				return
			}
		}
	}()

	time.Sleep(100 * time.Millisecond) // let the receiver block in the read
	provider.Update(users[1][0], users[1][1])
	if !waitFor(t, 3*time.Second, func() bool { return subscribedConns(t, admin)[users[1][0]] == 1 }) {
		t.Fatalf("no subscribed connection for the new user: %v", subscribedConns(t, admin))
	}
	if err := admin.Publish(ctx, channel, "last").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not get the message published after the rotation")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(errs) > 0 {
		t.Fatalf("ReceiveMessage returned %d error(s) during rotation, first: %v (payloads %v)", len(errs), errs[0], payloads)
	}
}

// A message that the server delivered to the old connection must reach the
// caller: the rotation must not drop messages that are already in flight.
// Messages published while no connection was subscribed (PUBLISH returned 0)
// are not delivered by Redis at all; they are logged, not asserted.
func TestPubSubRESP2RotationNoMessageLossRealRedis(t *testing.T) {
	resp2RotationLoss(t, false, false)
}

// Same as above, but the consumer reads only after the rotation. The
// Channel() buffer fills up, the reader goroutine stops reading the socket,
// and messages wait unread on the old connection when the rotation happens.
func TestPubSubRESP2RotationNoMessageLossSlowConsumerRealRedis(t *testing.T) {
	resp2RotationLoss(t, true, false)
}

// A second rotation arrives while the first one is still dialing, so it
// runs while the first replaced connection may still be draining.
func TestPubSubRESP2RapidRotationsNoMessageLossRealRedis(t *testing.T) {
	resp2RotationLoss(t, true, true)
}

func resp2RotationLoss(t *testing.T, slowConsumer, secondRotation bool) {
	admin, users := resp2ReauthUsers(t)
	inner := newTestStreamingProvider(users[0][0], users[0][1])
	provider := &subscribeHookProvider{testStreamingProvider: inner, onSubscribe: func(n int) {
		if secondRotation && n == 2 {
			go inner.Update(users[2][0], users[2][1])
		}
	}}
	client := resp2ReauthClient(provider)
	defer client.Close()
	finalUser := users[1][0]
	if secondRotation {
		finalUser = users[2][0]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channel := "resp2reauth_loss_" + users[0][0]
	ps := client.Subscribe(ctx, channel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ch := ps.Channel()

	var mu sync.Mutex
	seen := map[int]int{}
	consume := func(until <-chan time.Time) {
		for {
			select {
			case m := <-ch:
				n, _ := strconv.Atoi(m.Payload)
				mu.Lock()
				seen[n]++
				mu.Unlock()
			case <-until:
				return
			}
		}
	}
	consumerDone := make(chan struct{})
	if !slowConsumer {
		go func() {
			defer close(consumerDone)
			consume(time.After(3 * time.Second))
		}()
	}

	var receivers []int64 // PUBLISH reply per sequence number
	stop := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n, err := admin.Publish(ctx, channel, strconv.Itoa(i)).Result()
			if err != nil {
				return
			}
			receivers = append(receivers, n)
			time.Sleep(200 * time.Microsecond)
		}
	}()

	onlyUser := func(user string) func() bool {
		return func() bool {
			c := subscribedConns(t, admin)
			return c[user] == 1 && c[users[0][0]]+c[users[1][0]]+c[users[2][0]] == 1
		}
	}
	time.Sleep(100 * time.Millisecond)
	inner.Update(users[1][0], users[1][1])
	// With a slow consumer the second rotation waits until the first
	// replaced connection is drained, which needs the consumer to read.
	if !waitFor(t, 5*time.Second, func() bool {
		return onlyUser(finalUser)() || (slowConsumer && onlyUser(users[1][0])())
	}) {
		close(stop)
		<-pubDone
		t.Fatalf("subscribed conns = %v, want only %s", subscribedConns(t, admin), finalUser)
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	<-pubDone

	if slowConsumer {
		consume(time.After(time.Second))
	} else {
		<-consumerDone
	}
	if !waitFor(t, 3*time.Second, onlyUser(finalUser)) {
		t.Fatalf("subscribed conns = %v, want only %s", subscribedConns(t, admin), finalUser)
	}

	mu.Lock()
	defer mu.Unlock()
	var dropped, gap, dups []int
	for i, n := range receivers {
		switch {
		case n == 0:
			gap = append(gap, i)
		case seen[i] == 0:
			dropped = append(dropped, i)
		}
		if seen[i] > 1 {
			dups = append(dups, i)
		}
	}
	t.Logf("published=%d delivered-to-a-conn-but-dropped=%d published-with-no-subscriber=%d duplicates=%d",
		len(receivers), len(dropped), len(gap), len(dups))
	if len(dropped) > 0 {
		t.Fatalf("%d messages delivered by the server were dropped during rotation: %v", len(dropped), dropped)
	}
	// The old connection's UNSUBSCRIBE and the new connection's SUBSCRIBE
	// go out on two sockets, so the server can apply the SUBSCRIBE first:
	// at most one message per rotation can then arrive on both.
	rotations := 1
	if secondRotation {
		rotations = 2
	}
	if len(dups) > rotations {
		t.Fatalf("%d messages were delivered twice, want at most %d: %v", len(dups), rotations, dups)
	}
}

// Two rotations in quick succession: the second can arrive while the first
// is still dialing its new connection. The subscription must end up on a
// connection that uses the newest credentials.
func TestPubSubRESP2RapidRotationsRealRedis(t *testing.T) {
	admin, users := resp2ReauthUsers(t)
	inner := newTestStreamingProvider(users[0][0], users[0][1])
	// Subscribe #1 is the initial PubSub connection, #2 is the connection the
	// first rotation dials. Rotate again right after #2 subscribed, before
	// the first rotation swaps it in.
	provider := &subscribeHookProvider{testStreamingProvider: inner, onSubscribe: func(n int) {
		if n == 2 {
			go inner.Update(users[2][0], users[2][1])
		}
	}}
	client := resp2ReauthClient(provider)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channel := "resp2reauth_rapid_" + users[0][0]
	ps := client.Subscribe(ctx, channel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ch := ps.Channel()

	inner.Update(users[1][0], users[1][1])

	if !waitFor(t, 5*time.Second, func() bool {
		c := subscribedConns(t, admin)
		return c[users[2][0]] == 1 && c[users[1][0]] == 0 && c[users[0][0]] == 0
	}) {
		t.Fatalf("subscribed conns = %v, want only %s", subscribedConns(t, admin), users[2][0])
	}
	if err := admin.Publish(ctx, channel, "after").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ch:
		if m.Payload != "after" {
			t.Fatalf("payload = %q", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message after rotations")
	}
}

// subscribeHookProvider calls onSubscribe with the subscription count after
// each Subscribe.
type subscribeHookProvider struct {
	*testStreamingProvider
	mu          sync.Mutex
	n           int
	onSubscribe func(n int)
}

func (p *subscribeHookProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	creds, unsub, err := p.testStreamingProvider.Subscribe(listener)
	p.mu.Lock()
	p.n++
	n := p.n
	p.mu.Unlock()
	if p.onSubscribe != nil {
		p.onSubscribe(n)
	}
	return creds, unsub, err
}

// The user's own Unsubscribe, still unread when the rotation happens, must
// reach the caller; only the rotation's internal replies are hidden.
func TestPubSubRESP2RotationKeepsUserUnsubscribeRealRedis(t *testing.T) {
	_, users := resp2ReauthUsers(t)
	provider := newTestStreamingProvider(users[0][0], users[0][1])
	client := resp2ReauthClient(provider)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := "resp2reauth_ua_"+users[0][0], "resp2reauth_ub_"+users[0][0]
	ps := client.Subscribe(ctx, a, b)
	defer ps.Close()
	for i := 0; i < 2; i++ {
		if _, err := ps.Receive(ctx); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}
	if err := ps.Unsubscribe(ctx, a); err != nil {
		t.Fatal(err)
	}
	provider.Update(users[1][0], users[1][1])

	for {
		msg, err := ps.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			t.Fatalf("did not see the user's unsubscribe of %s: %v", a, err)
		}
		sub, ok := msg.(*redis.Subscription)
		if !ok {
			continue
		}
		if sub.Kind == "unsubscribe" && sub.Channel == b {
			t.Fatalf("internal unsubscribe of %s leaked to the caller", b)
		}
		if sub.Kind == "unsubscribe" && sub.Channel == a {
			return
		}
	}
}
