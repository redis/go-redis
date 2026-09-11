// Package pubsub_test holds the pub/sub integration suite. It lives in
// the external test package so it can exercise the engine through the
// public redis.Client API (an external test package may import the root
// package even though the root package imports internal/pubsub).
//
// The suite needs a running Redis (the docker-compose stack, see
// AGENTS.md); when the server is unreachable every test skips.
package pubsub_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func testAddr() string {
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		// The docker-compose standalone service (see the root suite's
		// redisStackPort in main_test.go).
		port = "6379"
	}
	return "127.0.0.1:" + port
}

// newTestClient returns a client against the test server, skipping the
// test when the server is unreachable (TCP probe, mirroring the root
// suite's skip gates).
func newTestClient(t *testing.T, opts ...func(*redis.Options)) *redis.Client {
	t.Helper()

	addr := testAddr()
	probe, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Skipf("redis server not reachable at %s: %v", addr, err)
	}
	_ = probe.Close()

	opt := &redis.Options{Addr: addr}
	for _, f := range opts {
		f(opt)
	}
	client := redis.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// skipBeforeVersion skips the test when the server is older than
// major.minor.
func skipBeforeVersion(t *testing.T, client *redis.Client, major, minor int, reason string) {
	t.Helper()

	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("INFO server: %v", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(line, "redis_version:"); ok {
			parts := strings.SplitN(strings.TrimSpace(v), ".", 3)
			if len(parts) < 2 {
				break
			}
			gotMajor, _ := strconv.Atoi(parts[0])
			gotMinor, _ := strconv.Atoi(parts[1])
			if gotMajor > major || (gotMajor == major && gotMinor >= minor) {
				return
			}
			t.Skipf("requires Redis >= %d.%d (server is %s): %s", major, minor, strings.TrimSpace(v), reason)
		}
	}
	t.Fatalf("redis_version not found in INFO server")
}

// eventually polls cond until it returns true or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitSubscribers waits until channel has want subscribers server-side.
// Subscribe writes are fire-and-forget, so this is the synchronization
// point tests use before publishing.
func waitSubscribers(t *testing.T, client *redis.Client, channel string, want int64) {
	t.Helper()
	eventually(t, 5*time.Second, fmt.Sprintf("%d subscriber(s) on %q", want, channel), func() bool {
		nums, err := client.PubSubNumSub(ctx, channel).Result()
		return err == nil && nums[channel] == want
	})
}

// waitShardSubscribers is waitSubscribers for the sharded namespace.
func waitShardSubscribers(t *testing.T, client *redis.Client, channel string, want int64) {
	t.Helper()
	eventually(t, 5*time.Second, fmt.Sprintf("%d shard subscriber(s) on %q", want, channel), func() bool {
		nums, err := client.PubSubShardNumSub(ctx, channel).Result()
		return err == nil && nums[channel] == want
	})
}

// recvMessage receives one message with a timeout.
func recvMessage(t *testing.T, ch <-chan *redis.Message, timeout time.Duration) *redis.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("message channel closed while waiting for a message")
		}
		return msg
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

// expectClosed drains ch until it closes or the timeout expires.
func expectClosed(t *testing.T, ch <-chan *redis.Message, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the message channel to close")
		}
	}
}

func TestPubSubPublishReceive(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "ps:ch1", "ps:ch2")
	defer pubsub.Close()
	ch := pubsub.Channel()

	waitSubscribers(t, client, "ps:ch1", 1)
	waitSubscribers(t, client, "ps:ch2", 1)

	if n, err := client.Publish(ctx, "ps:ch1", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("Publish ps:ch1 = (%d, %v), want (1, nil)", n, err)
	}
	if n, err := client.Publish(ctx, "ps:ch2", "hello2").Result(); err != nil || n != 1 {
		t.Fatalf("Publish ps:ch2 = (%d, %v), want (1, nil)", n, err)
	}

	msg := recvMessage(t, ch, 5*time.Second)
	if msg.Channel != "ps:ch1" || msg.Payload != "hello" {
		t.Fatalf("got %q on %q, want \"hello\" on \"ps:ch1\"", msg.Payload, msg.Channel)
	}
	msg = recvMessage(t, ch, 5*time.Second)
	if msg.Channel != "ps:ch2" || msg.Payload != "hello2" {
		t.Fatalf("got %q on %q, want \"hello2\" on \"ps:ch2\"", msg.Payload, msg.Channel)
	}

	// Unsubscribing from everything leaves the delivery channel open —
	// only Close ends it — and drains the server-side subscriptions.
	if err := pubsub.Unsubscribe(ctx, "ps:ch1", "ps:ch2"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitSubscribers(t, client, "ps:ch1", 0)

	if err := pubsub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectClosed(t, ch, 5*time.Second)
}

// TestPubSubReceive exercises the synchronous Receive API: the first
// Receive returns the subscribe confirmation, ReceiveMessage skips
// non-message events, and ReceiveTimeout surfaces a net.Error timeout.
func TestPubSubReceive(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psr:ch")
	defer pubsub.Close()

	ev, err := pubsub.ReceiveTimeout(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("ReceiveTimeout: %v", err)
	}
	sub, ok := ev.(*redis.Subscription)
	if !ok || sub.Kind != "subscribe" || sub.Channel != "psr:ch" || sub.Count != 1 {
		t.Fatalf("first event = %#v, want the subscribe confirmation", ev)
	}

	// Nothing else pending: the timeout error implements net.Error.
	if _, err := pubsub.ReceiveTimeout(ctx, 50*time.Millisecond); err == nil {
		t.Fatal("ReceiveTimeout on an idle subscription succeeded, want timeout")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("ReceiveTimeout error = %v, want a net.Error timeout", err)
	}

	if err := client.Publish(ctx, "psr:ch", "hello").Err(); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if msg.Channel != "psr:ch" || msg.Payload != "hello" {
		t.Fatalf("got %q on %q, want \"hello\" on \"psr:ch\"", msg.Payload, msg.Channel)
	}
}

func TestPubSubPatternMatching(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.PSubscribe(ctx, "psp:*")
	defer pubsub.Close()
	ch := pubsub.Channel()

	eventually(t, 5*time.Second, "pattern registered", func() bool {
		n, err := client.PubSubNumPat(ctx).Result()
		return err == nil && n >= 1
	})

	if n, err := client.Publish(ctx, "psp:one", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("Publish = (%d, %v), want (1, nil)", n, err)
	}

	msg := recvMessage(t, ch, 5*time.Second)
	if msg.Channel != "psp:one" || msg.Pattern != "psp:*" || msg.Payload != "hello" {
		t.Fatalf("got %+v, want channel=psp:one pattern=psp:* payload=hello", msg)
	}

	if err := pubsub.PUnsubscribe(ctx, "psp:*"); err != nil {
		t.Fatalf("PUnsubscribe: %v", err)
	}
	eventually(t, 5*time.Second, "pattern deregistered", func() bool {
		n, err := client.PubSubNumPat(ctx).Result()
		return err == nil && n == 0
	})

	if err := pubsub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectClosed(t, ch, 5*time.Second)
}

func TestPubSubSharded(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.SSubscribe(ctx, "pss:ch1", "pss:ch2")
	defer pubsub.Close()
	ch := pubsub.Channel()

	waitShardSubscribers(t, client, "pss:ch1", 1)
	waitShardSubscribers(t, client, "pss:ch2", 1)

	if n, err := client.SPublish(ctx, "pss:ch1", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("SPublish = (%d, %v), want (1, nil)", n, err)
	}
	msg := recvMessage(t, ch, 5*time.Second)
	if msg.Channel != "pss:ch1" || msg.Payload != "hello" {
		t.Fatalf("got %q on %q, want \"hello\" on \"pss:ch1\"", msg.Payload, msg.Channel)
	}

	// Sharded and regular namespaces are separate: a regular PUBLISH to
	// the shard channel's name must not reach the shard subscriber.
	if n, err := client.Publish(ctx, "pss:ch1", "wrong-namespace").Result(); err != nil || n != 0 {
		t.Fatalf("Publish to shard-only channel = (%d, %v), want (0, nil)", n, err)
	}

	if err := pubsub.SUnsubscribe(ctx); err != nil {
		t.Fatalf("SUnsubscribe: %v", err)
	}
	waitShardSubscribers(t, client, "pss:ch1", 0)

	if err := pubsub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectClosed(t, ch, 5*time.Second)
}

// TestPubSubSubscriptionEvents pins that subscription confirmations are
// delivered on ChannelWithSubscriptions — including for the initial
// Subscribe: the wrapper binds the delivery stream before the first
// subscribe command is written.
func TestPubSubSubscriptionEvents(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "pse:first")
	defer pubsub.Close()
	events := pubsub.ChannelWithSubscriptions()

	waitConfirm := func(channel string) {
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatal("event channel closed while waiting for the confirmation")
				}
				if sub, ok := ev.(*redis.Subscription); ok && sub.Channel == channel {
					if sub.Kind != "subscribe" {
						t.Fatalf("confirmation kind = %q, want \"subscribe\"", sub.Kind)
					}
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for the %s confirmation", channel)
			}
		}
	}

	waitConfirm("pse:first")

	if err := pubsub.Subscribe(ctx, "pse:second"); err != nil {
		t.Fatalf("Subscribe second: %v", err)
	}
	waitConfirm("pse:second")
}

// TestPubSubEmptySubscription pins the empty-subscription flow:
// Subscribe with no channels returns a usable PubSub that subscribes
// later.
func TestPubSubEmptySubscription(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx)
	defer pubsub.Close()
	ch := pubsub.Channel()

	if err := pubsub.Subscribe(ctx, "psempty:ch"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitSubscribers(t, client, "psempty:ch", 1)

	if err := client.Publish(ctx, "psempty:ch", "hello").Err(); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if msg := recvMessage(t, ch, 5*time.Second); msg.Payload != "hello" {
		t.Fatalf("got %q, want \"hello\"", msg.Payload)
	}
}

func TestPubSubAddChannelsToHandle(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psa:first")
	defer pubsub.Close()
	ch := pubsub.Channel()

	if err := pubsub.Subscribe(ctx, "psa:second"); err != nil {
		t.Fatalf("Subscribe second: %v", err)
	}
	waitSubscribers(t, client, "psa:second", 1)

	if err := client.Publish(ctx, "psa:second", "hello").Err(); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := recvMessage(t, ch, 5*time.Second)
	if msg.Channel != "psa:second" || msg.Payload != "hello" {
		t.Fatalf("got %q on %q, want \"hello\" on \"psa:second\"", msg.Payload, msg.Channel)
	}
}

func TestPubSubChannelClosedOnClose(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psc:ch")
	ch := pubsub.Channel()

	if err := pubsub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectClosed(t, ch, 5*time.Second)

	// A closed PubSub cannot be revived.
	if err := pubsub.Subscribe(ctx, "psc:other"); err == nil {
		t.Fatal("Subscribe on a closed PubSub succeeded, want error")
	}
	if err := pubsub.Close(); err == nil {
		t.Fatal("second Close succeeded, want error")
	}
}

func TestPubSubChannelClosedOnClientClose(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "pscc:ch")
	ch := pubsub.Channel()

	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	expectClosed(t, ch, 5*time.Second)
}

func TestPubSubPing(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psping:ch")
	defer pubsub.Close()

	// Drain the subscribe confirmation, then the pong surfaces through
	// Receive.
	if _, err := pubsub.ReceiveTimeout(ctx, 5*time.Second); err != nil {
		t.Fatalf("ReceiveTimeout (confirmation): %v", err)
	}

	if err := pubsub.Ping(ctx, "hello"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	ev, err := pubsub.ReceiveTimeout(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("ReceiveTimeout (pong): %v", err)
	}
	pong, ok := ev.(*redis.Pong)
	if !ok || pong.Payload != "hello" {
		t.Fatalf("event = %#v, want *Pong with payload \"hello\"", ev)
	}
}

func TestPubSubConcurrentPingAndReceive(t *testing.T) {
	const n = 100

	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "pscp:ch")
	defer pubsub.Close()
	ch := pubsub.Channel()

	waitSubscribers(t, client, "pscp:ch", 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if err := pubsub.Ping(ctx); err != nil {
				t.Errorf("Ping %d: %v", i, err)
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		if err := client.Publish(ctx, "pscp:ch", strconv.Itoa(i)).Err(); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		msg := recvMessage(t, ch, 5*time.Second)
		if msg.Payload != strconv.Itoa(i) {
			t.Fatalf("message %d out of order: got %q", i, msg.Payload)
		}
	}
	wg.Wait()
}

func TestPubSubBigMessagePayload(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psbig:ch")
	defer pubsub.Close()
	ch := pubsub.Channel()

	waitSubscribers(t, client, "psbig:ch", 1)

	// Larger than the connection read buffer (32 KiB default).
	bigVal := strings.Repeat("x", 1<<17)
	if err := client.Publish(ctx, "psbig:ch", bigVal).Err(); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recvMessage(t, ch, 10*time.Second)
	if msg.Payload != bigVal {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(msg.Payload), len(bigVal))
	}
}

func TestPubSubChannelSizeOption(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psopt:ch")
	defer pubsub.Close()

	if got := cap(pubsub.Channel(redis.WithChannelSize(7))); got != 7 {
		t.Fatalf("Channel buffer = %d, want 7 (WithChannelSize)", got)
	}
}

// TestPubSubSlowConsumerDrops pins the slow-consumer policy: when the
// consumer stops reading, backpressure fills the engine's delivery
// buffer, overflow is dropped there, and the stream keeps flowing once
// the consumer catches up.
func TestPubSubSlowConsumerDrops(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psslow:ch")
	defer pubsub.Close()
	ch := pubsub.Channel(redis.WithChannelSize(1))

	waitSubscribers(t, client, "psslow:ch", 1)

	// Publish more than fits in the 1-slot buffer while not consuming;
	// the pump drops what it cannot hand over within the send timeout.
	for i := 0; i < 5; i++ {
		if err := client.Publish(ctx, "psslow:ch", "burst").Err(); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Keep draining and republishing a marker until one gets through —
	// markers published while the buffer is full are dropped too.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delivery after drops")
		}
		if err := client.Publish(ctx, "psslow:ch", "after-drop").Err(); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case msg := <-ch:
			if msg.Payload == "after-drop" {
				return
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func TestPubSubIntrospectionCommands(t *testing.T) {
	client := newTestClient(t)

	pubsub := client.Subscribe(ctx, "psint:ch1", "psint:ch2")
	defer pubsub.Close()
	waitSubscribers(t, client, "psint:ch1", 1)
	waitSubscribers(t, client, "psint:ch2", 1)

	channels, err := client.PubSubChannels(ctx, "psint:*").Result()
	if err != nil {
		t.Fatalf("PubSubChannels: %v", err)
	}
	if len(channels) != 2 {
		t.Fatalf("PubSubChannels = %v, want the two subscribed channels", channels)
	}

	nums, err := client.PubSubNumSub(ctx, "psint:ch1", "psint:ch2", "psint:none").Result()
	if err != nil {
		t.Fatalf("PubSubNumSub: %v", err)
	}
	if nums["psint:ch1"] != 1 || nums["psint:ch2"] != 1 || nums["psint:none"] != 0 {
		t.Fatalf("PubSubNumSub = %v, want {psint:ch1:1 psint:ch2:1 psint:none:0}", nums)
	}

	spubsub := client.SSubscribe(ctx, "psint:sch")
	defer spubsub.Close()
	waitShardSubscribers(t, client, "psint:sch", 1)

	schannels, err := client.PubSubShardChannels(ctx, "psint:*").Result()
	if err != nil {
		t.Fatalf("PubSubShardChannels: %v", err)
	}
	if len(schannels) != 1 || schannels[0] != "psint:sch" {
		t.Fatalf("PubSubShardChannels = %v, want [psint:sch]", schannels)
	}
}

// TestPubSubSubkeyNotifications pins delivery of hash-field subkey
// keyspace notifications (Redis 8.8+) through the pub/sub engine.
func TestPubSubSubkeyNotifications(t *testing.T) {
	if os.Getenv("RE_CLUSTER") == "true" {
		t.Skip("keyspace notification config not available on Redis Enterprise")
	}
	client := newTestClient(t)
	skipBeforeVersion(t, client, 8, 8, "subkeyspace notifications")

	prev, err := client.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		t.Fatalf("ConfigGet: %v", err)
	}
	if err := client.ConfigSet(ctx, "notify-keyspace-events", "STh").Err(); err != nil {
		t.Fatalf("ConfigSet: %v", err)
	}
	t.Cleanup(func() {
		_ = client.ConfigSet(ctx, "notify-keyspace-events", prev["notify-keyspace-events"]).Err()
	})

	const (
		hashKey         = "skn:hash"
		field           = "field-alpha"
		hexpireChannel  = "__subkeyevent@0__:hexpire"
		expiredChannel  = "__subkeyevent@0__:hexpired"
		expectedPayload = "8:skn:hash|11:field-alpha"
	)
	if err := client.Del(ctx, hashKey).Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}

	pubsub := client.Subscribe(ctx, hexpireChannel, expiredChannel)
	defer pubsub.Close()
	ch := pubsub.Channel()

	waitSubscribers(t, client, hexpireChannel, 1)
	waitSubscribers(t, client, expiredChannel, 1)

	if err := client.HSet(ctx, hashKey, field, "value").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	res, err := client.HPExpire(ctx, hashKey, 50*time.Millisecond, field).Result()
	if err != nil {
		t.Fatalf("HPExpire: %v", err)
	}
	if len(res) != 1 || res[0] != 1 {
		t.Fatalf("HPExpire = %v, want [1]", res)
	}

	seen := make(map[string]string)
	deadline := time.After(10 * time.Second)
	for seen[hexpireChannel] == "" || seen[expiredChannel] == "" {
		select {
		case msg := <-ch:
			if msg.Channel != hexpireChannel && msg.Channel != expiredChannel {
				continue
			}
			if msg.Payload != expectedPayload {
				t.Fatalf("payload on %s = %q, want %q", msg.Channel, msg.Payload, expectedPayload)
			}
			seen[msg.Channel] = msg.Payload
		case <-deadline:
			t.Fatalf("timed out; seen: %v", seen)
		}
	}
}
