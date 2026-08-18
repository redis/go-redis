package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRingSubscribeEmptyChannelsNoPanic(t *testing.T) {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"shard1": "localhost:6379"},
	})
	defer ring.Close()

	pubsub := ring.Subscribe(context.Background())
	if pubsub == nil {
		t.Fatal("expected non-nil PubSub")
	}
	// Receive should surface sticky error, not panic.
	_, err := pubsub.Receive(context.Background())
	if err == nil {
		t.Fatal("expected sticky error from empty Subscribe")
	}
	_ = pubsub.Close()
}

func TestPubSubChannelMutualExclusionNoPanic(t *testing.T) {
	// Construct via a client that may not be reachable — Subscribe without
	// channels just builds a PubSub handle.
	t.Run("ChannelAfterChannelWithSubscriptions", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer client.Close()
		pubsub := client.Subscribe(context.Background())
		defer pubsub.Close()

		_ = pubsub.ChannelWithSubscriptions()
		ch := pubsub.Channel()
		// Conflicting call returns an already-closed channel (no panic).
		msg, ok := <-ch
		if ok || msg != nil {
			t.Fatalf("expected closed channel from conflicting Channel() call, got msg=%v ok=%v", msg, ok)
		}
	})

	t.Run("ChannelWithSubscriptionsAfterChannel", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer client.Close()
		pubsub := client.Subscribe(context.Background())
		defer pubsub.Close()

		_ = pubsub.Channel()
		ch := pubsub.ChannelWithSubscriptions()
		msg, ok := <-ch
		if ok || msg != nil {
			t.Fatalf("expected closed channel from conflicting ChannelWithSubscriptions() call, got msg=%v ok=%v", msg, ok)
		}
	})
}
