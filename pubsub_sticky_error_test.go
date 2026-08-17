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
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	pubsub := client.Subscribe(context.Background())
	defer pubsub.Close()

	_ = pubsub.ChannelWithSubscriptions()
	ch := pubsub.Channel()
	// Channel must return a closed/empty channel rather than panicking.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel from conflicting Channel() call")
		}
	default:
		// non-blocking closed channel may still be receivable; try again with receive
		_, ok := <-ch
		if ok {
			t.Fatal("expected closed channel")
		}
	}
}
