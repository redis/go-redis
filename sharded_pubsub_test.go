package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9/internal/hashtag"
)

func TestShardedPubSubChannelGrouping(t *testing.T) {
	// Verify that channels hash to different slots (the core issue).
	// In the bug report, ch1-ch9 hash to different slots, but only
	// messages from one shard's channels are received.
	slots := make(map[int][]string)
	for i := 1; i <= 9; i++ {
		ch := "ch" + string(rune('0'+i))
		slot := hashtag.Slot(ch)
		slots[slot] = append(slots[slot], ch)
	}

	// There should be multiple different slots for ch1-ch9.
	if len(slots) <= 1 {
		t.Fatalf("expected channels to hash to multiple slots, got %d", len(slots))
	}
	t.Logf("channels hash to %d different slots: %v", len(slots), slots)
}

func TestShardedPubSubNewAndClose(t *testing.T) {
	// Test that ShardedPubSub can be created and closed without panics.
	// We can't test actual cluster connectivity without a running cluster,
	// but we can verify the struct initializes correctly.
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}

	sps := newShardedPubSub(cluster)
	if sps == nil {
		t.Fatal("newShardedPubSub returned nil")
	}
	if sps.closed {
		t.Fatal("new ShardedPubSub should not be closed")
	}
	if len(sps.shards) != 0 {
		t.Fatalf("expected 0 shards, got %d", len(sps.shards))
	}

	// Close should work on empty ShardedPubSub.
	err := sps.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Double close should return ErrClosed.
	err = sps.Close()
	if err == nil {
		t.Fatal("expected error on double close")
	}
}

func TestShardedPubSubSSubscribeWhenClosed(t *testing.T) {
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}
	sps := newShardedPubSub(cluster)
	_ = sps.Close()

	err := sps.SSubscribe(context.Background(), "ch1")
	if err == nil {
		t.Fatal("expected error when subscribing on closed ShardedPubSub")
	}
}
