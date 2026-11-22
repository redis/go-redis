package redis

import (
	"context"
	"errors"
	"testing"
)

func TestShardSlotGroups(t *testing.T) {
	// Without slot info: one group with everything.
	c := &PubSub{}
	groups := c.shardSlotGroups([]string{"a", "b"})
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("expected one group of 2, got %v", groups)
	}
	if got := c.shardSlotGroups(nil); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}

	// With slot info (cluster): channels split by slot.
	c.slotOf = func(ch string) int { return int(ch[0]) }
	groups = c.shardSlotGroups([]string{"aa", "ab", "bc"})
	if len(groups) != 2 {
		t.Fatalf("expected 2 slot groups, got %d: %v", len(groups), groups)
	}
	for _, g := range groups {
		slot := int(g[0][0])
		for _, ch := range g {
			if int(ch[0]) != slot {
				t.Fatalf("group %v mixes slots", g)
			}
		}
	}
}

func TestRefreshSubKeysLockedKeepsCacheRetryableOnError(t *testing.T) {
	ctx := context.Background()
	fail := true
	c := &PubSub{
		connKey: func(_ context.Context, ch string) (string, error) {
			if fail && ch == "bad" {
				return "", errors.New("topology unavailable")
			}
			return "key:" + ch, nil
		},
	}
	c.ensureMuxLocked()
	c.schannels = map[string]struct{}{"a": {}, "bad": {}}

	// A failed connKey lookup must NOT install a partial/empty mapping: the
	// cache stays nil so the next Receive/Ping retries the refresh instead
	// of hanging with no readers.
	if err := c.refreshSubKeysLocked(ctx); err == nil {
		t.Fatal("expected refresh error")
	}
	if c.subKeys != nil {
		t.Fatalf("partial subKeys installed despite error: %v", c.subKeys)
	}

	// Once topology recovers, the refresh succeeds and installs the full map.
	fail = false
	if err := c.refreshSubKeysLocked(ctx); err != nil {
		t.Fatalf("unexpected error after recovery: %v", err)
	}
	if len(c.subKeys) != 2 || len(c.subKeys["key:a"]) != 1 || len(c.subKeys["key:bad"]) != 1 {
		t.Fatalf("unexpected subKeys: %v", c.subKeys)
	}

	// missingKeysLocked must propagate the refresh error (Receive reports it
	// instead of hanging) and report the shard keys that need dialing.
	fail = true
	c.subKeys = nil
	c.conns[""] = nil
	delete(c.conns, "")
	if _, err := c.missingKeysLocked(ctx); err == nil {
		t.Fatal("expected missingKeysLocked to propagate the refresh error")
	}
	fail = false
	need, err := c.missingKeysLocked(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(need) != 2 {
		t.Fatalf("expected the two shard keys to dial, got %v", need)
	}
	if _, ok := need["key:a"]; !ok {
		t.Fatalf("key:a not in need: %v", need)
	}
	if _, ok := need["key:bad"]; !ok {
		t.Fatalf("key:bad not in need: %v", need)
	}
	if false {
		t.Fatalf("expected the two shard keys to dial, got %v", need)
	}
}
