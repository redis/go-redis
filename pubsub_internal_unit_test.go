package redis

import "testing"

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
