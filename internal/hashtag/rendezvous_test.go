package hashtag

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
)

func TestEmptyNode(t *testing.T) {
	t.Run("EmptyNode", func(t *testing.T) {
		h := NewRendezvousHash(nil)
		got := h.Get("any")
		if got != "" {
			t.Errorf("Get(%q) = %q, want empty string", "any", got)
		}
	})
}
func TestSingleNode(t *testing.T) {
	h := NewRendezvousHash([]string{"only"})
	for i := 0; i < 100; i++ {
		key := "key-" + strconv.Itoa(i)
		t.Run(key, func(t *testing.T) {
			got := h.Get(key)
			if got != "only" {
				t.Errorf("Get(%q) = %q, want %q", key, got, "only")
			}
		})
	}
}
func TestDeterministicMultipleNodes(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2"}
	h := NewRendezvousHash(nodes)

	// Same key should always map to the same node
	expected := h.Get("consistent-key")
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("consistent-key-%d", i), func(t *testing.T) {
			got := h.Get("consistent-key")
			if got != expected {
				t.Errorf(
					"Get(%q) = %q, want %q",
					"consistent-key", got, expected,
				)
			}
		})
	}

	// Different keys may map to different nodes
	results := make(map[string]int)
	for i := 0; i < 100; i++ {
		key := "key-" + strconv.Itoa(i)
		t.Run(key, func(t *testing.T) {
			node := h.Get(key)
			if !slices.Contains(nodes, node) {
				t.Errorf("Get(%q) = %q, want one of %v", key, node, nodes)
			}
			results[node]++
		})
	}

	// Verify distribution is roughly even (allowing some variance)
	// With 100 keys and 3 nodes, expect roughly 33 keys per node.
	// Check that each node receives more than 25 keys to verify fairly even distribution.
	for _, node := range nodes {
		t.Run(node, func(t *testing.T) {
			count := results[node]
			if count <= 25 {
				t.Errorf("node %s should receive more than 25 keys (got %d)", node, count)
			}
		})
	}
}
func TestPerInstanceDeterminism(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	h := NewRendezvousHash(nodes)

	testKeys := []string{"key-1", "key-2", "key-3", "test", "foo", "bar"}

	for _, testKey := range testKeys {
		t.Run(testKey, func(t *testing.T) {
			// Same instance should consistently return the same node for the same key
			result1 := h.Get(testKey)
			result2 := h.Get(testKey)
			result3 := h.Get(testKey)

			if result1 != result2 {
				t.Errorf("key %q should return same node on repeated calls. Get(%q): first result %q != second result %q",
					testKey, testKey, result1, result2)
			}
			if result2 != result3 {
				t.Errorf("key %q should return same node on repeated calls. Get(%q): Second result %q != Third result %q",
					testKey, testKey, result2, result3)
			}
			if !slices.Contains(nodes, result1) {
				t.Errorf(
					"Get(%q) = %q, want one of %v",
					testKey, result1, nodes,
				)
			}
		})
	}
}
func TestDistributeKeysAcrossNodes(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	h := NewRendezvousHash(nodes)

	distribution := make(map[string]int)
	numKeys := 100000

	for i := 0; i < numKeys; i++ {
		node := h.Get("key-" + strconv.Itoa(i))
		distribution[node]++
	}

	// All nodes should receive some keys
	for _, node := range nodes {
		t.Run(node, func(t *testing.T) {
			if distribution[node] <= 0 {
				t.Errorf("node %q should receive at least one key", node)
			}
		})
	}

	// Distribution should be reasonably balanced.
	// With 100k keys across 5 nodes, we expect roughly 20% per node.
	// Use a loose 10% tolerance to account for natural statistical variance
	// while still verifying the hash distributes fairly uniformly.
	expectedPerNode := float64(numKeys) / float64(len(nodes))
	tolerance := expectedPerNode * 0.1

	for _, node := range nodes {
		t.Run("Distribution_"+node, func(t *testing.T) {
			count := float64(distribution[node])
			if math.Abs(count-expectedPerNode) > tolerance {
				t.Errorf(
					"node %q distribution should be reasonably balanced: got %.0f, expected %.0f (tolerance %.0f)",
					node, count, expectedPerNode, tolerance,
				)
			}
		})
	}
}
func TestArbitraryNodeNames(t *testing.T) {
	nodes := []string{
		"redis-cluster-node-1.example.com:6379",
		"redis-cluster-node-2.example.com:6379",
		"redis-cluster-node-3.example.com:6379",
	}
	h := NewRendezvousHash(nodes)

	// Verify all keys map to valid nodes
	for i := 0; i < 50; i++ {
		key := "key-" + strconv.Itoa(i)
		t.Run(key, func(t *testing.T) {
			result := h.Get(key)
			if !slices.Contains(nodes, result) {
				t.Errorf("Get(%q) = %q, want one of %v", key, result, nodes)
			}
		})
	}

	// Verify consistency
	key := "test-key"
	t.Run(key, func(t *testing.T) {
		firstResult := h.Get(key)
		got := h.Get(key)
		if got != firstResult {
			t.Errorf(
				"Get(%q) = %q, want %q",
				key, got, firstResult,
			)
		}
	})
}
func TestStableWhenNodesChange(t *testing.T) {
	// When removing one node, keys should redistribute to other nodes
	nodes := []string{"node-0", "node-1", "node-2"}
	h1 := NewRendezvousHash(nodes)

	// Get initial distribution
	initialMapping := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := "key-" + strconv.Itoa(i)
		initialMapping[key] = h1.Get(key)
	}

	// Remove one node
	nodesReduced := []string{"node-0", "node-1"}
	h2 := NewRendezvousHash(nodesReduced)

	// Keys should still map to valid nodes
	moved := 0
	stayed := 0

	for i := 0; i < 100; i++ {
		key := "key-" + strconv.Itoa(i)
		t.Run(key, func(t *testing.T) {
			newNode := h2.Get(key)
			if !slices.Contains(nodesReduced, newNode) {
				t.Errorf("Get(%q) = %q, want one of %v",
					key, newNode, nodesReduced)
			}

			if newNode != initialMapping[key] {
				moved++
			} else {
				stayed++
			}
		})
	}

	// Some keys should stay, some should move to other available nodes

	if moved <= 0 {
		t.Errorf("some keys should move when node is removed")
	}

	if stayed <= 0 {
		t.Errorf("some keys should stay with same node")
	}
}
