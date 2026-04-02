package hashtag

import (
	"strconv"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("RendezvousHash", func() {
	It("should return empty string for empty nodes", func() {
		h := NewRendezvousHash(nil)
		Expect(h.Get("any")).To(Equal(""))
	})

	It("should return the single node for all keys", func() {
		h := NewRendezvousHash([]string{"only"})

		for i := 0; i < 100; i++ {
			Expect(h.Get("key-" + strconv.Itoa(i))).To(Equal("only"))
		}
	})

	It("should be deterministic with multiple nodes", func() {
		nodes := []string{"node-0", "node-1", "node-2"}
		h := NewRendezvousHash(nodes)

		// Same key should always map to the same node
		expected := h.Get("consistent-key")
		for i := 0; i < 10; i++ {
			Expect(h.Get("consistent-key")).To(Equal(expected))
		}

		// Different keys may map to different nodes
		results := make(map[string]int)
		for i := 0; i < 100; i++ {
			node := h.Get("key-" + strconv.Itoa(i))
			Expect(node).To(BeElementOf(nodes))
			results[node]++
		}

		// Verify distribution is roughly even (allowing some variance)
		// With 100 keys and 3 nodes, expect roughly 33 keys per node.
		// Check that each node receives more than 25 keys to verify fairly even distribution.
		for _, node := range nodes {
			count := results[node]
			Expect(count).To(BeNumerically(">", 25),
				"node %s should receive more than 25 keys (got %d)", node, count)
		}
	})

	It("should be consistent within a single instance (per-instance determinism)", func() {
		// This test verifies per-instance determinism: the same RendezvousHash instance
		// returns consistent results for the same key across multiple calls.
		// Note: Node order in the input may affect results due to first-match tie-breaking
		// when multiple nodes produce equal scores (uses > not >= in score comparison).
		nodes := []string{"node-a", "node-b", "node-c"}
		h := NewRendezvousHash(nodes)

		testKeys := []string{"key-1", "key-2", "key-3", "test", "foo", "bar"}

		for _, testKey := range testKeys {
			// Same instance should consistently return the same node for the same key
			result1 := h.Get(testKey)
			result2 := h.Get(testKey)
			result3 := h.Get(testKey)

			Expect(result1).To(Equal(result2),
				"key %q should return same node on repeated calls", testKey)
			Expect(result2).To(Equal(result3),
				"key %q should return same node on repeated calls", testKey)
			Expect(result1).To(BeElementOf(nodes))
		}
	})

	It("should distribute keys across multiple nodes", func() {
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
			Expect(distribution[node]).To(BeNumerically(">", 0),
				"node %q should receive at least one key", node)
		}

		// Distribution should be reasonably balanced.
		// With 100k keys across 5 nodes, we expect roughly 20% per node.
		// Use a loose 10% tolerance to account for natural statistical variance
		// while still verifying the hash distributes fairly uniformly.
		expectedPerNode := float64(numKeys) / float64(len(nodes))
		tolerance := expectedPerNode * 0.1

		for _, node := range nodes {
			count := float64(distribution[node])
			Expect(count).To(BeNumerically("~", expectedPerNode, tolerance),
				"node %q distribution should be reasonably balanced", node)
		}
	})

	It("should handle arbitrary string names as nodes", func() {
		nodes := []string{
			"redis-cluster-node-1.example.com:6379",
			"redis-cluster-node-2.example.com:6379",
			"redis-cluster-node-3.example.com:6379",
		}
		h := NewRendezvousHash(nodes)

		// Verify all keys map to valid nodes
		for i := 0; i < 50; i++ {
			result := h.Get("key-" + strconv.Itoa(i))
			Expect(result).To(BeElementOf(nodes))
		}

		// Verify consistency
		key := "test-key"
		firstResult := h.Get(key)
		Expect(h.Get(key)).To(Equal(firstResult))
	})

	It("should be stable when nodes change minimally", func() {
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
			newNode := h2.Get(key)

			Expect(newNode).To(BeElementOf(nodesReduced))

			if newNode != initialMapping[key] {
				moved++
			} else {
				stayed++
			}
		}

		// Some keys should stay, some should move to other available nodes
		Expect(moved).To(BeNumerically(">", 0), "some keys should move when node is removed")
		Expect(stayed).To(BeNumerically(">", 0), "some keys should stay with same node")
	})
})
