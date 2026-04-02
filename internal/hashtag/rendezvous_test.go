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
		for _, node := range nodes {
			count := results[node]
			Expect(count).To(BeNumerically(">", 0), "node %s should receive at least one key", node)
		}
	})

	It("should handle node ordering consistently (tie-breaker semantics)", func() {
		// Test that node order doesn't affect determinism
		nodesA := []string{"node-a", "node-b", "node-c"}
		nodesB := []string{"node-c", "node-a", "node-b"}
		nodesC := []string{"node-b", "node-c", "node-a"}

		hA := NewRendezvousHash(nodesA)
		hB := NewRendezvousHash(nodesB)
		hC := NewRendezvousHash(nodesC)

		testKeys := []string{"key-1", "key-2", "key-3", "test", "foo", "bar"}

		for _, testKey := range testKeys {
			resultA := hA.Get(testKey)
			resultB := hB.Get(testKey)
			resultC := hC.Get(testKey)

			// HRW should map to the same logical node regardless of input order
			Expect(resultA).To(Equal(resultB),
				"key %q should map to same node regardless of node order", testKey)
			Expect(resultB).To(Equal(resultC),
				"key %q should map to same node regardless of node order", testKey)
		}
	})

	It("should distribute keys across multiple nodes", func() {
		nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
		h := NewRendezvousHash(nodes)

		distribution := make(map[string]int)
		numKeys := 1000

		for i := 0; i < numKeys; i++ {
			node := h.Get("key-" + strconv.Itoa(i))
			distribution[node]++
		}

		// All nodes should receive some keys
		for _, node := range nodes {
			Expect(distribution[node]).To(BeNumerically(">", 0),
				"node %q should receive at least one key", node)
		}

		// Distribution should be reasonably balanced
		// Each node should get roughly 20% of keys (10% tolerance)
		expectedPerNode := float64(numKeys) / float64(len(nodes))
		tolerance := expectedPerNode * 0.1

		for _, node := range nodes {
			count := float64(distribution[node])
			Expect(count).To(BeNumerically("~", expectedPerNode, tolerance),
				"node %q distribution should be balanced", node)
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
