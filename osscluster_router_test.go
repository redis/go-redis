package redis

// import (
// 	"context"
// 	"sync"
// 	"testing"
// 	"time"

// 	. "github.com/bsm/ginkgo/v2"
// 	. "github.com/bsm/gomega"

// 	"github.com/redis/go-redis/v9/internal/routing"
// )

// var _ = Describe("ExtractCommandValue", func() {
// 	It("should extract value from generic command", func() {
// 		cmd := NewCmd(nil, "test")
// 		cmd.SetVal("value")
// 		val := routing.ExtractCommandValue(cmd)
// 		Expect(val).To(Equal("value"))
// 	})

// 	It("should extract value from integer command", func() {
// 		intCmd := NewIntCmd(nil, "test")
// 		intCmd.SetVal(42)
// 		val := routing.ExtractCommandValue(intCmd)
// 		Expect(val).To(Equal(int64(42)))
// 	})

// 	It("should handle nil command", func() {
// 		val := routing.ExtractCommandValue(nil)
// 		Expect(val).To(BeNil())
// 	})
// })

// var _ = Describe("ClusterClient setCommandValue", func() {
// 	var client *ClusterClient

// 	BeforeEach(func() {
// 		client = &ClusterClient{}
// 	})

// 	It("should set generic value", func() {
// 		cmd := NewCmd(nil, "test")
// 		err := client.setCommandValue(cmd, "new_value")
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(cmd.Val()).To(Equal("new_value"))
// 	})

// 	It("should set integer value", func() {
// 		intCmd := NewIntCmd(nil, "test")
// 		err := client.setCommandValue(intCmd, int64(100))
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(intCmd.Val()).To(Equal(int64(100)))
// 	})

// 	It("should return error for type mismatch", func() {
// 		intCmd := NewIntCmd(nil, "test")
// 		err := client.setCommandValue(intCmd, "string_value")
// 		Expect(err).To(HaveOccurred())
// 		Expect(err.Error()).To(ContainSubstring("cannot set IntCmd value from string"))
// 	})
// })

// func TestConcurrentRouting(t *testing.T) {
// 	// This test ensures that concurrent execution doesn't cause response mismatches
// 	// or MOVED errors due to race conditions

// 	// Mock cluster client for testing
// 	opt := &ClusterOptions{
// 		Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
// 	}

// 	// Skip if no cluster available
// 	if testing.Short() {
// 		t.Skip("skipping cluster test in short mode")
// 	}

// 	client := NewClusterClient(opt)
// 	defer client.Close()

// 	// Test concurrent execution of commands with different policies
// 	var wg sync.WaitGroup
// 	numRoutines := 50
// 	numCommands := 100

// 	// Channel to collect errors
// 	errors := make(chan error, numRoutines*numCommands)

// 	for i := 0; i < numRoutines; i++ {
// 		wg.Add(1)
// 		go func(routineID int) {
// 			defer wg.Done()

// 			for j := 0; j < numCommands; j++ {
// 				ctx := context.Background()

// 				// Test different command types
// 				switch j % 4 {
// 				case 0:
// 					// Test keyless command (should use arbitrary shard)
// 					cmd := NewCmd(ctx, "PING")
// 					err := client.routeAndRun(ctx, cmd)
// 					if err != nil {
// 						errors <- err
// 					}
// 				case 1:
// 					// Test keyed command (should use slot-based routing)
// 					key := "test_key_" + string(rune(routineID)) + "_" + string(rune(j))
// 					cmd := NewCmd(ctx, "GET", key)
// 					err := client.routeAndRun(ctx, cmd)
// 					if err != nil {
// 						errors <- err
// 					}
// 				case 2:
// 					// Test multi-shard command
// 					cmd := NewCmd(ctx, "MGET", "key1", "key2", "key3")
// 					err := client.routeAndRun(ctx, cmd)
// 					if err != nil {
// 						errors <- err
// 					}
// 				case 3:
// 					// Test all-shards command
// 					cmd := NewCmd(ctx, "DBSIZE")
// 					// Note: In actual implementation, the policy would come from COMMAND tips
// 					err := client.routeAndRun(ctx, cmd)
// 					if err != nil {
// 						errors <- err
// 					}
// 				}
// 			}
// 		}(i)
// 	}

// 	// Wait for all routines to complete
// 	wg.Wait()
// 	close(errors)

// 	// Check for errors
// 	var errorCount int
// 	for err := range errors {
// 		t.Errorf("Concurrent routing error: %v", err)
// 		errorCount++
// 		if errorCount > 10 { // Limit error output
// 			break
// 		}
// 	}

// 	if errorCount > 0 {
// 		t.Fatalf("Found %d errors in concurrent routing test", errorCount)
// 	}
// }

// func TestResponseAggregation(t *testing.T) {
// 	// Test that response aggregation works correctly for different policies

// 	if testing.Short() {
// 		t.Skip("skipping cluster test in short mode")
// 	}

// 	// Test all_succeeded aggregation
// 	t.Run("AllSucceeded", func(t *testing.T) {
// 		aggregator := routing.NewResponseAggregator(routing.RespAllSucceeded, "TEST")

// 		// Add successful results
// 		err := aggregator.Add("result1", nil)
// 		if err != nil {
// 			t.Errorf("Failed to add result: %v", err)
// 		}

// 		err = aggregator.Add("result2", nil)
// 		if err != nil {
// 			t.Errorf("Failed to add result: %v", err)
// 		}

// 		result, err := aggregator.Finish()
// 		if err != nil {
// 			t.Errorf("AllSucceeded aggregation failed: %v", err)
// 		}

// 		if result != "result1" {
// 			t.Errorf("Expected 'result1', got %v", result)
// 		}
// 	})

// 	// Test agg_sum aggregation
// 	t.Run("AggSum", func(t *testing.T) {
// 		aggregator := routing.NewResponseAggregator(routing.RespAggSum, "TEST")

// 		// Add numeric results
// 		err := aggregator.Add(int64(5), nil)
// 		if err != nil {
// 			t.Errorf("Failed to add result: %v", err)
// 		}

// 		err = aggregator.Add(int64(10), nil)
// 		if err != nil {
// 			t.Errorf("Failed to add result: %v", err)
// 		}

// 		result, err := aggregator.Finish()
// 		if err != nil {
// 			t.Errorf("AggSum aggregation failed: %v", err)
// 		}

// 		if result != int64(15) {
// 			t.Errorf("Expected 15, got %v", result)
// 		}
// 	})

// 	// Test special aggregation for search commands
// 	t.Run("Special", func(t *testing.T) {
// 		aggregator := routing.NewResponseAggregator(routing.RespSpecial, "FT.SEARCH")

// 		// Add search results
// 		searchResult := map[string]interface{}{
// 			"total": 5,
// 			"docs":  []interface{}{"doc1", "doc2"},
// 		}

// 		err := aggregator.Add(searchResult, nil)
// 		if err != nil {
// 			t.Errorf("Failed to add result: %v", err)
// 		}

// 		result, err := aggregator.Finish()
// 		if err != nil {
// 			t.Errorf("Special aggregation failed: %v", err)
// 		}

// 		if result == nil {
// 			t.Error("Expected non-nil result from special aggregation")
// 		}
// 	})
// }

// func TestShardPicking(t *testing.T) {
// 	// Test that arbitrary shard picking works correctly and doesn't always pick the first shard

// 	opt := &ClusterOptions{
// 		Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
// 	}

// 	if testing.Short() {
// 		t.Skip("skipping cluster test in short mode")
// 	}

// 	client := NewClusterClient(opt)
// 	defer client.Close()

// 	ctx := context.Background()

// 	// Track which shards are picked
// 	shardCounts := make(map[string]int)
// 	var mu sync.Mutex

// 	// Execute keyless commands multiple times
// 	var wg sync.WaitGroup
// 	numRequests := 100

// 	for i := 0; i < numRequests; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()

// 			node := client.pickArbitraryShard(ctx)
// 			if node != nil {
// 				addr := node.Client.Options().Addr
// 				mu.Lock()
// 				shardCounts[addr]++
// 				mu.Unlock()
// 			}
// 		}()
// 	}

// 	wg.Wait()

// 	// Verify that multiple shards were used (not just the first one)
// 	if len(shardCounts) < 2 {
// 		t.Error("Shard picking should distribute across multiple shards")
// 	}

// 	// Verify reasonable distribution (no shard should have more than 80% of requests)
// 	for addr, count := range shardCounts {
// 		percentage := float64(count) / float64(numRequests) * 100
// 		if percentage > 80 {
// 			t.Errorf("Shard %s got %d%% of requests, distribution should be more even", addr, int(percentage))
// 		}
// 		t.Logf("Shard %s: %d requests (%.1f%%)", addr, count, percentage)
// 	}
// }

// func TestCursorRouting(t *testing.T) {
// 	// Test that cursor commands are routed to the correct shard

// 	opt := &ClusterOptions{
// 		Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
// 	}

// 	if testing.Short() {
// 		t.Skip("skipping cluster test in short mode")
// 	}

// 	client := NewClusterClient(opt)
// 	defer client.Close()

// 	ctx := context.Background()

// 	// Test FT.CURSOR command routing
// 	cmd := NewCmd(ctx, "FT.CURSOR", "READ", "myindex", "cursor123", "COUNT", "10")

// 	// This should not panic or return an error due to incorrect routing
// 	err := client.executeSpecial(ctx, cmd, &routing.CommandPolicy{
// 		Request:  routing.ReqSpecial,
// 		Response: routing.RespSpecial,
// 	})

// 	// We expect this to fail with connection error in test environment, but not with routing error
// 	if err != nil && err.Error() != "redis: connection refused" {
// 		t.Logf("Cursor routing test completed with expected connection error: %v", err)
// 	}
// }

// // Mock command methods for testing
// type testCmd struct {
// 	*Cmd
// 	requestPolicy  routing.RequestPolicy
// 	responsePolicy routing.ResponsePolicy
// }

// func (c *testCmd) setRequestPolicy(policy routing.RequestPolicy) {
// 	c.requestPolicy = policy
// }

// func (c *testCmd) setResponsePolicy(policy routing.ResponsePolicy) {
// 	c.responsePolicy = policy
// }

// func TestRaceConditionFree(t *testing.T) {
// 	// Test to ensure no race conditions in concurrent access

// 	opt := &ClusterOptions{
// 		Addrs: []string{"127.0.0.1:7000"},
// 	}

// 	if testing.Short() {
// 		t.Skip("skipping cluster test in short mode")
// 	}

// 	client := NewClusterClient(opt)
// 	defer client.Close()

// 	// Run with race detector enabled: go test -race
// 	var wg sync.WaitGroup
// 	numGoroutines := 100

// 	for i := 0; i < numGoroutines; i++ {
// 		wg.Add(1)
// 		go func(id int) {
// 			defer wg.Done()

// 			ctx := context.Background()

// 			// Simulate concurrent command execution
// 			for j := 0; j < 10; j++ {
// 				cmd := NewCmd(ctx, "PING")
// 				_ = client.routeAndRun(ctx, cmd)

// 				// Small delay to increase chance of race conditions
// 				time.Sleep(time.Microsecond)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()

// 	// If we reach here without race detector complaints, test passes
// 	t.Log("Race condition test completed successfully")
// }
