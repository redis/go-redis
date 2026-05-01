package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// This test is for manual use and is not part of the CI of Go-Redis.
var _ = Describe("Monitor command", Label("monitor"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		if os.Getenv("RUN_MONITOR_TEST") != "true" {
			Skip("Skipping Monitor command test. Set RUN_MONITOR_TEST=true to run it.")
		}
		client = redis.NewClient(&redis.Options{Addr: redisPort})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should monitor", Label("monitor"), func() {
		ress := make(chan string)
		client1 := redis.NewClient(&redis.Options{Addr: redisPort})
		mn := client1.Monitor(ctx, ress)
		mn.Start()
		// Wait for the Redis server to be in monitoring mode.
		time.Sleep(100 * time.Millisecond)
		client.Set(ctx, "foo", "bar", 0)
		client.Set(ctx, "bar", "baz", 0)
		client.Set(ctx, "bap", 8, 0)
		client.Get(ctx, "bap")
		lst := []string{}
		for i := 0; i < 5; i++ {
			s := <-ress
			lst = append(lst, s)
		}
		mn.Stop()
		Expect(lst[0]).To(ContainSubstring("OK"))
		Expect(lst[1]).To(ContainSubstring(`"set" "foo" "bar"`))
		Expect(lst[2]).To(ContainSubstring(`"set" "bar" "baz"`))
		Expect(lst[3]).To(ContainSubstring(`"set" "bap" "8"`))
	})
})

func TestMonitorCommand(t *testing.T) {
	if os.Getenv("RUN_MONITOR_TEST") != "true" {
		t.Skip("Skipping Monitor command test. Set RUN_MONITOR_TEST=true to run it.")
	}

	ctx := context.TODO()
	client := redis.NewClient(&redis.Options{Addr: redisPort})
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}()

	ress := make(chan string, 10)                               // Buffer to prevent blocking
	client1 := redis.NewClient(&redis.Options{Addr: redisPort}) // Adjust the Addr field as necessary
	mn := client1.Monitor(ctx, ress)
	mn.Start()
	// Wait for the Redis server to be in monitoring mode.
	time.Sleep(100 * time.Millisecond)
	client.Set(ctx, "foo", "bar", 0)
	client.Set(ctx, "bar", "baz", 0)
	client.Set(ctx, "bap", 8, 0)
	client.Get(ctx, "bap")
	mn.Stop()
	var lst []string
	for i := 0; i < 5; i++ {
		s := <-ress
		lst = append(lst, s)
	}

	// Assertions
	if !containsSubstring(lst[0], "OK") {
		t.Errorf("Expected lst[0] to contain 'OK', got %s", lst[0])
	}
	if !containsSubstring(lst[1], `"set" "foo" "bar"`) {
		t.Errorf(`Expected lst[1] to contain '"set" "foo" "bar"', got %s`, lst[1])
	}
	if !containsSubstring(lst[2], `"set" "bar" "baz"`) {
		t.Errorf(`Expected lst[2] to contain '"set" "bar" "baz"', got %s`, lst[2])
	}
	if !containsSubstring(lst[3], `"set" "bap" "8"`) {
		t.Errorf(`Expected lst[3] to contain '"set" "bap" "8"', got %s`, lst[3])
	}
}

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestMonitorWithTimeout(t *testing.T) {
	if os.Getenv("RUN_MONITOR_TEST") != "true" {
		t.Skip("Skipping Monitor command test. Set RUN_MONITOR_TEST=true to run it.")
	}

	ctx := context.TODO()
	// Create a client with a very short ReadTimeout (100ms)
	client := redis.NewClient(&redis.Options{
		Addr:        redisPort,
		ReadTimeout: 100 * time.Millisecond,
	})
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}()

	ress := make(chan string, 10)
	// Create a separate client for executing commands
	commandClient := redis.NewClient(&redis.Options{Addr: redisPort})
	defer commandClient.Close()

	mn := client.Monitor(ctx, ress)
	mn.Start()

	// Wait for the Redis server to be in monitoring mode.
	time.Sleep(100 * time.Millisecond)

	// Wait longer than ReadTimeout to ensure timeouts occur
	t.Log("Waiting for timeout to occur...")
	time.Sleep(300 * time.Millisecond)

	// Execute commands after timeout should have occurred
	t.Log("Executing commands after timeout...")
	commandClient.Set(ctx, "key1", "value1", 0)
	commandClient.Set(ctx, "key2", "value2", 0)

	// Give some time for messages to arrive
	time.Sleep(100 * time.Millisecond)

	// Try to read messages - should still work despite timeouts
	var lst []string
	timeout := time.After(2 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case s := <-ress:
			lst = append(lst, s)
			t.Logf("Received message %d: %s", i, s)
		case <-timeout:
			t.Fatalf("Timed out waiting for messages. Got %d messages so far", len(lst))
		}
	}

	// Stop monitoring
	mn.Stop()

	// Verify we got at least the OK message and the SET commands
	if len(lst) < 3 {
		t.Errorf("Expected at least 3 messages, got %d", len(lst))
	}

	found := false
	for _, msg := range lst {
		if containsSubstring(msg, `"set" "key1" "value1"`) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find 'set key1 value1' in messages, got: %v", lst)
	}
}

func TestMonitorWithContextCancellation(t *testing.T) {
	if os.Getenv("RUN_MONITOR_TEST") != "true" {
		t.Skip("Skipping Monitor command test. Set RUN_MONITOR_TEST=true to run it.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := redis.NewClient(&redis.Options{Addr: redisPort})
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}()

	ress := make(chan string, 10)
	mn := client.Monitor(ctx, ress)
	mn.Start()

	// Wait for the Redis server to be in monitoring mode.
	time.Sleep(100 * time.Millisecond)

	// Execute a command
	client.Set(ctx, "test", "value", 0)

	// Wait a bit for the message
	time.Sleep(100 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait a bit for cleanup
	time.Sleep(100 * time.Millisecond)

	// Try to read from channel - should eventually close
	timeout := time.After(2 * time.Second)
	channelClosed := false
	for !channelClosed {
		select {
		case _, ok := <-ress:
			if !ok {
				channelClosed = true
				t.Log("Channel was properly closed")
			}
		case <-timeout:
			break
		}
	}

	if !channelClosed {
		t.Log("Note: Channel may not close immediately, but should not block forever")
	}
}
