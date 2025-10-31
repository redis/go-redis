package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineTypedCommands tests that typed commands like Get() and Set()
// work correctly with autopipelining and block until execution.
func TestAutoPipelineTypedCommands(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Test Set and Get
	setResult, setErr := ap.Set(ctx, "test_key", "test_value", 0).Result()
	if setErr != nil {
		t.Fatalf("Set failed: %v", setErr)
	}
	if setResult != "OK" {
		t.Errorf("Expected 'OK', got '%s'", setResult)
	}

	getResult, getErr := ap.Get(ctx, "test_key").Result()
	if getErr != nil {
		t.Fatalf("Get failed: %v", getErr)
	}
	if getResult != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", getResult)
	}

	// Clean up
	client.Del(ctx, "test_key")
}

// TestAutoPipelineTypedCommandsMultiple tests multiple typed commands
func TestAutoPipelineTypedCommandsMultiple(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue multiple Set commands
	ap.Set(ctx, "key1", "value1", 0)
	ap.Set(ctx, "key2", "value2", 0)
	ap.Set(ctx, "key3", "value3", 0)

	// Get the values - should block until execution
	val1, err1 := ap.Get(ctx, "key1").Result()
	if err1 != nil {
		t.Fatalf("Get key1 failed: %v", err1)
	}
	if val1 != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val1)
	}

	val2, err2 := ap.Get(ctx, "key2").Result()
	if err2 != nil {
		t.Fatalf("Get key2 failed: %v", err2)
	}
	if val2 != "value2" {
		t.Errorf("Expected 'value2', got '%s'", val2)
	}

	val3, err3 := ap.Get(ctx, "key3").Result()
	if err3 != nil {
		t.Fatalf("Get key3 failed: %v", err3)
	}
	if val3 != "value3" {
		t.Errorf("Expected 'value3', got '%s'", val3)
	}

	// Clean up
	client.Del(ctx, "key1", "key2", "key3")
}

// TestAutoPipelineTypedCommandsVal tests the Val() method
func TestAutoPipelineTypedCommandsVal(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Set a value
	ap.Set(ctx, "test_val_key", "test_val_value", 0)

	// Get using Val() - should block until execution
	val := ap.Get(ctx, "test_val_key").Val()
	if val != "test_val_value" {
		t.Errorf("Expected 'test_val_value', got '%s'", val)
	}

	// Clean up
	client.Del(ctx, "test_val_key")
}

