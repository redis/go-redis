package redis

import (
	"context"
	"testing"
	"time"
)

// TestV10APIProofOfConcept tests the v10 API approach with a simple example
func TestV10APIProofOfConcept(t *testing.T) {
	// Skip if not in integration test mode
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	
	// Create a simple client for testing
	client := NewClient(&Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Test GetV10/SetV10
	t.Run("GetV10/SetV10", func(t *testing.T) {
		// Set a value
		status, err := client.SetV10(ctx, "test:v10:key", "hello", 0)
		if err != nil {
			t.Fatalf("SetV10 failed: %v", err)
		}
		if status != "OK" {
			t.Errorf("Expected status OK, got %s", status)
		}

		// Get the value
		val, err := client.GetV10(ctx, "test:v10:key")
		if err != nil {
			t.Fatalf("GetV10 failed: %v", err)
		}
		if val != "hello" {
			t.Errorf("Expected value 'hello', got '%s'", val)
		}

		// Clean up
		client.Del(ctx, "test:v10:key")
	})

	// Test IncrV10
	t.Run("IncrV10", func(t *testing.T) {
		// Incr a counter
		val, err := client.IncrV10(ctx, "test:v10:counter")
		if err != nil {
			t.Fatalf("IncrV10 failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected value 1, got %d", val)
		}

		val, err = client.IncrV10(ctx, "test:v10:counter")
		if err != nil {
			t.Fatalf("IncrV10 failed: %v", err)
		}
		if val != 2 {
			t.Errorf("Expected value 2, got %d", val)
		}

		// Clean up
		client.Del(ctx, "test:v10:counter")
	})

	// Test SetNXV10
	t.Run("SetNXV10", func(t *testing.T) {
		// First set should succeed
		wasSet, err := client.SetNXV10(ctx, "test:v10:nx", "value1", 0)
		if err != nil {
			t.Fatalf("SetNXV10 failed: %v", err)
		}
		if !wasSet {
			t.Error("Expected SetNX to succeed on first call")
		}

		// Second set should fail (key exists)
		wasSet, err = client.SetNXV10(ctx, "test:v10:nx", "value2", 0)
		if err != nil {
			t.Fatalf("SetNXV10 failed: %v", err)
		}
		if wasSet {
			t.Error("Expected SetNX to fail on second call")
		}

		// Value should still be value1
		val, err := client.GetV10(ctx, "test:v10:nx")
		if err != nil {
			t.Fatalf("GetV10 failed: %v", err)
		}
		if val != "value1" {
			t.Errorf("Expected value 'value1', got '%s'", val)
		}

		// Clean up
		client.Del(ctx, "test:v10:nx")
	})

	// Test with expiration
	t.Run("SetV10 with expiration", func(t *testing.T) {
		status, err := client.SetV10(ctx, "test:v10:exp", "value", 1*time.Second)
		if err != nil {
			t.Fatalf("SetV10 failed: %v", err)
		}
		if status != "OK" {
			t.Errorf("Expected status OK, got %s", status)
		}

		// Value should exist
		val, err := client.GetV10(ctx, "test:v10:exp")
		if err != nil {
			t.Fatalf("GetV10 failed: %v", err)
		}
		if val != "value" {
			t.Errorf("Expected value 'value', got '%s'", val)
		}

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Value should be gone
		_, err = client.GetV10(ctx, "test:v10:exp")
		if err != Nil {
			t.Errorf("Expected Nil error, got %v", err)
		}
	})

	// Test MGetV10/MSetV10
	t.Run("MGetV10/MSetV10", func(t *testing.T) {
		// Set multiple values
		status, err := client.MSetV10(ctx, "test:v10:m1", "val1", "test:v10:m2", "val2", "test:v10:m3", "val3")
		if err != nil {
			t.Fatalf("MSetV10 failed: %v", err)
		}
		if status != "OK" {
			t.Errorf("Expected status OK, got %s", status)
		}

		// Get multiple values
		vals, err := client.MGetV10(ctx, "test:v10:m1", "test:v10:m2", "test:v10:m3")
		if err != nil {
			t.Fatalf("MGetV10 failed: %v", err)
		}
		if len(vals) != 3 {
			t.Errorf("Expected 3 values, got %d", len(vals))
		}
		if vals[0] != "val1" || vals[1] != "val2" || vals[2] != "val3" {
			t.Errorf("Unexpected values: %v", vals)
		}

		// Clean up
		client.Del(ctx, "test:v10:m1", "test:v10:m2", "test:v10:m3")
	})

	t.Log("✅ All v10 API tests passed!")
}

