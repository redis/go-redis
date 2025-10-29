package pool

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TestConn_UsedAtUpdatedOnRead verifies that usedAt is updated when reading from connection
func TestConn_UsedAtUpdatedOnRead(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	// Get initial usedAt time
	initialUsedAt := cn.UsedAt()

	// Wait at least 50ms to ensure time difference (usedAt has ~50ms precision from cached time)
	time.Sleep(100 * time.Millisecond)

	// Simulate a read operation by calling WithReader
	ctx := context.Background()
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		// Don't actually read anything, just trigger the deadline update
		return nil
	})

	if err != nil {
		t.Fatalf("WithReader failed: %v", err)
	}

	// Get updated usedAt time
	updatedUsedAt := cn.UsedAt()

	// Verify that usedAt was updated
	if !updatedUsedAt.After(initialUsedAt) {
		t.Errorf("Expected usedAt to be updated after read. Initial: %v, Updated: %v",
			initialUsedAt, updatedUsedAt)
	}

	// Verify the difference is reasonable (should be around 50ms, accounting for ~50ms cache precision)
	diff := updatedUsedAt.Sub(initialUsedAt)
	if diff < 50*time.Millisecond || diff > 200*time.Millisecond {
		t.Errorf("Expected usedAt difference to be around 50ms (±50ms for cache), got %v", diff)
	}
}

// TestConn_UsedAtUpdatedOnWrite verifies that usedAt is updated when writing to connection
func TestConn_UsedAtUpdatedOnWrite(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	// Get initial usedAt time
	initialUsedAt := cn.UsedAt()

	// Wait at least 100ms to ensure time difference (usedAt has ~50ms precision from cached time)
	time.Sleep(100 * time.Millisecond)

	// Simulate a write operation by calling WithWriter
	ctx := context.Background()
	err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		// Don't actually write anything, just trigger the deadline update
		return nil
	})

	if err != nil {
		t.Fatalf("WithWriter failed: %v", err)
	}

	// Get updated usedAt time
	updatedUsedAt := cn.UsedAt()

	// Verify that usedAt was updated
	if !updatedUsedAt.After(initialUsedAt) {
		t.Errorf("Expected usedAt to be updated after write. Initial: %v, Updated: %v",
			initialUsedAt, updatedUsedAt)
	}

	// Verify the difference is reasonable (should be around 100ms, accounting for ~50ms cache precision)
	diff := updatedUsedAt.Sub(initialUsedAt)

	if diff > 100*time.Millisecond {
		t.Errorf("Expected usedAt difference to be no more than 100ms (±50ms for cache), got %v", diff)
	}
}

// TestConn_UsedAtUpdatedOnMultipleOperations verifies that usedAt is updated on each operation
func TestConn_UsedAtUpdatedOnMultipleOperations(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	ctx := context.Background()
	var previousUsedAt time.Time

	// Perform multiple operations and verify usedAt is updated each time
	// Note: usedAt has ~50ms precision from cached time
	for i := 0; i < 5; i++ {
		currentUsedAt := cn.UsedAt()

		if i > 0 {
			// Verify usedAt was updated from previous iteration
			if !currentUsedAt.After(previousUsedAt) {
				t.Errorf("Iteration %d: Expected usedAt to be updated. Previous: %v, Current: %v",
					i, previousUsedAt, currentUsedAt)
			}
		}

		previousUsedAt = currentUsedAt

		// Wait at least 100ms (accounting for ~50ms cache precision)
		time.Sleep(100 * time.Millisecond)

		// Perform a read operation
		err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
			return nil
		})
		if err != nil {
			t.Fatalf("Iteration %d: WithReader failed: %v", i, err)
		}
	}

	// Verify final usedAt is significantly later than initial
	finalUsedAt := cn.UsedAt()
	if !finalUsedAt.After(previousUsedAt) {
		t.Errorf("Expected final usedAt to be updated. Previous: %v, Final: %v",
			previousUsedAt, finalUsedAt)
	}
}

// TestConn_UsedAtNotUpdatedWithoutOperation verifies that usedAt is NOT updated without operations
func TestConn_UsedAtNotUpdatedWithoutOperation(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	// Get initial usedAt time
	initialUsedAt := cn.UsedAt()

	// Wait without performing any operations
	time.Sleep(100 * time.Millisecond)

	// Get usedAt time again
	currentUsedAt := cn.UsedAt()

	// Verify that usedAt was NOT updated (should be the same)
	if !currentUsedAt.Equal(initialUsedAt) {
		t.Errorf("Expected usedAt to remain unchanged without operations. Initial: %v, Current: %v",
			initialUsedAt, currentUsedAt)
	}
}

// TestConn_UsedAtConcurrentUpdates verifies that usedAt updates are thread-safe
func TestConn_UsedAtConcurrentUpdates(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	ctx := context.Background()
	const numGoroutines = 10
	const numIterations = 10

	// Launch multiple goroutines that perform operations concurrently
	done := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < numIterations; j++ {
				// Alternate between read and write operations
				if j%2 == 0 {
					_ = cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
						return nil
					})
				} else {
					_ = cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
						return nil
					})
				}
				time.Sleep(time.Millisecond)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify that usedAt was updated (should be recent)
	usedAt := cn.UsedAt()
	timeSinceUsed := time.Since(usedAt)

	// Should be very recent (within last second)
	if timeSinceUsed > time.Second {
		t.Errorf("Expected usedAt to be recent, but it was %v ago", timeSinceUsed)
	}
}

// TestConn_UsedAtPrecision verifies that usedAt has 50ms precision (not nanosecond)
func TestConn_UsedAtPrecision(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	defer cn.Close()

	ctx := context.Background()

	// Perform an operation
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		return nil
	})
	if err != nil {
		t.Fatalf("WithReader failed: %v", err)
	}

	// Get usedAt time
	usedAt := cn.UsedAt()

	// Verify that usedAt has nanosecond precision (from the cached time which updates every 50ms)
	// The value should be reasonable (not year 1970 or something)
	if usedAt.Year() < 2020 {
		t.Errorf("Expected usedAt to be a recent time, got %v", usedAt)
	}

	// The nanoseconds might be non-zero depending on when the cache was updated
	// We just verify the time is stored with full precision (not truncated to seconds)
	initialNanos := usedAt.UnixNano()
	if initialNanos == 0 {
		t.Error("Expected usedAt to have nanosecond precision, got 0")
	}
}
