package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/pool"
)

// TestIdleConnectionsAreInitialized verifies that connections created by MinIdleConns
// are properly initialized before being used (i.e., AUTH/HELLO/SELECT commands are executed).
func TestIdleConnectionsAreInitialized(t *testing.T) {
	// Create client with MinIdleConns
	opt := &redis.Options{
		Addr:           ":6379",
		Password:       "asdf",
		DB:             1,
		MinIdleConns:   5,
		PoolSize:       10,
		Protocol:       3,
		MaxActiveConns: 50,
	}

	client := redis.NewClient(opt)
	defer client.Close()

	// Wait for minIdle connections to be created
	time.Sleep(200 * time.Millisecond)
	// Now use these connections - they should be properly initialized
	// If they're not initialized, we'll get NOAUTH or WRONGDB errors
	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 200000)
	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each goroutine performs multiple operations
			for j := 0; j < 2000; j++ {
				key := fmt.Sprintf("test_key_%d_%d", id, j)

				// This will fail with NOAUTH if connection is not initialized
				err := client.Set(ctx, key, "value", 0).Err()
				if err != nil {
					errors <- fmt.Errorf("SET failed for %s: %w", key, err)
					return
				}

				val, err := client.Get(ctx, key).Result()
				if err != nil {
					errors <- fmt.Errorf("GET failed for %s: %w", key, err)
					return
				}
				if val != "value" {
					errors <- fmt.Errorf("GET returned wrong value for %s: got %s, want 'value'", key, val)
					return
				}

				err = client.Del(ctx, key).Err()
				if err != nil {
					errors <- fmt.Errorf("DEL failed for %s: %w", key, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	fmt.Printf("\nTOOK %s\n", time.Since(start))

	// Check for errors
	var errCount int
	for err := range errors {
		t.Errorf("Operation error: %v", err)
		errCount++
	}

	if errCount > 0 {
		t.Fatalf("Got %d errors during operations (likely NOAUTH or WRONGDB)", errCount)
	}

	// Verify final state
	err := client.Ping(ctx).Err()
	if err != nil {
		t.Errorf("Final Ping failed: %v", err)
	}

	fmt.Printf("pool stats: %+v\n", client.PoolStats())
}

// testPoolHook implements pool.PoolHook for testing
type testPoolHook struct {
	onGet    func(ctx context.Context, conn *pool.Conn, isNewConn bool) (bool, error)
	onPut    func(ctx context.Context, conn *pool.Conn) (bool, bool, error)
	onRemove func(ctx context.Context, conn *pool.Conn, reason error)
}

func (h *testPoolHook) OnGet(ctx context.Context, conn *pool.Conn, isNewConn bool) (bool, error) {
	if h.onGet != nil {
		return h.onGet(ctx, conn, isNewConn)
	}
	return true, nil
}

func (h *testPoolHook) OnPut(ctx context.Context, conn *pool.Conn) (bool, bool, error) {
	if h.onPut != nil {
		return h.onPut(ctx, conn)
	}
	return true, false, nil
}

func (h *testPoolHook) OnRemove(ctx context.Context, conn *pool.Conn, reason error) {
	if h.onRemove != nil {
		h.onRemove(ctx, conn, reason)
	}
}
