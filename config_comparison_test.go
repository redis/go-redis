package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBadConfigurationHighLoad demonstrates the problem with default configuration
// under high load with slow dials.
func TestBadConfigurationHighLoad(t *testing.T) {
	var dialCount atomic.Int32
	var dialsFailed atomic.Int32
	var dialsSucceeded atomic.Int32
	
	// Simulate slow network - 300ms per dial (e.g., network latency, TLS handshake)
	slowDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialCount.Add(1)
		select {
		case <-time.After(300 * time.Millisecond):
			dialsSucceeded.Add(1)
			return &net.TCPConn{}, nil
		case <-ctx.Done():
			dialsFailed.Add(1)
			return nil, ctx.Err()
		}
	}

	// BAD CONFIGURATION: Default settings
	// On an 8-CPU machine:
	// - PoolSize = 10 * 8 = 80
	// - MaxConcurrentDials = 80
	// - MinIdleConns = 0 (no pre-warming)
	opt := &Options{
		Addr:               "localhost:6379",
		Dialer:             slowDialer,
		PoolSize:           80,  // Default: 10 * GOMAXPROCS
		MaxConcurrentDials: 80,  // Default: same as PoolSize
		MinIdleConns:       0,   // Default: no pre-warming
		DialTimeout:        5 * time.Second,
	}

	client := NewClient(opt)
	defer client.Close()

	// Simulate high load: 200 concurrent requests with 200ms timeout
	// This simulates a burst of traffic (e.g., after a deployment or cache miss)
	const numRequests = 200
	const requestTimeout = 200 * time.Millisecond

	var wg sync.WaitGroup
	var timeouts atomic.Int32
	var successes atomic.Int32
	var errors atomic.Int32

	startTime := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			defer cancel()

			_, err := client.Get(ctx, fmt.Sprintf("key-%d", id)).Result()

			if err != nil {
				if ctx.Err() == context.DeadlineExceeded || err == context.DeadlineExceeded {
					timeouts.Add(1)
				} else {
					errors.Add(1)
				}
			} else {
				successes.Add(1)
			}
		}(i)

		// Stagger requests slightly to simulate real traffic
		if i%20 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	timeoutRate := float64(timeouts.Load()) / float64(numRequests) * 100
	successRate := float64(successes.Load()) / float64(numRequests) * 100

	t.Logf("\n=== BAD CONFIGURATION (Default Settings) ===")
	t.Logf("Configuration:")
	t.Logf("  PoolSize: %d", opt.PoolSize)
	t.Logf("  MaxConcurrentDials: %d", opt.MaxConcurrentDials)
	t.Logf("  MinIdleConns: %d", opt.MinIdleConns)
	t.Logf("\nResults:")
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Successes: %d (%.1f%%)", successes.Load(), successRate)
	t.Logf("  Timeouts: %d (%.1f%%)", timeouts.Load(), timeoutRate)
	t.Logf("  Other errors: %d", errors.Load())
	t.Logf("  Total dials: %d (succeeded: %d, failed: %d)", 
		dialCount.Load(), dialsSucceeded.Load(), dialsFailed.Load())

	// With bad configuration:
	// - MaxConcurrentDials=80 means only 80 dials can run concurrently
	// - Each dial takes 300ms, but request timeout is 200ms
	// - Requests timeout waiting for dial slots
	// - Expected: High timeout rate (>50%)

	if timeoutRate < 50 {
		t.Logf("WARNING: Expected high timeout rate (>50%%), got %.1f%%. Test may not be stressing the system enough.", timeoutRate)
	}
}

// TestGoodConfigurationHighLoad demonstrates how proper configuration fixes the problem
func TestGoodConfigurationHighLoad(t *testing.T) {
	var dialCount atomic.Int32
	var dialsFailed atomic.Int32
	var dialsSucceeded atomic.Int32
	
	// Same slow dialer - 300ms per dial
	slowDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialCount.Add(1)
		select {
		case <-time.After(300 * time.Millisecond):
			dialsSucceeded.Add(1)
			return &net.TCPConn{}, nil
		case <-ctx.Done():
			dialsFailed.Add(1)
			return nil, ctx.Err()
		}
	}

	// GOOD CONFIGURATION: Tuned for high load
	opt := &Options{
		Addr:               "localhost:6379",
		Dialer:             slowDialer,
		PoolSize:           300, // Increased from 80
		MaxConcurrentDials: 300, // Increased from 80
		MinIdleConns:       50,  // Pre-warm the pool
		DialTimeout:        5 * time.Second,
	}

	client := NewClient(opt)
	defer client.Close()

	// Wait for pool to warm up
	time.Sleep(100 * time.Millisecond)

	// Same load: 200 concurrent requests with 200ms timeout
	const numRequests = 200
	const requestTimeout = 200 * time.Millisecond

	var wg sync.WaitGroup
	var timeouts atomic.Int32
	var successes atomic.Int32
	var errors atomic.Int32

	startTime := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			defer cancel()

			_, err := client.Get(ctx, fmt.Sprintf("key-%d", id)).Result()

			if err != nil {
				if ctx.Err() == context.DeadlineExceeded || err == context.DeadlineExceeded {
					timeouts.Add(1)
				} else {
					errors.Add(1)
				}
			} else {
				successes.Add(1)
			}
		}(i)

		// Stagger requests slightly
		if i%20 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	timeoutRate := float64(timeouts.Load()) / float64(numRequests) * 100
	successRate := float64(successes.Load()) / float64(numRequests) * 100

	t.Logf("\n=== GOOD CONFIGURATION (Tuned Settings) ===")
	t.Logf("Configuration:")
	t.Logf("  PoolSize: %d", opt.PoolSize)
	t.Logf("  MaxConcurrentDials: %d", opt.MaxConcurrentDials)
	t.Logf("  MinIdleConns: %d", opt.MinIdleConns)
	t.Logf("\nResults:")
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Successes: %d (%.1f%%)", successes.Load(), successRate)
	t.Logf("  Timeouts: %d (%.1f%%)", timeouts.Load(), timeoutRate)
	t.Logf("  Other errors: %d", errors.Load())
	t.Logf("  Total dials: %d (succeeded: %d, failed: %d)", 
		dialCount.Load(), dialsSucceeded.Load(), dialsFailed.Load())

	// With good configuration:
	// - Higher MaxConcurrentDials allows more concurrent dials
	// - MinIdleConns pre-warms the pool
	// - Expected: Low timeout rate (<20%)

	if timeoutRate > 20 {
		t.Errorf("Expected low timeout rate (<20%%), got %.1f%%", timeoutRate)
	}
}

// TestConfigurationComparison runs both tests and shows the difference
func TestConfigurationComparison(t *testing.T) {
	t.Run("BadConfiguration", TestBadConfigurationHighLoad)
	t.Run("GoodConfiguration", TestGoodConfigurationHighLoad)
}

