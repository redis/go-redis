package pool

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestBackgroundGoroutineOverhead measures the overhead of the background time updater
func TestBackgroundGoroutineOverhead(t *testing.T) {
	// Measure baseline goroutines
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()
	
	t.Logf("Baseline goroutines: %d", baselineGoroutines)
	
	// The background goroutine is already running from init()
	// Let's verify it's updating the cache
	time1 := getCachedTimeNs()
	time.Sleep(100 * time.Millisecond) // Wait for at least 2 updates (50ms each)
	time2 := getCachedTimeNs()
	
	if time2 <= time1 {
		t.Errorf("Time cache not updating: time1=%d, time2=%d", time1, time2)
	}
	
	diff := time2 - time1
	expectedDiff := int64(100 * time.Millisecond)
	
	// Allow 10% tolerance
	if diff < expectedDiff*9/10 || diff > expectedDiff*11/10 {
		t.Logf("Warning: Time diff %dns not close to expected %dns (within 10%%)", diff, expectedDiff)
	} else {
		t.Logf("Time cache updating correctly: diff=%dns, expected~%dns", diff, expectedDiff)
	}
	
	// Check goroutine count (should be baseline + 1 for our updater)
	currentGoroutines := runtime.NumGoroutine()
	t.Logf("Current goroutines: %d (expected: %d)", currentGoroutines, baselineGoroutines)
}

// BenchmarkBackgroundGoroutineImpact measures if the background goroutine impacts performance
func BenchmarkBackgroundGoroutineImpact(b *testing.B) {
	// This benchmark runs alongside the background goroutine
	// to see if there's any measurable impact on other operations
	
	var counter atomic.Int64
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Add(1)
			_ = getCachedTimeNs()
		}
	})
	
	b.Logf("Total operations: %d", counter.Load())
}

// BenchmarkTickerOverhead measures the overhead of time.Ticker
func BenchmarkTickerOverhead(b *testing.B) {
	// Create a ticker to measure its overhead
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	
	var updates atomic.Int64
	done := make(chan struct{})
	
	// Background goroutine that receives from ticker
	go func() {
		for {
			select {
			case <-ticker.C:
				updates.Add(1)
			case <-done:
				return
			}
		}
	}()
	
	// Run benchmark for 3 seconds
	time.Sleep(3 * time.Second)
	close(done)
	
	totalUpdates := updates.Load()
	expectedUpdates := int64(3000 / 50) // 3000ms / 50ms = 60 updates
	
	b.Logf("Ticker fired %d times in 3s (expected ~%d)", totalUpdates, expectedUpdates)
	
	if totalUpdates < expectedUpdates*9/10 || totalUpdates > expectedUpdates*11/10 {
		b.Logf("Warning: Update count not within 10%% of expected")
	}
}

