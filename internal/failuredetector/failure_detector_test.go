package failuredetector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withFakeClock returns the detector with a hand-driven clock so tests can
// advance time without sleeping. The returned closure shifts the clock by
// the given duration. Use this for any test that depends on window expiry.
func withFakeClock(fd *CommandFailureDetector, start time.Time) (advance func(time.Duration)) {
	cur := start
	fd.now = func() time.Time { return cur }
	return func(d time.Duration) { cur = cur.Add(d) }
}

func TestCommandFailureDetector_ShouldFailover(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         10,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// Not enough failures
	for i := 0; i < 5; i++ {
		fd.RecordFailure(errors.New("error"))
	}
	if fd.ShouldFailover() {
		t.Error("should not failover with insufficient failures")
	}

	// Add more failures to reach threshold
	for i := 0; i < 5; i++ {
		fd.RecordFailure(errors.New("error"))
	}
	if !fd.ShouldFailover() {
		t.Error("should failover with 100% failure rate")
	}
}

func TestCommandFailureDetector_SuccessResetsRate(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         10,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// Record failures
	for i := 0; i < 10; i++ {
		fd.RecordFailure(errors.New("error"))
	}

	// Record successes to bring rate below threshold
	for i := 0; i < 15; i++ {
		fd.RecordSuccess()
	}

	// 10 failures / 25 total = 40% < 50%
	if fd.ShouldFailover() {
		t.Error("should not failover when failure rate is below threshold")
	}
}

func TestCommandFailureDetector_IgnoresContextErrors(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// Record context errors - should be ignored
	for i := 0; i < 10; i++ {
		fd.RecordFailure(context.Canceled)
		fd.RecordFailure(context.DeadlineExceeded)
	}

	// Record some successes
	for i := 0; i < 10; i++ {
		fd.RecordSuccess()
	}

	// Only successes should be counted
	successes, failures := fd.Stats()
	if failures != 0 {
		t.Errorf("expected 0 failures, got %d", failures)
	}
	if successes != 10 {
		t.Errorf("expected 10 successes, got %d", successes)
	}
}

func TestCommandFailureDetector_IgnoresNilError(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         1,
		FailureRateThreshold:   0.0,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// A nil error represents success and must not be counted as a failure.
	for i := 0; i < 10; i++ {
		fd.RecordFailure(nil)
	}

	_, failures := fd.Stats()
	if failures != 0 {
		t.Errorf("expected 0 failures for nil errors, got %d", failures)
	}
	if fd.ShouldFailover() {
		t.Error("should not failover when only nil errors were recorded")
	}
}

func TestCommandFailureDetector_DefaultsWindowWhenUnset(t *testing.T) {
	// A zero window must fall back to the documented default so the counters
	// are not reset on every call (which would prevent failover entirely).
	config := CommandFailureDetectorConfig{
		MinNumFailures:       1,
		FailureRateThreshold: 0.0,
	}
	fd := NewCommandFailureDetector(config)

	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("should failover after a failure with the defaulted window")
	}

	_, failures := fd.Stats()
	if failures != 1 {
		t.Errorf("expected 1 failure to be retained within the window, got %d", failures)
	}
}

func TestCommandFailureDetector_WindowExpiry(t *testing.T) {
	// Outcomes older than the window must not be counted. The sliding
	// implementation drops them one bucket at a time, so by advancing time
	// past the full window every previously-recorded outcome should age out.
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Second,
		NumBuckets:             10,
	}
	fd := NewCommandFailureDetector(config)
	advance := withFakeClock(fd, time.Unix(1_700_000_000, 0))

	for i := 0; i < 10; i++ {
		fd.RecordFailure(errors.New("error"))
	}
	if _, failures := fd.Stats(); failures != 10 {
		t.Fatalf("expected 10 failures before expiry, got %d", failures)
	}

	// Step past the window by one full bucket width so every bucket falls
	// outside the trailing window.
	advance(config.FailureDetectionWindow + config.FailureDetectionWindow/time.Duration(config.NumBuckets))

	fd.RecordSuccess()

	successes, failures := fd.Stats()
	if failures != 0 {
		t.Errorf("expected 0 failures after window expiry, got %d", failures)
	}
	if successes != 1 {
		t.Errorf("expected 1 success after window expiry, got %d", successes)
	}
}

func TestCommandFailureDetector_Reset(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// Record some activity
	for i := 0; i < 10; i++ {
		fd.RecordFailure(errors.New("error"))
		fd.RecordSuccess()
	}

	fd.Reset()

	successes, failures := fd.Stats()
	if failures != 0 || successes != 0 {
		t.Errorf("expected 0/0 after reset, got %d/%d", successes, failures)
	}
}

func TestCommandFailureDetector_AppliesDefaultsForZeroFields(t *testing.T) {
	// A zero-valued config must be equivalent to passing the documented
	// defaults; reviewers flagged that the field comments promised defaults
	// the constructor did not apply.
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{})
	defaults := DefaultCommandFailureDetectorConfig()

	if fd.config.MinNumFailures != defaults.MinNumFailures {
		t.Errorf("MinNumFailures: got %d, want %d (default)",
			fd.config.MinNumFailures, defaults.MinNumFailures)
	}
	if fd.config.FailureRateThreshold != defaults.FailureRateThreshold {
		t.Errorf("FailureRateThreshold: got %v, want %v (default)",
			fd.config.FailureRateThreshold, defaults.FailureRateThreshold)
	}
	if fd.config.FailureDetectionWindow != defaults.FailureDetectionWindow {
		t.Errorf("FailureDetectionWindow: got %v, want %v (default)",
			fd.config.FailureDetectionWindow, defaults.FailureDetectionWindow)
	}
	if fd.config.NumBuckets != defaults.NumBuckets {
		t.Errorf("NumBuckets: got %d, want %d (default)",
			fd.config.NumBuckets, defaults.NumBuckets)
	}
}

func TestCommandFailureDetector_PreservesExplicitValues(t *testing.T) {
	// Defaults must not overwrite explicit non-zero settings, otherwise the
	// detector would be impossible to tune away from the defaults.
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.25,
		FailureDetectionWindow: 500 * time.Millisecond,
		NumBuckets:             4,
	}
	fd := NewCommandFailureDetector(config)
	if fd.config != config {
		t.Errorf("explicit config was overwritten: got %+v, want %+v", fd.config, config)
	}
}

func TestCommandFailureDetector_DoesNotPanicOnTinyWindow(t *testing.T) {
	// A FailureDetectionWindow shorter than NumBuckets nanoseconds would
	// previously produce bucketWidthNano=0 and panic on the first record
	// via "%". The constructor must clamp the bucket width.
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		FailureDetectionWindow: 5 * time.Nanosecond,
		NumBuckets:             10,
	})
	// Both hot-path entry points must not panic.
	fd.RecordSuccess()
	fd.RecordFailure(errors.New("error"))
	_ = fd.ShouldFailover()
}

func TestCommandFailureDetector_IgnoreMinNumFailures(t *testing.T) {
	// With IgnoreMinNumFailures set, ShouldFailover should ignore the count
	// threshold and decide purely on the failure rate.
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		MinNumFailures:         10_000, // would otherwise gate every reasonable burst
		IgnoreMinNumFailures:   true,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	})

	// 1 failure out of 1 command => 100% rate, well above the threshold.
	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("should failover when rate exceeds threshold and count is ignored")
	}

	// Drive the rate below the threshold with successes; the count threshold
	// is still ignored, but the rate check must now reject.
	for i := 0; i < 10; i++ {
		fd.RecordSuccess()
	}
	if fd.ShouldFailover() {
		t.Error("should not failover when rate drops below threshold")
	}
}

func TestCommandFailureDetector_IgnoreFailureRateThreshold(t *testing.T) {
	// With IgnoreFailureRateThreshold set, ShouldFailover should ignore the
	// rate threshold and decide purely on the absolute failure count.
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		MinNumFailures:             3,
		FailureRateThreshold:       0.99, // would otherwise be impossible to meet
		IgnoreFailureRateThreshold: true,
		FailureDetectionWindow:     time.Hour,
	})

	// A small failure rate but below the count threshold => no failover.
	fd.RecordFailure(errors.New("error"))
	fd.RecordFailure(errors.New("error"))
	for i := 0; i < 100; i++ {
		fd.RecordSuccess()
	}
	if fd.ShouldFailover() {
		t.Error("should not failover before reaching the failure count")
	}

	// Reaching the count threshold triggers failover regardless of the rate.
	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("should failover once the failure count is reached")
	}
}

func TestCommandFailureDetector_IgnoreBothThresholds(t *testing.T) {
	// With both thresholds ignored, any single failure in the window should
	// be enough to trigger failover.
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		MinNumFailures:             10_000,
		IgnoreMinNumFailures:       true,
		FailureRateThreshold:       0.99,
		IgnoreFailureRateThreshold: true,
		FailureDetectionWindow:     time.Hour,
	})

	if fd.ShouldFailover() {
		t.Error("should not failover before any failure is recorded")
	}

	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("any single failure should trigger when both thresholds are ignored")
	}
}

func TestCommandFailureDetector_RequiresBothThresholds(t *testing.T) {
	// Both thresholds must be met when both are configured.
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// 5 failures reaches the count, but 5/20 = 25% is below the rate.
	for i := 0; i < 5; i++ {
		fd.RecordFailure(errors.New("error"))
	}
	for i := 0; i < 15; i++ {
		fd.RecordSuccess()
	}
	if fd.ShouldFailover() {
		t.Error("should not failover when only the count threshold is met")
	}
}

// TestCommandFailureDetector_SlidingWindow exercises the property that
// distinguishes a sliding window from a tumbling one: outcomes age out one
// bucket at a time as time advances, rather than disappearing en masse at a
// fixed window boundary.
func TestCommandFailureDetector_SlidingWindow(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         1,
		FailureRateThreshold:   0.0,
		FailureDetectionWindow: time.Second,
		NumBuckets:             10,
	}
	fd := NewCommandFailureDetector(config)
	advance := withFakeClock(fd, time.Unix(1_700_000_000, 0))

	bucket := config.FailureDetectionWindow / time.Duration(config.NumBuckets)

	// Spread one failure across each of the first three buckets.
	fd.RecordFailure(errors.New("e"))
	advance(bucket)
	fd.RecordFailure(errors.New("e"))
	advance(bucket)
	fd.RecordFailure(errors.New("e"))

	if _, failures := fd.Stats(); failures != 3 {
		t.Fatalf("expected 3 failures in window, got %d", failures)
	}

	// Step time so the very first bucket falls outside the trailing window.
	// In a tumbling implementation, advancing by < window would change
	// nothing; in a sliding implementation, the oldest failure must have
	// aged out.
	advance(config.FailureDetectionWindow - bucket)
	if _, failures := fd.Stats(); failures != 2 {
		t.Fatalf("expected 2 failures after oldest bucket aged out, got %d", failures)
	}

	// One more bucket-width drops the next oldest failure.
	advance(bucket)
	if _, failures := fd.Stats(); failures != 1 {
		t.Fatalf("expected 1 failure after second bucket aged out, got %d", failures)
	}

	// And once we walk past the full window, everything is gone.
	advance(config.FailureDetectionWindow)
	if _, failures := fd.Stats(); failures != 0 {
		t.Fatalf("expected 0 failures after full window elapsed, got %d", failures)
	}
}

// TestCommandFailureDetector_ConcurrentRecord checks that the lock-free
// hot path doesn't lose counts under contention. We fan out N goroutines
// each recording a fixed number of successes and failures and assert the
// totals match. The window is long enough that no bucket rotates during
// the test, so the count must be exact.
func TestCommandFailureDetector_ConcurrentRecord(t *testing.T) {
	const (
		numGoroutines     = 32
		opsPerGoroutine   = 5_000
		expectedSuccesses = numGoroutines * opsPerGoroutine
		expectedFailures  = numGoroutines * opsPerGoroutine
	)

	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		FailureDetectionWindow: time.Hour,
		NumBuckets:             10,
	})

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)
	errFail := errors.New("failure")

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				fd.RecordSuccess()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				fd.RecordFailure(errFail)
			}
		}()
	}
	wg.Wait()

	successes, failures := fd.Stats()
	if successes != uint64(expectedSuccesses) {
		t.Errorf("successes: got %d, want %d", successes, expectedSuccesses)
	}
	if failures != uint64(expectedFailures) {
		t.Errorf("failures: got %d, want %d", failures, expectedFailures)
	}
}

// TestCommandFailureDetector_ConcurrentRecordWithReaders pairs the
// concurrent recorders above with a flock of concurrent readers calling
// ShouldFailover. The test passes if no race is reported (-race) and no
// panic is observed.
func TestCommandFailureDetector_ConcurrentRecordWithReaders(t *testing.T) {
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		MinNumFailures:         50,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
		NumBuckets:             10,
	})

	var wg sync.WaitGroup
	var stop atomic.Bool
	errFail := errors.New("failure")

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				fd.RecordSuccess()
				fd.RecordFailure(errFail)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = fd.ShouldFailover()
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// BenchmarkCommandFailureDetector_RecordSuccess measures the cost of the
// hot path under contention so we can compare future implementations.
func BenchmarkCommandFailureDetector_RecordSuccess(b *testing.B) {
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		FailureDetectionWindow: time.Hour,
		NumBuckets:             10,
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fd.RecordSuccess()
		}
	})
}

// BenchmarkCommandFailureDetector_RecordFailure mirrors RecordSuccess for
// the failure path (same hot-path code, includes the error filter).
func BenchmarkCommandFailureDetector_RecordFailure(b *testing.B) {
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		FailureDetectionWindow: time.Hour,
		NumBuckets:             10,
	})
	err := errors.New("failure")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fd.RecordFailure(err)
		}
	})
}

// BenchmarkCommandFailureDetector_ShouldFailover measures the read path,
// which sums NumBuckets atomically-loaded counters.
func BenchmarkCommandFailureDetector_ShouldFailover(b *testing.B) {
	fd := NewCommandFailureDetector(CommandFailureDetectorConfig{
		MinNumFailures:         100,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
		NumBuckets:             10,
	})
	// Populate so the rate check branch runs.
	for i := 0; i < 1000; i++ {
		fd.RecordSuccess()
	}
	fd.RecordFailure(errors.New("failure"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fd.ShouldFailover()
	}
}
