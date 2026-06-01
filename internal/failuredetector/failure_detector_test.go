package failuredetector

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
	successes, failures, _ := fd.Stats()
	if failures != 0 {
		t.Errorf("expected 0 failures, got %d", failures)
	}
	if successes != 10 {
		t.Errorf("expected 10 successes, got %d", successes)
	}
}

func TestCommandFailureDetector_WindowReset(t *testing.T) {
	config := CommandFailureDetectorConfig{
		MinNumFailures:         5,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: 50 * time.Millisecond,
	}
	fd := NewCommandFailureDetector(config)

	// Record failures
	for i := 0; i < 10; i++ {
		fd.RecordFailure(errors.New("error"))
	}

	// Wait for window to expire
	time.Sleep(60 * time.Millisecond)

	// Record a success to trigger window check
	fd.RecordSuccess()

	successes, failures, _ := fd.Stats()
	if failures != 0 {
		t.Errorf("expected 0 failures after window reset, got %d", failures)
	}
	if successes != 1 {
		t.Errorf("expected 1 success after window reset, got %d", successes)
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

	successes, failures, _ := fd.Stats()
	if failures != 0 || successes != 0 {
		t.Errorf("expected 0/0 after reset, got %d/%d", successes, failures)
	}
}

func TestCommandFailureDetector_RateOnly(t *testing.T) {
	// MinNumFailures == 0 means only the failure rate is taken into account.
	config := CommandFailureDetectorConfig{
		MinNumFailures:         0,
		FailureRateThreshold:   0.5,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// 1 failure out of 1 command => 100% rate, count threshold disabled.
	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("should failover when rate exceeds threshold and count is disabled")
	}

	// Drive the rate below the threshold with successes.
	for i := 0; i < 10; i++ {
		fd.RecordSuccess()
	}
	if fd.ShouldFailover() {
		t.Error("should not failover when rate drops below threshold")
	}
}

func TestCommandFailureDetector_CountOnly(t *testing.T) {
	// FailureRateThreshold == 0.0 means only the failure count is considered.
	config := CommandFailureDetectorConfig{
		MinNumFailures:         3,
		FailureRateThreshold:   0.0,
		FailureDetectionWindow: time.Hour,
	}
	fd := NewCommandFailureDetector(config)

	// A small failure rate but below the count threshold => no failover.
	fd.RecordFailure(errors.New("error"))
	fd.RecordFailure(errors.New("error"))
	for i := 0; i < 100; i++ {
		fd.RecordSuccess()
	}
	if fd.ShouldFailover() {
		t.Error("should not failover before reaching the failure count")
	}

	// Reaching the count threshold triggers failover regardless of rate.
	fd.RecordFailure(errors.New("error"))
	if !fd.ShouldFailover() {
		t.Error("should failover once the failure count is reached")
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
