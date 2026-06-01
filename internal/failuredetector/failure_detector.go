package failuredetector

import (
	"context"
	"errors"
	"sync"
	"time"
)

// FailureDetector detects failures and determines when failover should occur.
type FailureDetector interface {
	// RecordSuccess records a successful operation.
	RecordSuccess()
	// RecordFailure records a failed operation.
	RecordFailure(err error)
	// ShouldFailover returns true if failover should be triggered.
	ShouldFailover() bool
	// Reset resets the failure detector state.
	Reset()
}

// CommandFailureDetectorConfig holds configuration for CommandFailureDetector.
type CommandFailureDetectorConfig struct {
	// MinNumFailures is the minimum number of failed commands that must be
	// observed within the detection window before failover is considered.
	// A value of 0 means the failure count is ignored and only
	// FailureRateThreshold is taken into account.
	// Default: 1000
	MinNumFailures int
	// FailureRateThreshold is the failure rate (0.0-1.0) that, together with
	// MinNumFailures, triggers failover. For example, 0.1 means failover when
	// 10% or more of the commands in the window fail.
	// A value of 0.0 means the rate is ignored and only MinNumFailures is
	// taken into account.
	// Default: 0.1
	FailureRateThreshold float64
	// FailureDetectionWindow is the time window for failure detection.
	// Counters are reset when this window expires.
	// Default: 2 seconds
	FailureDetectionWindow time.Duration
}

// DefaultCommandFailureDetectorConfig returns the default configuration.
func DefaultCommandFailureDetectorConfig() CommandFailureDetectorConfig {
	return CommandFailureDetectorConfig{
		MinNumFailures:         1000,
		FailureRateThreshold:   0.1,
		FailureDetectionWindow: 2 * time.Second,
	}
}

// CommandFailureDetector detects failures based on command success/failure rates.
type CommandFailureDetector struct {
	config CommandFailureDetectorConfig

	mu            sync.Mutex
	successCount  int
	failuresCount int
	windowStart   time.Time
}

// NewCommandFailureDetector creates a new command failure detector.
func NewCommandFailureDetector(config CommandFailureDetectorConfig) *CommandFailureDetector {
	return &CommandFailureDetector{
		config:      config,
		windowStart: time.Now(),
	}
}

// RecordSuccess records a successful operation.
func (d *CommandFailureDetector) RecordSuccess() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.checkWindow()
	d.successCount++
}

// RecordFailure records a failed operation.
// Context errors (Canceled, DeadlineExceeded) are ignored.
func (d *CommandFailureDetector) RecordFailure(err error) {
	// Ignore context errors - they're client-side, not server failures
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.checkWindow()
	d.failuresCount++
}

// ShouldFailover returns true if failover should be triggered.
//
// A database is considered faulty when, within the detection window, at least
// MinNumFailures commands have failed AND the observed failure rate is at least
// FailureRateThreshold. The two thresholds have special cases:
//   - MinNumFailures == 0 means the failure count is ignored and only the rate
//     is taken into account.
//   - FailureRateThreshold == 0.0 means the rate is ignored and only the
//     failure count is taken into account.
//
// At least one failure must have been observed for failover to be considered.
func (d *CommandFailureDetector) ShouldFailover() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.checkWindow()

	// Nothing has failed within the current window.
	if d.failuresCount == 0 {
		return false
	}

	// Count condition: satisfied when the count threshold is disabled
	// (MinNumFailures <= 0) or the observed failures reach the threshold.
	countMet := d.config.MinNumFailures <= 0 || d.failuresCount >= d.config.MinNumFailures

	// Rate condition: satisfied when the rate threshold is disabled
	// (FailureRateThreshold <= 0.0) or the observed failure rate reaches it.
	rateMet := true
	if d.config.FailureRateThreshold > 0 {
		totalCommands := d.successCount + d.failuresCount
		failureRate := float64(d.failuresCount) / float64(totalCommands)
		rateMet = failureRate >= d.config.FailureRateThreshold
	}

	return countMet && rateMet
}

// Reset resets the failure detector state.
func (d *CommandFailureDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.successCount = 0
	d.failuresCount = 0
	d.windowStart = time.Now()
}

// checkWindow checks if the current window has expired and resets if needed.
// Must be called with lock held.
func (d *CommandFailureDetector) checkWindow() {
	if time.Since(d.windowStart) >= d.config.FailureDetectionWindow {
		d.successCount = 0
		d.failuresCount = 0
		d.windowStart = time.Now()
	}
}

// Stats returns the current failure detector statistics.
func (d *CommandFailureDetector) Stats() (successes, failures int, windowStart time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.successCount, d.failuresCount, d.windowStart
}
