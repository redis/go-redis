// Package failuredetector provides primitives for deciding when a Redis
// database is unhealthy enough that the multi-database client should trigger
// a failover.
package failuredetector

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// FailureDetector decides when failover should be triggered based on a stream
// of command outcomes observed by the caller.
type FailureDetector interface {
	// RecordSuccess records a successful command outcome.
	RecordSuccess()
	// RecordFailure records a failed command outcome. Implementations may
	// ignore errors that are not health signals (for example client-side
	// context cancellation).
	RecordFailure(err error)
	// ShouldFailover returns true when the recent outcomes indicate that
	// failover should be triggered.
	ShouldFailover() bool
	// Reset discards all observed outcomes and starts fresh.
	Reset()
}

// CommandFailureDetectorConfig configures CommandFailureDetector.
type CommandFailureDetectorConfig struct {
	// MinNumFailures is the minimum number of failed commands that must be
	// observed within the detection window before failover is considered.
	// A value of 0 means the failure count is ignored and only
	// FailureRateThreshold is taken into account.
	// Default: 1000
	MinNumFailures uint64

	// FailureRateThreshold is the failure rate (0.0-1.0) that, together with
	// MinNumFailures, triggers failover. For example, 0.1 means failover when
	// 10% or more of the commands in the window fail.
	// A value of 0.0 means the rate is ignored and only MinNumFailures is
	// taken into account.
	// Default: 0.1
	FailureRateThreshold float64

	// FailureDetectionWindow is the sliding time window over which command
	// outcomes are considered. Outcomes older than FailureDetectionWindow
	// from now are no longer counted by ShouldFailover.
	// Default: 2 seconds.
	FailureDetectionWindow time.Duration

	// NumBuckets controls the time resolution of the sliding window. The
	// window is divided into NumBuckets sub-buckets, each of width
	// FailureDetectionWindow / NumBuckets, and outcomes age out one bucket
	// at a time. Larger values give finer-grained ageing at the cost of
	// O(NumBuckets) work per ShouldFailover call.
	// Default: 10.
	NumBuckets int
}

// DefaultCommandFailureDetectorConfig returns the default configuration.
func DefaultCommandFailureDetectorConfig() CommandFailureDetectorConfig {
	return CommandFailureDetectorConfig{
		MinNumFailures:         1000,
		FailureRateThreshold:   0.1,
		FailureDetectionWindow: defaultFailureDetectionWindow,
		NumBuckets:             defaultNumBuckets,
	}
}

const (
	defaultFailureDetectionWindow = 2 * time.Second
	defaultNumBuckets             = 10
)

// bucket holds the outcomes recorded inside a single sub-bucket of the
// sliding window. All fields are accessed atomically so the detector is
// lock-free on the hot path.
//
// epochNano is the start time of the bucket's current "lap" around the ring,
// expressed in nanoseconds since the Unix epoch. When the ring wraps around
// and a writer revisits a bucket whose epochNano belongs to a previous lap,
// the writer claims the bucket via CompareAndSwap on epochNano and then
// zeroes the counters. Readers ignore any bucket whose epochNano falls
// outside the current window.
type bucket struct {
	epochNano atomic.Int64
	successes atomic.Uint64
	failures  atomic.Uint64
}

// CommandFailureDetector observes command outcomes inside a sliding time
// window and reports when failover should be triggered. The implementation
// uses a fixed-size ring of buckets and only sync/atomic operations on the
// hot path, so RecordSuccess and RecordFailure scale across many goroutines
// without contention.
type CommandFailureDetector struct {
	config          CommandFailureDetectorConfig
	buckets         []bucket
	bucketWidthNano int64
	windowNano      int64
	now             func() time.Time // injectable for tests
}

// NewCommandFailureDetector creates a new sliding-window failure detector
// with the given configuration. Zero values for FailureDetectionWindow or
// NumBuckets fall back to the package defaults; the threshold fields
// (MinNumFailures, FailureRateThreshold) are intentionally not defaulted
// because a zero value carries the documented "disabled" meaning.
func NewCommandFailureDetector(config CommandFailureDetectorConfig) *CommandFailureDetector {
	if config.FailureDetectionWindow <= 0 {
		config.FailureDetectionWindow = defaultFailureDetectionWindow
	}
	if config.NumBuckets <= 0 {
		config.NumBuckets = defaultNumBuckets
	}
	return &CommandFailureDetector{
		config:          config,
		buckets:         make([]bucket, config.NumBuckets),
		bucketWidthNano: int64(config.FailureDetectionWindow) / int64(config.NumBuckets),
		windowNano:      int64(config.FailureDetectionWindow),
		now:             time.Now,
	}
}

// RecordSuccess records a successful command outcome.
func (d *CommandFailureDetector) RecordSuccess() {
	d.bucketFor(d.now().UnixNano()).successes.Add(1)
}

// RecordFailure records a failed command outcome. A nil error is treated as
// a no-op (so callers that forward errors unconditionally do not accumulate
// phantom failures); context cancellation and deadline-exceeded errors are
// also ignored because they originate on the client side and are not a
// signal about the database's health.
func (d *CommandFailureDetector) RecordFailure(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	d.bucketFor(d.now().UnixNano()).failures.Add(1)
}

// ShouldFailover returns true when the outcomes observed within the trailing
// FailureDetectionWindow indicate that failover should be triggered. A
// database is considered faulty when at least MinNumFailures commands have
// failed AND the observed failure rate is at least FailureRateThreshold; a
// zero value for either threshold disables that half of the check.
// At least one failure must have been observed for failover to be considered.
func (d *CommandFailureDetector) ShouldFailover() bool {
	successes, failures := d.snapshot()

	if failures == 0 {
		return false
	}

	countMet := d.config.MinNumFailures == 0 || failures >= d.config.MinNumFailures

	rateMet := true
	if d.config.FailureRateThreshold > 0 {
		total := successes + failures
		failureRate := float64(failures) / float64(total)
		rateMet = failureRate >= d.config.FailureRateThreshold
	}

	return countMet && rateMet
}

// Reset discards all recorded outcomes. Concurrent recorders may race with
// Reset; in the worst case a small number of in-flight increments survive
// the reset, which is acceptable for a failure detector.
func (d *CommandFailureDetector) Reset() {
	for i := range d.buckets {
		b := &d.buckets[i]
		b.epochNano.Store(0)
		b.successes.Store(0)
		b.failures.Store(0)
	}
}

// Stats returns a read-only snapshot of the outcomes observed within the
// current sliding window. The returned counts are aggregated across the
// bucket ring and reflect the same view of state used by ShouldFailover.
func (d *CommandFailureDetector) Stats() (successes, failures uint64) {
	return d.snapshot()
}

// bucketFor returns the bucket that owns the supplied nanosecond timestamp,
// initialising it (resetting counters and stamping the new epoch) if a
// previous lap of the ring left stale data in that slot.
func (d *CommandFailureDetector) bucketFor(nowNano int64) *bucket {
	bucketStart := nowNano - (nowNano % d.bucketWidthNano)
	idx := (bucketStart / d.bucketWidthNano) % int64(len(d.buckets))
	b := &d.buckets[idx]

	for {
		current := b.epochNano.Load()
		if current == bucketStart {
			return b
		}
		if current > bucketStart {
			// Clock skew or a concurrent writer already moved this bucket
			// past the current instant; tolerate it.
			return b
		}
		if b.epochNano.CompareAndSwap(current, bucketStart) {
			// We claimed the stale bucket. Zero the counters so concurrent
			// writers that race past the epoch update only contribute to
			// the new lap.
			b.successes.Store(0)
			b.failures.Store(0)
			return b
		}
		// Another writer won the race; reload and decide again.
	}
}

// snapshot sums the outcomes across every bucket whose time slot overlaps
// the trailing window. A bucket spans [epoch, epoch + bucketWidth) and is
// included when (epoch + bucketWidth) > (now - window), i.e. when
// epoch > now - window - bucketWidth. The cutoff is precomputed below.
//
// The sum is not atomic across buckets, which is acceptable for a failure
// detector: a snapshot can interleave with concurrent writers, but the
// aggregated counts only ever undercount the true value by at most the
// in-flight writes.
func (d *CommandFailureDetector) snapshot() (successes, failures uint64) {
	nowNano := d.now().UnixNano()
	cutoff := nowNano - d.windowNano - d.bucketWidthNano
	for i := range d.buckets {
		b := &d.buckets[i]
		if b.epochNano.Load() > cutoff {
			successes += b.successes.Load()
			failures += b.failures.Load()
		}
	}
	return successes, failures
}
