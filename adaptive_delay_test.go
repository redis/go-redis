package redis

import (
	"testing"
	"time"
)

// TestAdaptiveDelayCalculation tests the adaptive delay calculation logic
func TestAdaptiveDelayCalculation(t *testing.T) {
	tests := []struct {
		name         string
		maxBatchSize int
		maxDelay     time.Duration
		adaptive     bool
		queueLen     int
		expected     time.Duration
	}{
		// Disabled adaptive delay
		{
			name:         "adaptive disabled, returns fixed delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     false,
			queueLen:     50,
			expected:     100 * time.Microsecond,
		},
		{
			name:         "adaptive disabled, zero delay",
			maxBatchSize: 100,
			maxDelay:     0,
			adaptive:     false,
			queueLen:     50,
			expected:     0,
		},

		// Enabled adaptive delay - 75% threshold
		{
			name:         "75% full - no delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     75,
			expected:     0,
		},
		{
			name:         "76% full - no delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     76,
			expected:     0,
		},
		{
			name:         "100% full - no delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     100,
			expected:     0,
		},

		// Enabled adaptive delay - 50% threshold
		{
			name:         "50% full - 25% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     50,
			expected:     25 * time.Microsecond,
		},
		{
			name:         "60% full - 25% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     60,
			expected:     25 * time.Microsecond,
		},
		{
			name:         "74% full - 25% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     74,
			expected:     25 * time.Microsecond,
		},

		// Enabled adaptive delay - 25% threshold
		{
			name:         "25% full - 50% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     25,
			expected:     50 * time.Microsecond,
		},
		{
			name:         "30% full - 50% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     30,
			expected:     50 * time.Microsecond,
		},
		{
			name:         "49% full - 50% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     49,
			expected:     50 * time.Microsecond,
		},

		// Enabled adaptive delay - <25% threshold
		{
			name:         "24% full - 100% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     24,
			expected:     100 * time.Microsecond,
		},
		{
			name:         "10% full - 100% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     10,
			expected:     100 * time.Microsecond,
		},
		{
			name:         "1% full - 100% delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     1,
			expected:     100 * time.Microsecond,
		},

		// Edge cases
		{
			name:         "empty queue - no delay",
			maxBatchSize: 100,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     0,
			expected:     0,
		},
		{
			name:         "zero max delay - no delay",
			maxBatchSize: 100,
			maxDelay:     0,
			adaptive:     true,
			queueLen:     50,
			expected:     0,
		},

		// Different batch sizes
		{
			name:         "small batch - 75% full",
			maxBatchSize: 10,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     8,
			expected:     0,
		},
		{
			name:         "small batch - 50% full",
			maxBatchSize: 10,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     5,
			expected:     25 * time.Microsecond,
		},
		{
			name:         "large batch - 75% full",
			maxBatchSize: 1000,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     750,
			expected:     0,
		},
		{
			name:         "large batch - 50% full",
			maxBatchSize: 1000,
			maxDelay:     100 * time.Microsecond,
			adaptive:     true,
			queueLen:     500,
			expected:     25 * time.Microsecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create autopipeliner with test config
			ap := &AutoPipeliner{
				config: &AutoPipelineConfig{
					MaxBatchSize:  tt.maxBatchSize,
					MaxFlushDelay: tt.maxDelay,
					AdaptiveDelay: tt.adaptive,
				},
			}
			ap.queueLen.Store(int32(tt.queueLen))

			// Calculate delay
			result := ap.calculateDelay()

			// Verify result
			if result != tt.expected {
				t.Errorf("calculateDelay() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestAdaptiveDelayIntegerArithmetic verifies integer arithmetic correctness
func TestAdaptiveDelayIntegerArithmetic(t *testing.T) {
	maxBatch := 100
	maxDelay := 100 * time.Microsecond

	ap := &AutoPipeliner{
		config: &AutoPipelineConfig{
			MaxBatchSize:  maxBatch,
			MaxFlushDelay: maxDelay,
			AdaptiveDelay: true,
		},
	}

	// Test all queue lengths from 0 to maxBatch
	for queueLen := 0; queueLen <= maxBatch; queueLen++ {
		ap.queueLen.Store(int32(queueLen))
		delay := ap.calculateDelay()

		// Verify delay is one of the expected values
		switch {
		case queueLen == 0:
			if delay != 0 {
				t.Errorf("queueLen=%d: expected 0, got %v", queueLen, delay)
			}
		case queueLen*4 >= maxBatch*3: // ≥75%
			if delay != 0 {
				t.Errorf("queueLen=%d (≥75%%): expected 0, got %v", queueLen, delay)
			}
		case queueLen*2 >= maxBatch: // ≥50%
			if delay != maxDelay/4 {
				t.Errorf("queueLen=%d (≥50%%): expected %v, got %v", queueLen, maxDelay/4, delay)
			}
		case queueLen*4 >= maxBatch: // ≥25%
			if delay != maxDelay/2 {
				t.Errorf("queueLen=%d (≥25%%): expected %v, got %v", queueLen, maxDelay/2, delay)
			}
		default: // <25%
			if delay != maxDelay {
				t.Errorf("queueLen=%d (<25%%): expected %v, got %v", queueLen, maxDelay, delay)
			}
		}
	}
}

// BenchmarkCalculateDelay benchmarks the delay calculation
func BenchmarkCalculateDelay(b *testing.B) {
	ap := &AutoPipeliner{
		config: &AutoPipelineConfig{
			MaxBatchSize:  100,
			MaxFlushDelay: 100 * time.Microsecond,
			AdaptiveDelay: true,
		},
	}
	ap.queueLen.Store(50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ap.calculateDelay()
	}
}

// BenchmarkCalculateDelayDisabled benchmarks with adaptive delay disabled
func BenchmarkCalculateDelayDisabled(b *testing.B) {
	ap := &AutoPipeliner{
		config: &AutoPipelineConfig{
			MaxBatchSize:  100,
			MaxFlushDelay: 100 * time.Microsecond,
			AdaptiveDelay: false,
		},
	}
	ap.queueLen.Store(50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ap.calculateDelay()
	}
}

