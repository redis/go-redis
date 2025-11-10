package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

// channelSemaphore is a simple semaphore using a buffered channel
type channelSemaphore struct {
	ch chan struct{}
}

func newChannelSemaphore(capacity int) *channelSemaphore {
	return &channelSemaphore{
		ch: make(chan struct{}, capacity),
	}
}

func (s *channelSemaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *channelSemaphore) Acquire(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (s *channelSemaphore) AcquireBlocking() {
	s.ch <- struct{}{}
}

func (s *channelSemaphore) Release() {
	<-s.ch
}

// Benchmarks for FastSemaphore

func BenchmarkFastSemaphore_TryAcquire(b *testing.B) {
	sem := NewFastSemaphore(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	})
}

func BenchmarkFastSemaphore_AcquireRelease(b *testing.B) {
	sem := NewFastSemaphore(100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

func BenchmarkFastSemaphore_Contention(b *testing.B) {
	sem := NewFastSemaphore(10) // Small capacity to create contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

func BenchmarkFastSemaphore_HighContention(b *testing.B) {
	sem := NewFastSemaphore(1) // Very high contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

// Benchmarks for channelSemaphore

func BenchmarkChannelSemaphore_TryAcquire(b *testing.B) {
	sem := newChannelSemaphore(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	})
}

func BenchmarkChannelSemaphore_AcquireRelease(b *testing.B) {
	sem := newChannelSemaphore(100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second)
			sem.Release()
		}
	})
}

func BenchmarkChannelSemaphore_Contention(b *testing.B) {
	sem := newChannelSemaphore(10) // Small capacity to create contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second)
			sem.Release()
		}
	})
}

func BenchmarkChannelSemaphore_HighContention(b *testing.B) {
	sem := newChannelSemaphore(1) // Very high contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second)
			sem.Release()
		}
	})
}

// Benchmark with realistic workload (some work between acquire/release)

func BenchmarkFastSemaphore_WithWork(b *testing.B) {
	sem := NewFastSemaphore(10)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			// Simulate some work
			_ = make([]byte, 64)
			sem.Release()
		}
	})
}

func BenchmarkChannelSemaphore_WithWork(b *testing.B) {
	sem := newChannelSemaphore(10)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second)
			// Simulate some work
			_ = make([]byte, 64)
			sem.Release()
		}
	})
}

// Benchmark mixed TryAcquire and Acquire

func BenchmarkFastSemaphore_Mixed(b *testing.B) {
	sem := NewFastSemaphore(10)
	ctx := context.Background()
	var wg sync.WaitGroup
	
	b.ResetTimer()
	
	// Half goroutines use TryAcquire
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N/2; i++ {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	}()
	
	// Half goroutines use Acquire
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N/2; i++ {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	}()
	
	wg.Wait()
}

func BenchmarkChannelSemaphore_Mixed(b *testing.B) {
	sem := newChannelSemaphore(10)
	ctx := context.Background()
	var wg sync.WaitGroup
	
	b.ResetTimer()
	
	// Half goroutines use TryAcquire
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N/2; i++ {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	}()
	
	// Half goroutines use Acquire
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N/2; i++ {
			sem.Acquire(ctx, time.Second)
			sem.Release()
		}
	}()
	
	wg.Wait()
}

