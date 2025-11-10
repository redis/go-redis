package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

// channelSemaphore is a simple semaphore using a buffered channel
// The channel is pre-filled with tokens. Acquire = receive, Release = send.
// This allows closing the channel to unblock all waiters.
type channelSemaphore struct {
	ch chan struct{}
}

func newChannelSemaphore(capacity int) *channelSemaphore {
	ch := make(chan struct{}, capacity)
	// Pre-fill with tokens
	for i := 0; i < capacity; i++ {
		ch <- struct{}{}
	}
	return &channelSemaphore{
		ch: ch,
	}
}

func (s *channelSemaphore) TryAcquire() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

func (s *channelSemaphore) Acquire(ctx context.Context, timeout time.Duration) error {
	// Try fast path first (no timer needed)
	select {
	case <-s.ch:
		return nil
	default:
	}

	// Slow path: need to wait with timeout
	timer := semTimers.Get().(*time.Timer)
	defer semTimers.Put(timer)
	timer.Reset(timeout)

	select {
	case <-s.ch:
		if !timer.Stop() {
			<-timer.C
		}
		return nil
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (s *channelSemaphore) AcquireBlocking() {
	<-s.ch
}

func (s *channelSemaphore) Release() {
	s.ch <- struct{}{}
}

func (s *channelSemaphore) Close() {
	close(s.ch)
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

// Benchmarks for FIFOSemaphore

func BenchmarkFIFOSemaphore_TryAcquire(b *testing.B) {
	sem := NewFIFOSemaphore(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	})
}

func BenchmarkFIFOSemaphore_AcquireRelease(b *testing.B) {
	sem := NewFIFOSemaphore(100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

func BenchmarkFIFOSemaphore_Contention(b *testing.B) {
	sem := NewFIFOSemaphore(10) // Small capacity to create contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

func BenchmarkFIFOSemaphore_HighContention(b *testing.B) {
	sem := NewFIFOSemaphore(1) // Very high contention
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx, time.Second, context.DeadlineExceeded)
			sem.Release()
		}
	})
}

func BenchmarkFIFOSemaphore_WithWork(b *testing.B) {
	sem := NewFIFOSemaphore(10)
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

func BenchmarkFIFOSemaphore_Mixed(b *testing.B) {
	sem := NewFIFOSemaphore(10)
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

