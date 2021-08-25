package pool

import (
	"context"
	"time"
)

type Semaphore struct {
	queue chan struct{}
}

func NewSemaphore(max int) *Semaphore {
	return &Semaphore{
		queue: make(chan struct{}, max),
	}
}

func (sem *Semaphore) Acquire() {
	sem.queue <- struct{}{}
}

func (sem *Semaphore) Wait(ctx context.Context, timeout time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case sem.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(timeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case sem.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		return ErrPoolTimeout
	}
}

func (sem *Semaphore) Release() {
	<-sem.queue
}
