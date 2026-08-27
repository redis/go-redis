package callbackqueue

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9/internal"
)

// CallbackQueue runs queued callbacks on a single goroutine, in FIFO order.
// The drain goroutine is spawned on demand and exits when the queue empties,
// so an idle queue holds no goroutine.
type CallbackQueue struct {
	mu       sync.Mutex
	queue    []func()
	draining bool
}

func (q *CallbackQueue) Dispatch(fn func()) {
	q.mu.Lock()
	q.queue = append(q.queue, fn)
	if q.draining {
		q.mu.Unlock()
		return
	}
	q.draining = true
	q.mu.Unlock()
	go q.drain()
}

func (q *CallbackQueue) drain() {
	for {
		q.mu.Lock()
		if len(q.queue) == 0 {
			q.draining = false
			q.mu.Unlock()
			return
		}
		fn := q.queue[0]
		q.queue = q.queue[1:]
		q.mu.Unlock()
		runCallbackSafely(fn)
	}
}

// runCallbackSafely keeps a panicking callback from crashing the process
// (callbacks run on a library-owned goroutine) and from wedging the queue in
// the draining state.
func runCallbackSafely(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(), "callback queue error: the callback panicked: %v", r)
		}
	}()
	fn()
}
