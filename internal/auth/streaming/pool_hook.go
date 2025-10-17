package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type ReAuthPoolHook struct {
	// conn id -> func() reauth func with error handling
	shouldReAuth  map[uint64]func(error)
	lock          sync.RWMutex
	workers       chan struct{}
	reAuthTimeout time.Duration
}

func NewReAuthPoolHook(poolSize int, reAuthTimeout time.Duration) *ReAuthPoolHook {
	workers := make(chan struct{}, poolSize)
	// Initialize the workers channel with tokens (semaphore pattern)
	for i := 0; i < poolSize; i++ {
		workers <- struct{}{}
	}

	return &ReAuthPoolHook{
		shouldReAuth:  make(map[uint64]func(error)),
		workers:       workers,
		reAuthTimeout: reAuthTimeout,
	}

}

func (r *ReAuthPoolHook) MarkForReAuth(connID uint64, reAuthFn func(error)) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.shouldReAuth[connID] = reAuthFn
}

func (r *ReAuthPoolHook) ClearReAuthMark(connID uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.shouldReAuth, connID)
}

func (r *ReAuthPoolHook) OnGet(_ context.Context, conn *pool.Conn, _ bool) error {
	// This connection was marked for reauth while in the pool,
	// so we need to reauth it before returning it to the user.
	r.lock.RLock()
	reAuthFn, ok := r.shouldReAuth[conn.GetID()]
	r.lock.RUnlock()
	if ok {
		// Clear the mark immediately to prevent duplicate reauth attempts
		r.ClearReAuthMark(conn.GetID())
		reAuthFn(nil)
	}
	return nil
}

func (r *ReAuthPoolHook) OnPut(_ context.Context, conn *pool.Conn) (bool, bool, error) {
	// Check if reauth is needed and get the function with proper locking
	r.lock.RLock()
	reAuthFn, ok := r.shouldReAuth[conn.GetID()]
	r.lock.RUnlock()

	if ok {
		// Clear the mark immediately to prevent duplicate reauth attempts
		r.ClearReAuthMark(conn.GetID())

		go func() {
			<-r.workers
			defer func() {
				r.workers <- struct{}{}
			}()

			var err error
			timeout := time.After(r.reAuthTimeout)

			// Try to acquire the connection (set Usable to false)
			for !conn.Usable.CompareAndSwap(true, false) {
				select {
				case <-timeout:
					// Timeout occurred, cannot acquire connection
					err = pool.ErrConnUnusableTimeout
					reAuthFn(err)
					return
				default:
					time.Sleep(time.Millisecond)
				}
			}

			// Successfully acquired the connection, perform reauth
			reAuthFn(nil)

			// Release the connection
			conn.Usable.Store(true)
		}()
	}

	// the reauth will happen in background, as far as the pool is concerned:
	// pool the connection, don't remove it, no error
	return true, false, nil
}

func (r *ReAuthPoolHook) OnRemove(_ context.Context, conn *pool.Conn, _ error) {
	r.ClearReAuthMark(conn.GetID())
}

var _ pool.PoolHook = (*ReAuthPoolHook)(nil)
