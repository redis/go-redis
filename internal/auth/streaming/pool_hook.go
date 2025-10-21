package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type ReAuthPoolHook struct {
	// conn id -> func() reauth func with error handling
	shouldReAuth     map[uint64]func(error)
	shouldReAuthLock sync.RWMutex
	workers          chan struct{}
	reAuthTimeout time.Duration
	// conn id -> bool
	scheduledReAuth map[uint64]bool
	scheduledLock   sync.RWMutex
}

func NewReAuthPoolHook(poolSize int, reAuthTimeout time.Duration) *ReAuthPoolHook {
	workers := make(chan struct{}, poolSize)
	// Initialize the workers channel with tokens (semaphore pattern)
	for i := 0; i < poolSize; i++ {
		workers <- struct{}{}
	}

	return &ReAuthPoolHook{
		shouldReAuth:    make(map[uint64]func(error)),
		scheduledReAuth: make(map[uint64]bool),
		workers:         workers,
		reAuthTimeout:   reAuthTimeout,
	}

}

func (r *ReAuthPoolHook) MarkForReAuth(connID uint64, reAuthFn func(error)) {
	r.shouldReAuthLock.Lock()
	defer r.shouldReAuthLock.Unlock()
	r.shouldReAuth[connID] = reAuthFn
}

func (r *ReAuthPoolHook) ClearReAuthMark(connID uint64) {
	r.shouldReAuthLock.Lock()
	defer r.shouldReAuthLock.Unlock()
	delete(r.shouldReAuth, connID)
}

func (r *ReAuthPoolHook) OnGet(_ context.Context, conn *pool.Conn, _ bool) (accept bool, err error) {
	r.shouldReAuthLock.RLock()
	_, ok := r.shouldReAuth[conn.GetID()]
	r.shouldReAuthLock.RUnlock()
	// This connection was marked for reauth while in the pool,
	// reject the connection
	if ok {
		// simply reject the connection, it will be re-authenticated in OnPut
		return false, nil
	}
	r.scheduledLock.RLock()
	hasScheduled, ok := r.scheduledReAuth[conn.GetID()]
	r.scheduledLock.RUnlock()
	// has scheduled reauth, reject the connection
	if ok && hasScheduled {
		// simply reject the connection, it will be re-authenticated in OnPut
		return false, nil
	}
	return true, nil
}

func (r *ReAuthPoolHook) OnPut(_ context.Context, conn *pool.Conn) (bool, bool, error) {
	// Check if reauth is needed and get the function with proper locking
	r.shouldReAuthLock.RLock()
	reAuthFn, ok := r.shouldReAuth[conn.GetID()]
	r.shouldReAuthLock.RUnlock()

	if ok {
		r.scheduledLock.Lock()
		r.scheduledReAuth[conn.GetID()] = true
		r.scheduledLock.Unlock()
		// Clear the mark immediately to prevent duplicate reauth attempts
		r.ClearReAuthMark(conn.GetID())
		go func() {
			<-r.workers
			defer func() {
				r.scheduledLock.Lock()
				delete(r.scheduledReAuth, conn.GetID())
				r.scheduledLock.Unlock()
				r.workers <- struct{}{}
			}()

			var err error
			timeout := time.After(r.reAuthTimeout)

			// Try to acquire the connection
			// We need to ensure the connection is both Usable and not Used
			// to prevent data races with concurrent operations
			const baseDelay = time.Microsecond
			acquired := false
			attempt := 0
			for !acquired {
				select {
				case <-timeout:
					// Timeout occurred, cannot acquire connection
					err = pool.ErrConnUnusableTimeout
					reAuthFn(err)
					return
				default:
					// Try to acquire: set Usable=false, then check Used
					if conn.Usable.CompareAndSwap(true, false) {
						if !conn.Used.Load() {
							acquired = true
						} else {
							// Release Usable and retry with exponential backoff
							conn.Usable.Store(true)
							if attempt > 0 {
								// Exponential backoff: 1, 2, 4, 8... up to 512 microseconds
								delay := baseDelay * time.Duration(1<<uint(attempt%10)) // Cap exponential growth
								time.Sleep(delay)
							}
							attempt++
						}
					} else {
						// Connection not usable, retry with exponential backoff
						if attempt > 0 {
							// Exponential backoff: 1, 2, 4, 8... up to 512 microseconds
							delay := baseDelay * time.Duration(1<<uint(attempt%10)) // Cap exponential growth
							time.Sleep(delay)
						}
						attempt++
					}
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
	r.scheduledLock.Lock()
	delete(r.scheduledReAuth, conn.GetID())
	r.scheduledLock.Unlock()
	r.shouldReAuthLock.Lock()
	delete(r.shouldReAuth, conn.GetID())
	r.shouldReAuthLock.Unlock()
	r.ClearReAuthMark(conn.GetID())
}

var _ pool.PoolHook = (*ReAuthPoolHook)(nil)
