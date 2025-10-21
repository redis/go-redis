package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

type ReAuthPoolHook struct {
	// conn id -> func() reauth func with error handling
	shouldReAuth     map[uint64]func(error)
	shouldReAuthLock sync.RWMutex
	workers          chan struct{}
	reAuthTimeout    time.Duration
	// conn id -> bool
	scheduledReAuth map[uint64]bool
	scheduledLock   sync.RWMutex

	// for cleanup
	manager *Manager
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

func (r *ReAuthPoolHook) OnGet(_ context.Context, conn *pool.Conn, _ bool) (accept bool, err error) {
	connID := conn.GetID()
	r.shouldReAuthLock.RLock()
	_, shouldReAuth := r.shouldReAuth[connID]
	r.shouldReAuthLock.RUnlock()
	// This connection was marked for reauth while in the pool,
	// reject the connection
	if shouldReAuth {
		// simply reject the connection, it will be re-authenticated in OnPut
		return false, nil
	}
	r.scheduledLock.RLock()
	_, hasScheduled := r.scheduledReAuth[connID]
	r.scheduledLock.RUnlock()
	// has scheduled reauth, reject the connection
	if hasScheduled {
		// simply reject the connection, it currently has a reauth scheduled
		// and the worker is waiting for slot to execute the reauth
		return false, nil
	}
	return true, nil
}

func (r *ReAuthPoolHook) OnPut(_ context.Context, conn *pool.Conn) (bool, bool, error) {
	if conn == nil {
		// noop
		return true, false, nil
	}
	connID := conn.GetID()
	// Check if reauth is needed and get the function with proper locking
	r.shouldReAuthLock.RLock()
	reAuthFn, ok := r.shouldReAuth[connID]
	r.shouldReAuthLock.RUnlock()

	if ok {
		// Acquire both locks to atomically move from shouldReAuth to scheduledReAuth
		// This prevents race conditions where OnGet might miss the transition
		r.shouldReAuthLock.Lock()
		r.scheduledLock.Lock()
		r.scheduledReAuth[connID] = true
		delete(r.shouldReAuth, connID)
		r.scheduledLock.Unlock()
		r.shouldReAuthLock.Unlock()
		go func() {
			<-r.workers
			// safety first
			if conn == nil || (conn != nil && conn.IsClosed()) {
				r.workers <- struct{}{}
				return
			}
			defer func() {
				if rec := recover(); rec != nil {
					// once again - safety first
					internal.Logger.Printf(context.Background(), "panic in reauth worker: %v", rec)
				}
				r.scheduledLock.Lock()
				delete(r.scheduledReAuth, connID)
				r.scheduledLock.Unlock()
				r.workers <- struct{}{}
			}()

			var err error
			timeout := time.After(r.reAuthTimeout)

			// Try to acquire the connection
			// We need to ensure the connection is both Usable and not Used
			// to prevent data races with concurrent operations
			const baseDelay = 10 * time.Microsecond
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
					if conn.CompareAndSwapUsable(true, false) {
						if !conn.IsUsed() {
							acquired = true
						} else {
							// Release Usable and retry with exponential backoff
							// todo(ndyakov): think of a better way to do this without the need
							// to release the connection, but just wait till it is not used
							conn.SetUsable(true)
						}
					}
					if !acquired {
						// Exponential backoff: 10, 20, 40, 80... up to 5120 microseconds
						delay := baseDelay * time.Duration(1<<uint(attempt%10)) // Cap exponential growth
						time.Sleep(delay)
						attempt++
					}
				}
			}

			// safety first
			if !conn.IsClosed() {
				// Successfully acquired the connection, perform reauth
				reAuthFn(nil)
			}

			// Release the connection
			conn.SetUsable(true)
		}()
	}

	// the reauth will happen in background, as far as the pool is concerned:
	// pool the connection, don't remove it, no error
	return true, false, nil
}

func (r *ReAuthPoolHook) OnRemove(_ context.Context, conn *pool.Conn, _ error) {
	connID := conn.GetID()
	r.shouldReAuthLock.Lock()
	r.scheduledLock.Lock()
	delete(r.scheduledReAuth, connID)
	delete(r.shouldReAuth, connID)
	r.scheduledLock.Unlock()
	r.shouldReAuthLock.Unlock()
	if r.manager != nil {
		r.manager.RemoveListener(connID)
	}
}

var _ pool.PoolHook = (*ReAuthPoolHook)(nil)
