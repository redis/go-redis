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
	return &ReAuthPoolHook{
		shouldReAuth:  make(map[uint64]func(error)),
		workers:       make(chan struct{}, poolSize),
		reAuthTimeout: reAuthTimeout,
	}

}

func (r *ReAuthPoolHook) MarkForReAuth(connID uint64, reAuthFn func(error)) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.shouldReAuth[connID] = reAuthFn
}

func (r *ReAuthPoolHook) ShouldReAuth(connID uint64) bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	_, ok := r.shouldReAuth[connID]
	return ok
}

func (r *ReAuthPoolHook) ClearReAuthMark(connID uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.shouldReAuth, connID)
}

func (r *ReAuthPoolHook) OnGet(_ context.Context, _ *pool.Conn, _ bool) error {
	// noop
	return nil
}

func (r *ReAuthPoolHook) OnPut(_ context.Context, conn *pool.Conn) (bool, bool, error) {
	if reAuthFn, ok := r.shouldReAuth[conn.GetID()]; ok {
		go func() {
			<-r.workers
			var err error
			timeout := time.After(r.reAuthTimeout)
			for !conn.Usable.CompareAndSwap(true, false) {
				select {
				case <-timeout:
					err = pool.ErrConnUnusableTimeout
				default:
					time.Sleep(time.Millisecond)
					// connection closed, cannot re-authenticate
				}
			}

			reAuthFn(err)

			conn.Usable.Store(true)
			r.workers <- struct{}{}
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
