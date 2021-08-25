package pool

import (
	"container/list"
	"context"
	"crypto/sha1"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
)

const (
	idleStatsRecent = 10
)

type FifoConnPool struct {
	Pooler

	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	waitTurn *Semaphore

	connsMu      sync.Mutex
	conns        []*Conn
	idleConns    list.List
	poolSize     int
	idleConnsLen int
	idleStats    []int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

func NewFifoConnPool(opt *Options) *FifoConnPool {
	p := &FifoConnPool{
		opt: opt,

		waitTurn:  NewSemaphore(opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleStats: make([]int, idleStatsRecent),
		closedCh:  make(chan struct{}),
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.idleCounter(opt.IdleTimeout)
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (pool *FifoConnPool) checkMinIdleConns() {
	// should lock outside
	if pool.opt.MinIdleConns == 0 {
		return
	}
	for pool.poolSize < pool.opt.PoolSize && pool.idleConnsLen < pool.opt.MinIdleConns {
		pool.poolSize++
		pool.idleConnsLen++
		go func() {
			err := pool.addIdleConn()
			if err != nil {
				pool.connsMu.Lock()
				pool.poolSize--
				pool.idleConnsLen--
				pool.connsMu.Unlock()
			}
		}()
	}
}

func (pool *FifoConnPool) addIdleConn() error {
	cn, err := pool.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	pool.connsMu.Lock()
	pool.conns = append(pool.conns, cn)
	pool.idleConns.PushBack(cn)
	pool.connsMu.Unlock()
	return nil
}

func (pool *FifoConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if pool.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&pool.dialErrorsNum) >= uint32(pool.opt.PoolSize) {
		return nil, pool.getLastDialError()
	}

	netConn, err := pool.opt.Dialer(ctx)
	if err != nil {
		pool.setLastDialError(err)
		if atomic.AddUint32(&pool.dialErrorsNum, 1) == uint32(pool.opt.PoolSize) {
			go pool.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)
	cn.pooled = pooled
	return cn, nil
}

func (pool *FifoConnPool) tryDial() {
	for {
		if pool.closed() {
			return
		}

		conn, err := pool.opt.Dialer(context.Background())
		if err != nil {
			pool.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&pool.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (pool *FifoConnPool) getLastDialError() error {
	err, _ := pool.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

func (pool *FifoConnPool) setLastDialError(err error) {
	pool.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (pool *FifoConnPool) closed() bool {
	return atomic.LoadUint32(&pool._closed) == 1
}

func (pool *FifoConnPool) Filter(fn func(*Conn) bool) error {
	pool.connsMu.Lock()
	defer pool.connsMu.Unlock()

	var firstErr error
	for _, cn := range pool.conns {
		if fn(cn) {
			if err := pool.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (pool *FifoConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return pool.newConn(ctx, false)
}

func (pool *FifoConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := pool.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	pool.connsMu.Lock()
	pool.conns = append(pool.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if pool.poolSize >= pool.opt.PoolSize {
			cn.pooled = false
		} else {
			pool.poolSize++
		}
	}
	pool.connsMu.Unlock()

	return cn, nil
}

func (pool *FifoConnPool) CloseConn(cn *Conn) error {
	pool.removeConnWithLock(cn)
	return pool.closeConn(cn)
}

func (pool *FifoConnPool) Get(ctx context.Context) (*Conn, error) {
	if pool.closed() {
		return nil, ErrClosed
	}

	if err := pool.waitTurn.Wait(ctx, pool.opt.PoolTimeout); err != nil {
		if err == ErrPoolTimeout {
			atomic.AddUint32(&pool.stats.Timeouts, 1)
		}
		return nil, err
	}

	for {
		pool.connsMu.Lock()
		cn := pool.popIdle()
		pool.connsMu.Unlock()

		if cn == nil {
			break
		}

		if pool.isMaxLifeConn(cn) {
			_ = pool.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&pool.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&pool.stats.Misses, 1)

	newConn, err := pool.newConn(ctx, true)
	if err != nil {
		pool.waitTurn.Release()
		return nil, err
	}

	return newConn, nil
}

func (pool *FifoConnPool) popIdle() *Conn {
	if pool.idleConns.Len() == 0 {
		return nil
	}

	elem := pool.idleConns.Front()
	pool.idleConns.Remove(elem)
	pool.idleConnsLen--
	pool.checkMinIdleConns()
	return elem.Value.(*Conn)
}

func (pool *FifoConnPool) Put(ctx context.Context, cn *Conn) {
	if cn.rd.Buffered() > 0 {
		internal.Logger.Printf(ctx, "Conn has unread data")
		pool.Remove(ctx, cn, BadConnError{})
		return
	}

	if !cn.pooled {
		pool.Remove(ctx, cn, nil)
		return
	}

	pool.connsMu.Lock()
	pool.idleConns.PushBack(cn)
	pool.idleConnsLen++
	pool.connsMu.Unlock()
	pool.waitTurn.Release()
}

func (pool *FifoConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	pool.removeConnWithLock(cn)
	pool.waitTurn.Release()
	_ = pool.closeConn(cn)
}

func (pool *FifoConnPool) Len() int {
	pool.connsMu.Lock()
	n := len(pool.conns)
	pool.connsMu.Unlock()
	return n
}

func (pool *FifoConnPool) IdleLen() int {
	pool.connsMu.Lock()
	n := pool.idleConnsLen
	pool.connsMu.Unlock()
	return n
}

func (pool *FifoConnPool) Stats() *Stats {
	idleLen := pool.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&pool.stats.Hits),
		Misses:   atomic.LoadUint32(&pool.stats.Misses),
		Timeouts: atomic.LoadUint32(&pool.stats.Timeouts),

		TotalConns: uint32(pool.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&pool.stats.StaleConns),
	}
}

func (pool *FifoConnPool) closeConn(cn *Conn) error {
	if pool.opt.OnClose != nil {
		_ = pool.opt.OnClose(cn)
	}
	return cn.Close()
}

func (pool *FifoConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&pool._closed, 0, 1) {
		return ErrClosed
	}
	close(pool.closedCh)

	var firstErr error
	pool.connsMu.Lock()
	for _, cn := range pool.conns {
		if err := pool.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	pool.conns = nil
	pool.poolSize = 0
	pool.idleConns = list.List{}
	pool.idleConnsLen = 0
	pool.connsMu.Unlock()

	return firstErr
}

func (pool *FifoConnPool) idleCounter(idleTimeout time.Duration) {
	ticker := time.NewTicker(idleTimeout / idleStatsRecent)
	defer ticker.Stop()

	idx := 0
	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if pool.closed() {
				return
			}

			pool.connsMu.Lock()
			pool.idleStats[idx] = pool.idleConnsLen
			pool.connsMu.Unlock()

			idx++
			idx %= idleStatsRecent
		case <-pool.closedCh:
			return
		}
	}
}

func (pool *FifoConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if pool.closed() {
				return
			}
			_, err := pool.ReapStaleConns()
			if err != nil {
				internal.Logger.Printf(context.Background(), "ReapStaleConns failed: %s", err)
				continue
			}
		case <-pool.closedCh:
			return
		}
	}
}

func (pool *FifoConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		pool.waitTurn.Acquire()

		pool.connsMu.Lock()
		cn := pool.reapStaleConn()
		pool.connsMu.Unlock()

		pool.waitTurn.Release()

		if cn != nil {
			_ = pool.closeConn(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&pool.stats.StaleConns, uint32(n))
	return n, nil
}

func (pool *FifoConnPool) minIdleRecent() int {
	min := pool.idleConnsLen
	for _, n := range pool.idleStats {
		if n < min {
			min = n
		}
	}
	return min
}

func (pool *FifoConnPool) reapStaleConn() *Conn {
	if pool.idleConns.Len() == 0 {
		return nil
	}

	minIdleCur := pool.idleConnsLen
	elem := pool.idleConns.Front()
	cn := elem.Value.(*Conn)

	now := time.Now()
	isIdle := false
	if pool.opt.IdleTimeout > 0 && now.Sub(cn.createdAt) >= pool.opt.IdleTimeout {
		minIdleRecent := pool.minIdleRecent()
		if minIdleCur < minIdleRecent {
			minIdleRecent = minIdleCur
		}

		if minIdleRecent > pool.opt.MinIdleConns {
			isIdle = true
		}
	}

	if !isIdle && !pool.isMaxLifeConn(cn) {
		return nil
	}

	pool.removeConn(cn)
	pool.idleConnsLen--
	pool.idleConns.Remove(elem)
	return cn
}

func (pool *FifoConnPool) removeConnWithLock(cn *Conn) {
	pool.connsMu.Lock()
	defer pool.connsMu.Unlock()
	pool.removeConn(cn)
}

func (pool *FifoConnPool) removeConn(cn *Conn) {
	for i, c := range pool.conns {
		if c == cn {
			pool.conns = append(pool.conns[:i], pool.conns[i+1:]...)
			if cn.pooled {
				pool.poolSize--
				pool.checkMinIdleConns()
			}
			return
		}
	}
}

func (pool *FifoConnPool) isMaxLifeConn(cn *Conn) bool {
	if pool.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	minAge := pool.opt.MaxConnAge * 8 / 10
	maxAge := pool.opt.MaxConnAge
	localAddr := "dummy"
	if cn.netConn != nil && cn.netConn.LocalAddr() != nil {
		localAddr = cn.netConn.LocalAddr().String()
	}

	sum := sha1.Sum([]byte(localAddr))
	age := minAge + (maxAge-minAge)*time.Duration(sum[0])/255
	return now.Sub(cn.createdAt) >= age
}
