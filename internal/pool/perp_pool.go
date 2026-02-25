package pool

import (
	"runtime"
	"sync"
	"sync/atomic"
	_ "unsafe" // for go:linkname
)

// PerPIdleConns is a per-P idle connection pool that minimizes contention
// by using per-processor local storage, similar to sync.Pool.
// Unlike sync.Pool, connections are never dropped during GC.
type PerPIdleConns struct {
	// Per-P local pools. Each P has its own pool to minimize contention.
	// The array is sized to GOMAXPROCS at initialization time.
	local     atomic.Pointer[[]perpLocal] // *[]perpLocal
	localSize atomic.Int32                // len(local)

	// Shared overflow pool for when local pools are empty/full
	shared struct {
		mu    sync.Mutex
		conns []*Conn
	}

	maxIdleConns int32
}

// perpLocal is the per-P local pool
type perpLocal struct {
	// private is a single connection that can be accessed without locking
	// Only the owning P can access this field
	private atomic.Pointer[Conn]

	// shared is a small buffer of connections for this P
	// Protected by mu
	mu    sync.Mutex
	conns []*Conn

	// Padding to prevent false sharing between perpLocal instances
	// Cache line is typically 64 bytes on modern CPUs
	_ [64 - 32]byte // Adjust based on actual struct size
}

// NewPerPIdleConns creates a new per-P idle connection pool
func NewPerPIdleConns(maxIdleConns int32) *PerPIdleConns {
	p := &PerPIdleConns{
		maxIdleConns: maxIdleConns,
	}
	p.shared.conns = make([]*Conn, 0, maxIdleConns)
	p.init()
	return p
}

// init initializes the per-P local pools
func (p *PerPIdleConns) init() {
	numP := runtime.GOMAXPROCS(0)
	local := make([]perpLocal, numP)
	for i := range local {
		local[i].conns = make([]*Conn, 0, 4) // Small local buffer per P
	}
	p.local.Store(&local)
	p.localSize.Store(int32(numP))
}

// Linked from runtime - these are the same functions sync.Pool uses
//
//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin()

// Push adds a connection to the pool (LIFO)
func (p *PerPIdleConns) Push(conn *Conn) {
	// Pin to current P to prevent preemption
	pid := runtime_procPin()

	local := p.local.Load()
	if local == nil || pid >= len(*local) {
		runtime_procUnpin()
		// Fall back to shared pool
		p.pushShared(conn)
		return
	}

	l := &(*local)[pid]

	// Fast path: try to store in private slot (no locking needed)
	if l.private.CompareAndSwap(nil, conn) {
		runtime_procUnpin()
		return
	}

	// Must unpin before acquiring lock to avoid "schedule: holding locks" panic
	runtime_procUnpin()

	// Slow path: add to local shared buffer
	l.mu.Lock()
	if len(l.conns) < cap(l.conns) {
		l.conns = append(l.conns, conn)
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	// Local buffer full, add to shared pool
	p.pushShared(conn)
}

func (p *PerPIdleConns) pushShared(conn *Conn) {
	p.shared.mu.Lock()
	if int32(len(p.shared.conns)) < p.maxIdleConns {
		p.shared.conns = append(p.shared.conns, conn)
	}
	p.shared.mu.Unlock()
}

// Pop removes and returns a connection from the pool (LIFO)
func (p *PerPIdleConns) Pop() *Conn {
	// Pin to current P to prevent preemption
	pid := runtime_procPin()

	local := p.local.Load()
	if local == nil || pid >= len(*local) {
		runtime_procUnpin()
		return p.popShared()
	}

	l := &(*local)[pid]

	// Fast path: try to get from private slot (no locking needed)
	if conn := l.private.Swap(nil); conn != nil {
		runtime_procUnpin()
		return conn
	}

	// Must unpin before acquiring lock to avoid "schedule: holding locks" panic
	runtime_procUnpin()

	// Try local shared buffer
	l.mu.Lock()
	if len(l.conns) > 0 {
		idx := len(l.conns) - 1
		conn := l.conns[idx]
		l.conns = l.conns[:idx]
		l.mu.Unlock()
		return conn
	}
	l.mu.Unlock()

	// Try to steal from other P's local pools
	numP := int(p.localSize.Load())
	for i := 1; i < numP; i++ {
		otherPid := (pid + i) % numP
		other := &(*local)[otherPid]

		// Try other's private slot (lock-free)
		if conn := other.private.Swap(nil); conn != nil {
			return conn
		}

		// Try other's shared buffer
		other.mu.Lock()
		if len(other.conns) > 0 {
			idx := len(other.conns) - 1
			conn := other.conns[idx]
			other.conns = other.conns[:idx]
			other.mu.Unlock()
			return conn
		}
		other.mu.Unlock()
	}

	// Fall back to shared pool
	return p.popShared()
}

func (p *PerPIdleConns) popShared() *Conn {
	p.shared.mu.Lock()
	if len(p.shared.conns) == 0 {
		p.shared.mu.Unlock()
		return nil
	}
	idx := len(p.shared.conns) - 1
	conn := p.shared.conns[idx]
	p.shared.conns = p.shared.conns[:idx]
	p.shared.mu.Unlock()
	return conn
}

// Len returns the approximate number of idle connections
func (p *PerPIdleConns) Len() int {
	count := 0

	local := p.local.Load()
	if local != nil {
		for i := range *local {
			l := &(*local)[i]
			if l.private.Load() != nil {
				count++
			}
			l.mu.Lock()
			count += len(l.conns)
			l.mu.Unlock()
		}
	}

	p.shared.mu.Lock()
	count += len(p.shared.conns)
	p.shared.mu.Unlock()

	return count
}

// Clear removes all connections from the pool
func (p *PerPIdleConns) Clear() []*Conn {
	var conns []*Conn

	local := p.local.Load()
	if local != nil {
		for i := range *local {
			l := &(*local)[i]
			if conn := l.private.Swap(nil); conn != nil {
				conns = append(conns, conn)
			}
			l.mu.Lock()
			conns = append(conns, l.conns...)
			l.conns = l.conns[:0]
			l.mu.Unlock()
		}
	}

	p.shared.mu.Lock()
	conns = append(conns, p.shared.conns...)
	p.shared.conns = p.shared.conns[:0]
	p.shared.mu.Unlock()

	return conns
}

