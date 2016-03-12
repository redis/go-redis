package pool

import (
	"sync"
	"sync/atomic"
)

type connList struct {
	cns  []*Conn
	mu   sync.Mutex
	len  int32 // atomic
	size int32
}

func newConnList(size int) *connList {
	return &connList{
		cns:  make([]*Conn, size),
		size: int32(size),
	}
}

func (l *connList) Len() int {
	return int(atomic.LoadInt32(&l.len))
}

// Reserve reserves place in the list and returns true on success.
// The caller must add or remove connection if place was reserved.
func (l *connList) Reserve() bool {
	len := atomic.AddInt32(&l.len, 1)
	reserved := len <= l.size
	if !reserved {
		atomic.AddInt32(&l.len, -1)
	}
	return reserved
}

// Add adds connection to the list. The caller must reserve place first.
func (l *connList) Add(cn *Conn) {
	l.mu.Lock()
	for i, c := range l.cns {
		if c == nil {
			cn.idx = i
			l.cns[i] = cn
			l.mu.Unlock()
			return
		}
	}
	panic("not reached")
}

// Remove closes connection and removes it from the list.
func (l *connList) Remove(cn *Conn) error {
	atomic.AddInt32(&l.len, -1)

	if cn == nil { // free reserved place
		return nil
	}

	l.mu.Lock()
	if l.cns != nil {
		l.cns[cn.idx] = nil
		cn.idx = -1
	}
	l.mu.Unlock()

	return nil
}

func (l *connList) Close() error {
	var retErr error
	l.mu.Lock()
	for _, c := range l.cns {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	l.cns = nil
	atomic.StoreInt32(&l.len, 0)
	l.mu.Unlock()
	return retErr
}
