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
// The caller must add connection or cancel reservation if it was reserved.
func (l *connList) Reserve() bool {
	len := atomic.AddInt32(&l.len, 1)
	reserved := len <= l.size
	if !reserved {
		atomic.AddInt32(&l.len, -1)
	}
	return reserved
}

func (l *connList) CancelReservation() {
	atomic.AddInt32(&l.len, -1)
}

// Add adds connection to the list. The caller must reserve place first.
func (l *connList) Add(cn *Conn) {
	l.mu.Lock()
	for i, c := range l.cns {
		if c == nil {
			cn.SetIndex(i)
			l.cns[i] = cn
			l.mu.Unlock()
			return
		}
	}
	panic("not reached")
}

func (l *connList) Replace(cn *Conn) {
	l.mu.Lock()
	if l.cns != nil {
		l.cns[cn.idx] = cn
	}
	l.mu.Unlock()
}

// Remove closes connection and removes it from the list.
func (l *connList) Remove(idx int) {
	l.mu.Lock()
	if l.cns != nil {
		l.cns[idx] = nil
		l.len -= 1
	}
	l.mu.Unlock()
}

func (l *connList) Close() error {
	l.mu.Lock()
	for _, c := range l.cns {
		if c == nil {
			continue
		}
		c.Close()
	}
	l.cns = nil
	l.len = 0
	l.mu.Unlock()
	return nil
}
