package pool

import (
	"sync"
	"time"
)

// connStack is used as a LIFO to maintain free connections
type connStack struct {
	cns  []*Conn
	free chan struct{}
	mu   sync.Mutex
}

func newConnStack(max int) *connStack {
	return &connStack{
		cns:  make([]*Conn, 0, max),
		free: make(chan struct{}, max),
	}
}

func (s *connStack) Len() int { return len(s.free) }

func (s *connStack) Push(cn *Conn) {
	s.mu.Lock()
	s.cns = append(s.cns, cn)
	s.mu.Unlock()
	s.free <- struct{}{}
}

func (s *connStack) ShiftStale(idleTimeout time.Duration) *Conn {
	select {
	case <-s.free:
		s.mu.Lock()
		defer s.mu.Unlock()

		if cn := s.cns[0]; cn.IsStale(idleTimeout) {
			copy(s.cns, s.cns[1:])
			s.cns = s.cns[:len(s.cns)-1]
			return cn
		}

		s.free <- struct{}{}
		return nil
	default:
		return nil
	}
}

func (s *connStack) Pop() *Conn {
	select {
	case <-s.free:
		return s.pop()
	default:
		return nil
	}
}

func (s *connStack) PopWithTimeout(d time.Duration) *Conn {
	select {
	case <-s.free:
		return s.pop()
	case <-time.After(d):
		return nil
	}
}

func (s *connStack) pop() (cn *Conn) {
	s.mu.Lock()
	ci := len(s.cns) - 1
	cn, s.cns = s.cns[ci], s.cns[:ci]
	s.mu.Unlock()
	return
}
