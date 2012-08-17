package redis

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/vmihailenco/bufio"
)

type Conn struct {
	RW io.ReadWriteCloser
	Rd reader
}

func NewConn(rw io.ReadWriteCloser) *Conn {
	return &Conn{
		RW: rw,
		Rd: bufio.NewReaderSize(rw, 1024),
	}
}

type ConnPool interface {
	Get() (*Conn, bool, error)
	Add(*Conn) error
	Remove(*Conn) error
	Len() int
	Close() error
}

//------------------------------------------------------------------------------

type MultiConnPool struct {
	Logger      *log.Logger
	cond        *sync.Cond
	conns       []*Conn
	OpenConn    OpenConnFunc
	CloseConn   CloseConnFunc
	cap, MaxCap int
}

func NewMultiConnPool(openConn OpenConnFunc, closeConn CloseConnFunc, maxCap int) *MultiConnPool {
	logger := log.New(
		os.Stdout,
		"redis.connpool: ",
		log.Ldate|log.Ltime|log.Lshortfile,
	)
	return &MultiConnPool{
		cond:      sync.NewCond(&sync.Mutex{}),
		Logger:    logger,
		conns:     make([]*Conn, 0),
		OpenConn:  openConn,
		CloseConn: closeConn,
		MaxCap:    maxCap,
	}
}

func (p *MultiConnPool) Get() (*Conn, bool, error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	for len(p.conns) == 0 && p.cap >= p.MaxCap {
		p.cond.Wait()
	}

	if len(p.conns) == 0 {
		rw, err := p.OpenConn()
		if err != nil {
			return nil, false, err
		}

		p.cap++
		return NewConn(rw), true, nil
	}

	last := len(p.conns) - 1
	conn := p.conns[last]
	p.conns[last] = nil
	p.conns = p.conns[:last]

	return conn, false, nil
}

func (p *MultiConnPool) Add(conn *Conn) error {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.conns = append(p.conns, conn)
	p.cond.Signal()
	return nil
}

func (p *MultiConnPool) Remove(conn *Conn) error {
	defer func() {
		p.cond.L.Lock()
		p.cap--
		p.cond.Signal()
		p.cond.L.Unlock()
	}()
	if conn == nil {
		return nil
	}
	return p.closeConn(conn)
}

func (p *MultiConnPool) Len() int {
	return len(p.conns)
}

func (p *MultiConnPool) Close() error {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	for _, conn := range p.conns {
		err := p.closeConn(conn)
		if err != nil {
			return err
		}
	}
	p.conns = make([]*Conn, 0)
	p.cap = 0
	return nil
}

func (p *MultiConnPool) closeConn(conn *Conn) error {
	if p.CloseConn != nil {
		err := p.CloseConn(conn.RW)
		if err != nil {
			return err
		}
	}
	return conn.RW.Close()
}

//------------------------------------------------------------------------------

type SingleConnPool struct {
	mtx        sync.Mutex
	pool       ConnPool
	conn       *Conn
	isReusable bool
}

func NewSingleConnPoolConn(pool ConnPool, conn *Conn, isReusable bool) *SingleConnPool {
	return &SingleConnPool{
		pool:       pool,
		conn:       conn,
		isReusable: isReusable,
	}
}

func NewSingleConnPool(pool ConnPool, isReusable bool) *SingleConnPool {
	return NewSingleConnPoolConn(pool, nil, isReusable)
}

func (p *SingleConnPool) Get() (*Conn, bool, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.conn != nil {
		return p.conn, false, nil
	}
	conn, isNew, err := p.pool.Get()
	if err != nil {
		return nil, false, err
	}
	p.conn = conn
	return p.conn, isNew, nil
}

func (p *SingleConnPool) Add(conn *Conn) error {
	return nil
}

func (p *SingleConnPool) Remove(conn *Conn) error {
	return nil
}

func (p *SingleConnPool) Len() int {
	return 1
}

func (p *SingleConnPool) Close() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.conn == nil {
		return nil
	}

	var err error
	if p.isReusable {
		err = p.pool.Add(p.conn)
	} else {
		err = p.pool.Remove(p.conn)
	}
	p.conn = nil
	return err
}
