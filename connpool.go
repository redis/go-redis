package redis

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/vmihailenco/bufreader"
)

type Conn struct {
	RW io.ReadWriter
	Rd *bufreader.Reader
}

func NewConn(rw io.ReadWriter) *Conn {
	return &Conn{
		RW: rw,
		Rd: bufreader.NewSizedReader(8024),
	}
}

type ConnPool struct {
	Logger      *log.Logger
	cond        *sync.Cond
	conns       []*Conn
	OpenConn    OpenConnFunc
	CloseConn   CloseConnFunc
	cap, MaxCap int64
}

func NewConnPool(openConn OpenConnFunc, closeConn CloseConnFunc, maxCap int64) *ConnPool {
	logger := log.New(
		os.Stdout,
		"redis.connpool: ",
		log.Ldate|log.Ltime|log.Lshortfile,
	)
	return &ConnPool{
		cond:      sync.NewCond(&sync.Mutex{}),
		Logger:    logger,
		conns:     make([]*Conn, 0),
		OpenConn:  openConn,
		CloseConn: closeConn,
		MaxCap:    maxCap,
	}
}

func (p *ConnPool) Get() (*Conn, bool, error) {
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

func (p *ConnPool) Add(conn *Conn) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.conns = append(p.conns, conn)
	p.cond.Signal()
}

func (p *ConnPool) Remove(conn *Conn) {
	p.cond.L.Lock()
	p.cap--
	p.cond.Signal()
	p.cond.L.Unlock()

	if p.CloseConn != nil && conn != nil {
		p.CloseConn(conn.RW)
	}
}

func (p *ConnPool) Len() int {
	return len(p.conns)
}
