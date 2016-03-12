package pool

import (
	"bufio"
	"net"
	"time"
)

const defaultBufSize = 4096

var noTimeout = time.Time{}

type Conn struct {
	NetConn net.Conn
	Rd      *bufio.Reader
	Buf     []byte

	UsedAt       time.Time
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		NetConn: netConn,
		Buf:     make([]byte, defaultBufSize),

		UsedAt: time.Now(),
	}
	cn.Rd = bufio.NewReader(cn)
	return cn
}

func (cn *Conn) Read(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.ReadTimeout != 0 {
		cn.NetConn.SetReadDeadline(cn.UsedAt.Add(cn.ReadTimeout))
	} else {
		cn.NetConn.SetReadDeadline(noTimeout)
	}
	return cn.NetConn.Read(b)
}

func (cn *Conn) Write(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.WriteTimeout != 0 {
		cn.NetConn.SetWriteDeadline(cn.UsedAt.Add(cn.WriteTimeout))
	} else {
		cn.NetConn.SetWriteDeadline(noTimeout)
	}
	return cn.NetConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.NetConn.RemoteAddr()
}

func (cn *Conn) Close() error {
	return cn.NetConn.Close()
}
