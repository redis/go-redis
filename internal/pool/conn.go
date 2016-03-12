package pool

import (
	"bufio"
	"net"
	"time"
)

const defaultBufSize = 4096

var noDeadline = time.Time{}

type Conn struct {
	idx int

	netConn net.Conn
	Rd      *bufio.Reader
	Buf     []byte

	UsedAt       time.Time
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		idx: -1,

		netConn: netConn,
		Buf:     make([]byte, defaultBufSize),

		UsedAt: time.Now(),
	}
	cn.Rd = bufio.NewReader(cn)
	return cn
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt) > timeout
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.UsedAt = time.Now()
}

func (cn *Conn) Read(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.ReadTimeout != 0 {
		cn.netConn.SetReadDeadline(cn.UsedAt.Add(cn.ReadTimeout))
	} else {
		cn.netConn.SetReadDeadline(noDeadline)
	}
	return cn.netConn.Read(b)
}

func (cn *Conn) Write(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.WriteTimeout != 0 {
		cn.netConn.SetWriteDeadline(cn.UsedAt.Add(cn.WriteTimeout))
	} else {
		cn.netConn.SetWriteDeadline(noDeadline)
	}
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}
