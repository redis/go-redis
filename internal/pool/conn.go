package pool

import (
	"bufio"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const defaultBufSize = 4096

var noDeadline = time.Time{}

type Conn struct {
	idx int32

	NetConn net.Conn
	Rd      *bufio.Reader
	Buf     []byte

	Inited bool
	UsedAt time.Time

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		idx: -1,

		NetConn: netConn,
		Buf:     make([]byte, defaultBufSize),

		UsedAt: time.Now(),
	}
	cn.Rd = bufio.NewReader(cn)
	return cn
}

func (cn *Conn) Index() int {
	return int(atomic.LoadInt32(&cn.idx))
}

func (cn *Conn) SetIndex(newIdx int) int {
	oldIdx := cn.Index()
	if !atomic.CompareAndSwapInt32(&cn.idx, int32(oldIdx), int32(newIdx)) {
		return -1
	}
	return oldIdx
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt) > timeout
}

func (cn *Conn) Read(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.ReadTimeout != 0 {
		cn.NetConn.SetReadDeadline(cn.UsedAt.Add(cn.ReadTimeout))
	} else {
		cn.NetConn.SetReadDeadline(noDeadline)
	}
	return cn.NetConn.Read(b)
}

func (cn *Conn) Write(b []byte) (int, error) {
	cn.UsedAt = time.Now()
	if cn.WriteTimeout != 0 {
		cn.NetConn.SetWriteDeadline(cn.UsedAt.Add(cn.WriteTimeout))
	} else {
		cn.NetConn.SetWriteDeadline(noDeadline)
	}
	return cn.NetConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.NetConn.RemoteAddr()
}

func (cn *Conn) ReadN(n int) ([]byte, error) {
	if d := n - cap(cn.Buf); d > 0 {
		cn.Buf = cn.Buf[:cap(cn.Buf)]
		cn.Buf = append(cn.Buf, make([]byte, d)...)
	} else {
		cn.Buf = cn.Buf[:n]
	}
	_, err := io.ReadFull(cn.Rd, cn.Buf)
	return cn.Buf, err
}

func (cn *Conn) Close() error {
	return cn.NetConn.Close()
}
