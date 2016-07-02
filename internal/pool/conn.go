package pool

import (
	"net"
	"time"

	"gopkg.in/redis.v4/internal/proto"
)

const defaultBufSize = 4096

var noDeadline = time.Time{}

type Conn struct {
	NetConn net.Conn
	Rd      *proto.Reader
	Wb      *proto.WriteBuffer

	Inited bool
	UsedAt time.Time

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		NetConn: netConn,
		Wb:      proto.NewWriteBuffer(),

		UsedAt: time.Now(),
	}
	cn.Rd = proto.NewReader(cn)
	return cn
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

func (cn *Conn) Close() error {
	return cn.NetConn.Close()
}
