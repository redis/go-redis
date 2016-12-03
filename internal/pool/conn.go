package pool

import (
	"net"
	"time"

	"gopkg.in/redis.v5/internal/proto"
)

const defaultBufSize = 4096

var noDeadline = time.Time{}

type Conn struct {
	NetConn net.Conn
	Rd      *proto.Reader
	Wb      *proto.WriteBuffer

	Inited bool
	UsedAt time.Time
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		NetConn: netConn,
		Wb:      proto.NewWriteBuffer(),

		UsedAt: time.Now(),
	}
	cn.Rd = proto.NewReader(cn.NetConn)
	return cn
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt) > timeout
}

func (cn *Conn) SetReadTimeout(timeout time.Duration) error {
	cn.UsedAt = time.Now()
	if timeout > 0 {
		return cn.NetConn.SetReadDeadline(cn.UsedAt.Add(timeout))
	}
	return cn.NetConn.SetReadDeadline(noDeadline)

}

func (cn *Conn) SetWriteTimeout(timeout time.Duration) error {
	cn.UsedAt = time.Now()
	if timeout > 0 {
		return cn.NetConn.SetWriteDeadline(cn.UsedAt.Add(timeout))
	}
	return cn.NetConn.SetWriteDeadline(noDeadline)
}

func (cn *Conn) Close() error {
	return cn.NetConn.Close()
}
