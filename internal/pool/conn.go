package pool

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/internal/proto"
)

func makeBuffer() []byte {
	const defaulBufSize = 4096
	return make([]byte, defaulBufSize)
}

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	buf      []byte
	rd       proto.Reader
	rdLocked bool
	wb       *proto.WriteBuffer

	InitedAt time.Time
	pooled   bool
	usedAt   atomic.Value
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn: netConn,
		buf:     makeBuffer(),
	}
	cn.rd = proto.NewReader(proto.NewElasticBufReader(netConn))
	cn.wb = proto.NewWriteBuffer()
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	return cn.usedAt.Load().(time.Time)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	cn.usedAt.Store(tm)
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
}

func (cn *Conn) setReadTimeout(timeout time.Duration) error {
	now := time.Now()
	cn.SetUsedAt(now)
	if timeout > 0 {
		return cn.netConn.SetReadDeadline(now.Add(timeout))
	}
	return cn.netConn.SetReadDeadline(noDeadline)
}

func (cn *Conn) setWriteTimeout(timeout time.Duration) error {
	now := time.Now()
	cn.SetUsedAt(now)
	if timeout > 0 {
		return cn.netConn.SetWriteDeadline(now.Add(timeout))
	}
	return cn.netConn.SetWriteDeadline(noDeadline)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) LockReaderBuffer() {
	cn.rdLocked = true
	cn.rd.ResetBuffer(makeBuffer())
}

func (cn *Conn) WithReader(timeout time.Duration, fn func(rd proto.Reader) error) error {
	_ = cn.setReadTimeout(timeout)

	if !cn.rdLocked {
		cn.rd.ResetBuffer(cn.buf)
	}

	err := fn(cn.rd)

	if !cn.rdLocked {
		cn.buf = cn.rd.Buffer()
	}

	return err
}

func (cn *Conn) WithWriter(timeout time.Duration, fn func(wb *proto.WriteBuffer) error) error {
	_ = cn.setWriteTimeout(timeout)

	cn.wb.ResetBuffer(cn.buf)

	firstErr := fn(cn.wb)

	_, err := cn.netConn.Write(cn.wb.Bytes())
	cn.buf = cn.wb.Buffer()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}
