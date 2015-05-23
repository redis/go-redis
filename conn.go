package redis

import (
	"net"
	"time"

	"gopkg.in/bufio.v1"
)

var (
	zeroTime = time.Time{}
)

type conn struct {
	netcn net.Conn
	rd    *bufio.Reader
	buf   []byte

	usedAt       time.Time
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func newConnDialer(opt *Options) func() (*conn, error) {
	dialer := opt.getDialer()
	return func() (*conn, error) {
		netcn, err := dialer()
		if err != nil {
			return nil, err
		}
		cn := &conn{
			netcn: netcn,
			buf:   make([]byte, 0, 64),
		}
		cn.rd = bufio.NewReader(cn)
		return cn, cn.init(opt)
	}
}

func (cn *conn) init(opt *Options) error {
	if opt.Password == "" && opt.DB == 0 {
		return nil
	}

	// Use connection to connect to redis
	pool := newSingleConnPool(nil, false)
	pool.SetConn(cn)

	// Client is not closed because we want to reuse underlying connection.
	client := newClient(opt, pool)

	if opt.Password != "" {
		if err := client.Auth(opt.Password).Err(); err != nil {
			return err
		}
	}

	if opt.DB > 0 {
		if err := client.Select(opt.DB).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (cn *conn) writeCmds(cmds ...Cmder) error {
	buf := cn.buf[:0]
	for _, cmd := range cmds {
		buf = appendArgs(buf, cmd.args())
	}

	_, err := cn.Write(buf)
	return err
}

func (cn *conn) Read(b []byte) (int, error) {
	if cn.ReadTimeout != 0 {
		cn.netcn.SetReadDeadline(time.Now().Add(cn.ReadTimeout))
	} else {
		cn.netcn.SetReadDeadline(zeroTime)
	}
	return cn.netcn.Read(b)
}

func (cn *conn) Write(b []byte) (int, error) {
	if cn.WriteTimeout != 0 {
		cn.netcn.SetWriteDeadline(time.Now().Add(cn.WriteTimeout))
	} else {
		cn.netcn.SetWriteDeadline(zeroTime)
	}
	return cn.netcn.Write(b)
}

func (cn *conn) RemoteAddr() net.Addr {
	return cn.netcn.RemoteAddr()
}

func (cn *conn) Close() error {
	return cn.netcn.Close()
}
