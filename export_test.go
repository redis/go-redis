package redis

import (
	"net"
	"time"
)

func (c *baseClient) Pool() pool {
	return c.connPool
}

func (c *PubSub) Pool() pool {
	return c.base.connPool
}

var NewConnDialer = newConnDialer

func (cn *conn) SetNetConn(netcn net.Conn) {
	cn.netcn = netcn
}

func SetTime(tm time.Time) {
	now = func() time.Time {
		return tm
	}
}

func RestoreTime() {
	now = time.Now
}
