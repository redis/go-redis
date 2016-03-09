package redis

import (
	"net"
	"sync"
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

var timeMu sync.Mutex

func SetTime(tm time.Time) {
	timeMu.Lock()
	now = func() time.Time {
		return tm
	}
	timeMu.Unlock()
}

func RestoreTime() {
	timeMu.Lock()
	now = time.Now
	timeMu.Unlock()
}
