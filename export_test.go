package redis

import "net"

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
