package redis

import "net"

func (c *baseClient) Pool() pool {
	return c.connPool
}

var NewConnDialer = newConnDialer

func (cn *conn) SetNetConn(netcn net.Conn) {
	cn.netcn = netcn
}

func HashSlot(key string) int {
	return hashSlot(key)
}
