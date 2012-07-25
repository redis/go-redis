package redis

import (
	"strconv"
)

//------------------------------------------------------------------------------

func (c *Client) Ping() *StatusReq {
	req := NewStatusReq("PING")
	c.Run(req)
	return req
}

func (c *Client) Flushall() *StatusReq {
	req := NewStatusReq("FLUSHALL")
	c.Run(req)
	return req
}

func (c *Client) Flushdb() *StatusReq {
	req := NewStatusReq("FLUSHDB")
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Get(key string) *BulkReq {
	req := NewBulkReq("GET", key)
	c.Run(req)
	return req
}

func (c *Client) Set(key, value string) *StatusReq {
	req := NewStatusReq("SET", key, value)
	c.Run(req)
	return req
}

func (c *Client) Auth(password string) *StatusReq {
	req := NewStatusReq("AUTH", password)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Sadd(key string, members ...string) *IntReq {
	args := append([]string{"SADD", key}, members...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Srem(key string, members ...string) *IntReq {
	args := append([]string{"SREM", key}, members...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Smembers(key string) *MultiBulkReq {
	req := NewMultiBulkReq("SMEMBERS", key)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Multi() *MultiClient {
	return NewMultiClient(c.connect, c.disconnect)
}

//------------------------------------------------------------------------------

func (c *Client) PubSubClient() *PubSubClient {
	return NewPubSubClient(c.connect, c.disconnect)
}

func (c *Client) Publish(channel, message string) *IntReq {
	req := NewIntReq("PUBLISH", channel, message)
	c.Run(req)
	return req
}

//------------------------------------------------------------------------------

func (c *Client) Hset(key, field, value string) *BoolReq {
	req := NewBoolReq("HSET", key, field, value)
	c.Run(req)
	return req
}

func (c *Client) Hsetnx(key, field, value string) *BoolReq {
	req := NewBoolReq("HSETNX", key, field, value)
	c.Run(req)
	return req
}

func (c *Client) Hmset(key, field, value string, pairs ...string) *StatusReq {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	req := NewStatusReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Hget(key, field string) *BulkReq {
	req := NewBulkReq("HGET", key, field)
	c.Run(req)
	return req
}

func (c *Client) Hmget(key string, fields ...string) *MultiBulkReq {
	args := append([]string{"HMGET", key}, fields...)
	req := NewMultiBulkReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Hexists(key, field string) *BoolReq {
	req := NewBoolReq("HEXISTS", key, field)
	c.Run(req)
	return req
}

func (c *Client) Hdel(key string, fields ...string) *IntReq {
	args := append([]string{"HDEL", key}, fields...)
	req := NewIntReq(args...)
	c.Run(req)
	return req
}

func (c *Client) Hlen(key string) *IntReq {
	req := NewIntReq("HLEN", key)
	c.Run(req)
	return req
}

func (c *Client) Hgetall(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HGETALL", key)
	c.Run(req)
	return req
}

func (c *Client) Hkeys(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HKEYS", key)
	c.Run(req)
	return req
}

func (c *Client) Hvals(key string) *MultiBulkReq {
	req := NewMultiBulkReq("HVALS", key)
	c.Run(req)
	return req
}

func (c *Client) Hincrby(key, field string, incr int64) *IntReq {
	req := NewIntReq("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Run(req)
	return req
}
