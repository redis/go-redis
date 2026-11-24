package pubsub

import "time"

// connConfiger is the client-wide part of PubSubConfiger: settings of
// the shared connection, which every subscriber uses.
type connConfiger interface {
	UpdatePingTimeout(newTimeout time.Duration)
	UpdateHealthCheckInterval(newInterval time.Duration)
	UpdateReconnectTimeout(newTimeout time.Duration)
}

// channelConfig collects the ChannelOptions of one Channel or
// ChannelWithSubscriptions call. The buffer size and send timeout
// belong to the calling subscriber's consumer channel; the connection
// settings are forwarded to conn.
type channelConfig struct {
	conn        connConfiger
	size        int
	sendTimeout time.Duration
}

var _ PubSubConfiger = (*channelConfig)(nil)

// newChannelConfig applies opts on top of the manager's size and send
// timeout.
func newChannelConfig(conn connConfiger, size int, sendTimeout time.Duration, opts []ChannelOption) *channelConfig {
	c := &channelConfig{conn: conn, size: size, sendTimeout: sendTimeout}
	for _, o := range opts {
		o(c)
	}
	return c
}

// UpdateChannelSize sets this subscriber's channel buffer size. A
// non-positive size is ignored: zero would make the channel unbuffered,
// a negative one would panic in make.
func (c *channelConfig) UpdateChannelSize(newSize int) {
	if newSize > 0 {
		c.size = newSize
	}
}

// UpdateSendTimeout sets how long this subscriber's pump waits on a full
// channel before it drops the message; <= 0 waits forever.
func (c *channelConfig) UpdateSendTimeout(newTimeout time.Duration) {
	c.sendTimeout = newTimeout
}

func (c *channelConfig) UpdatePingTimeout(newTimeout time.Duration) {
	c.conn.UpdatePingTimeout(newTimeout)
}

func (c *channelConfig) UpdateHealthCheckInterval(newInterval time.Duration) {
	c.conn.UpdateHealthCheckInterval(newInterval)
}

func (c *channelConfig) UpdateReconnectTimeout(newTimeout time.Duration) {
	c.conn.UpdateReconnectTimeout(newTimeout)
}
