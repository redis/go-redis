package redis

import (
	"time"

	"gopkg.in/redis.v3/internal/pool"
)

func (c *baseClient) Pool() pool.Pooler {
	return c.connPool
}

func (c *PubSub) Pool() pool.Pooler {
	return c.base.connPool
}

func SetReceiveMessageTimeout(d time.Duration) {
	receiveMessageTimeout = d
}
