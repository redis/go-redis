package redis

import (
	"time"
)

type rateLimiter struct {
	C chan struct{}
}

func newRateLimiter(limit time.Duration, chanSize int) *rateLimiter {
	rl := &rateLimiter{
		C: make(chan struct{}, chanSize),
	}
	for i := 0; i < chanSize; i++ {
		rl.C <- struct{}{}
	}
	go rl.loop(limit)
	return rl
}

func (rl *rateLimiter) loop(limit time.Duration) {
	for {
		select {
		case rl.C <- struct{}{}:
		default:
		}
		time.Sleep(limit)
	}
}
