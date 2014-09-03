package redis

import (
	"time"
)

type rateLimiter struct {
	C chan struct{}

	closer, done chan struct{}
}

func newRateLimiter(limit time.Duration, chanSize int) *rateLimiter {
	rl := &rateLimiter{
		C:      make(chan struct{}, chanSize),
		closer: make(chan struct{}, 1),
		done:   make(chan struct{}, 1),
	}
	for i := 0; i < chanSize; i++ {
		rl.C <- struct{}{}
	}
	go rl.loop(limit)
	return rl
}

func (rl *rateLimiter) Close() error {
	close(rl.closer)
	<-rl.done
	return nil
}

func (rl *rateLimiter) loop(limit time.Duration) {
	for {
		select {
		case <-rl.closer:
			close(rl.done)
			return
		case rl.C <- struct{}{}:
		default:
		}
		time.Sleep(limit)
	}
}
