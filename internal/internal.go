package internal

import (
	"time"

	"github.com/redis/go-redis/v9/internal/rand"
)

func RetryBackoff(retry int, minBackoff, maxBackoff time.Duration) time.Duration {
	if retry < 0 {
		panic("not reached")
	}
	if minBackoff == 0 {
		return 0
	}

	d := minBackoff << uint(retry)
	if d < minBackoff {
		d = minBackoff
	}

	jitter := time.Duration(rand.Int63n(int64(d)))

	d = d + jitter
	if d > maxBackoff {
		return maxBackoff
	}

	return d
}
