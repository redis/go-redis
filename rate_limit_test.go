package redis

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	const n = 100000
	rl := newRateLimiter(time.Second, n)

	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			if !rl.Check() {
				panic("check failed")
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if rl.Check() && rl.Check() {
		t.Fatal("check passed")
	}
}
