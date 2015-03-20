package redis

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateLimiter", func() {
	var N = 10000
	if testing.Short() {
		N = 1000
	}

	It("should rate limit", func() {
		rl := newRateLimiter(time.Minute, N)
		for i := 0; i <= N; i++ {
			Expect(rl.Check()).To(BeTrue(), "cycle %d/%d", i, N)
		}
		Expect(rl.Check()).To(BeFalse())
	})

})
