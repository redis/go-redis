package redis

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateLimiter", func() {
	var n = 100000
	if testing.Short() {
		n = 1000
	}

	It("should rate limit", func() {
		rl := newRateLimiter(time.Minute, n)
		for i := 0; i <= n; i++ {
			Expect(rl.Check()).To(BeTrue())
		}
		Expect(rl.Check()).To(BeFalse())
	})

})
