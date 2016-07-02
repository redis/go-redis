package ratelimit

import (
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateLimiter", func() {

	It("should accurately rate-limit at small rates", func() {
		var count int
		rl := New(10, time.Minute)
		for !rl.Limit() {
			count++
		}
		Expect(count).To(Equal(10))
	})

	It("should accurately rate-limit at large rates", func() {
		var count int
		rl := New(100000, time.Hour)
		for !rl.Limit() {
			count++
		}
		Expect(count).To(BeNumerically("~", 100000, 10))
	})

	It("should accurately rate-limit at large intervals", func() {
		var count int
		rl := New(100, 360*24*time.Hour)
		for !rl.Limit() {
			count++
		}
		Expect(count).To(Equal(100))
	})

	It("should correctly increase allowance", func() {
		n := 25
		rl := New(n, 50*time.Millisecond)
		for i := 0; i < n; i++ {
			Expect(rl.Limit()).To(BeFalse(), "on cycle %d", i)
		}
		Expect(rl.Limit()).To(BeTrue())
		Eventually(rl.Limit, "60ms", "10ms").Should(BeFalse())
	})

	It("should correctly spread allowance", func() {
		var count int
		rl := New(5, 10*time.Millisecond)
		start := time.Now()
		for time.Now().Sub(start) < 100*time.Millisecond {
			if !rl.Limit() {
				count++
			}
		}
		Expect(count).To(BeNumerically("~", 54, 1))
	})

	It("should undo", func() {
		rl := New(5, time.Minute)

		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeTrue())

		rl.Undo()
		Expect(rl.Limit()).To(BeFalse())
		Expect(rl.Limit()).To(BeTrue())
	})

	It("should be thread-safe", func() {
		c := 100
		n := 100
		wg := sync.WaitGroup{}
		rl := New(c*n, time.Hour)
		for i := 0; i < c; i++ {
			wg.Add(1)

			go func(thread int) {
				defer GinkgoRecover()
				defer wg.Done()

				for j := 0; j < n; j++ {
					Expect(rl.Limit()).To(BeFalse(), "thread %d, cycle %d", thread, j)
				}
			}(i)
		}
		wg.Wait()
		Expect(rl.Limit()).To(BeTrue())
	})

	It("should allow to upate rate", func() {
		var count int
		rl := New(5, 50*time.Millisecond)
		for !rl.Limit() {
			count++
		}
		Expect(count).To(Equal(5))

		rl.UpdateRate(10)
		time.Sleep(50 * time.Millisecond)

		for !rl.Limit() {
			count++
		}
		Expect(count).To(Equal(15))
	})

})

// --------------------------------------------------------------------

func BenchmarkLimit(b *testing.B) {
	rl := New(1000, time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Limit()
	}
}

// --------------------------------------------------------------------

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ratelimit")
}
