package redis_test

import (
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	redis "gopkg.in/redis.v5"
)

func Example_instrumentation() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": ":6379",
		},
	})
	ring.ForEachShard(func(client *redis.Client) error {
		wrapRedisProcess(client)
		return nil
	})

	for {
		ring.Ping()
	}
}

func wrapRedisProcess(client *redis.Client) {
	const precision = time.Microsecond
	var count, avgDur uint32

	go func() {
		for _ = range time.Tick(3 * time.Second) {
			n := atomic.LoadUint32(&count)
			dur := time.Duration(atomic.LoadUint32(&avgDur)) * precision
			fmt.Printf("%s: processed=%d avg_dur=%s\n", client, n, dur)
		}
	}()

	client.WrapProcess(func(oldProcess func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			start := time.Now()
			err := oldProcess(cmd)
			dur := time.Since(start)

			const decay = float64(1) / 100
			ms := float64(dur / precision)
			for {
				avg := atomic.LoadUint32(&avgDur)
				newAvg := uint32((1-decay)*float64(avg) + decay*ms)
				if atomic.CompareAndSwapUint32(&avgDur, avg, newAvg) {
					break
				}
			}
			atomic.AddUint32(&count, 1)

			return err
		}
	})
}

var _ = Describe("Instrumentation", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("WrapProcess", func() {

		It("should call for client", func() {
			wrapperFnCalled := false

			client.WrapProcess(func(oldProcess func(redis.Cmder) error) func(redis.Cmder) error {
				return func(cmd redis.Cmder) error {
					wrapperFnCalled = true
					return oldProcess(cmd)
				}
			})

			client.Ping()

			Expect(wrapperFnCalled).To(Equal(true))
		})

	})
})
