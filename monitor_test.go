package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Monitor command", Label("monitor"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should monitor", Label("monitor"), func() {
		ress := make(chan string)
		client1 := redis.NewClient(&redis.Options{Addr: rediStackAddr})
		mn := client1.Monitor(ctx, ress)
		mn.Start()
		// Wait for the Redis server to be in monitoring mode.
		time.Sleep(100 * time.Millisecond)
		client.Set(ctx, "foo", "bar", 0)
		client.Set(ctx, "bar", "baz", 0)
		client.Set(ctx, "bap", 8, 0)
		client.Get(ctx, "bap")
		lst := []string{}
		for i := 0; i < 5; i++ {
			s := <-ress
			lst = append(lst, s)
		}
		mn.Stop()
		Expect(lst[0]).To(ContainSubstring("OK"))
		Expect(lst[1]).To(ContainSubstring(`"set" "foo" "bar"`))
		Expect(lst[2]).To(ContainSubstring(`"set" "bar" "baz"`))
		Expect(lst[3]).To(ContainSubstring(`"set" "bap" "8"`))
	})
})
