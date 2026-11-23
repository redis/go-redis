package redis_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/maintnotifications"
)

var _ = Describe("PubSub", func() {
	var client *redis.Client

	BeforeEach(func() {
		opt := redisOptions()
		opt.MinIdleConns = 0
		opt.ConnMaxLifetime = 0
		client = redis.NewClient(opt)
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("implements Stringer", func() {
		pubsub := client.PSubscribe(ctx, "mychannel*")
		defer pubsub.Close()

		Expect(pubsub.String()).To(Equal("PubSub(mychannel*)"))
	})

	It("should support pattern matching", func() {
		pubsub := client.PSubscribe(ctx, "mychannel*")
		defer pubsub.Close()

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("psubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel*"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err.(net.Error).Timeout()).To(Equal(true))
			Expect(msgi).To(BeNil())
		}

		n, err := client.Publish(ctx, "mychannel1", "hello").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))

		Expect(pubsub.PUnsubscribe(ctx, "mychannel*")).NotTo(HaveOccurred())

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Message)
			Expect(subscr.Channel).To(Equal("mychannel1"))
			Expect(subscr.Pattern).To(Equal("mychannel*"))
			Expect(subscr.Payload).To(Equal("hello"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("punsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel*"))
			Expect(subscr.Count).To(Equal(0))
		}

		stats := client.PoolStats()
		Expect(stats.Misses).To(Equal(uint32(1)))
	})

	It("should receive hash field subkey notifications", Label("hash-expiration", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion("8.8", "subkeyspace notifications require Redis 8.8")

		config, err := client.ConfigGet(ctx, "notify-keyspace-events").Result()
		Expect(err).NotTo(HaveOccurred())
		previousNotifyConfig := config["notify-keyspace-events"]
		Expect(client.ConfigSet(ctx, "notify-keyspace-events", "STh").Err()).NotTo(HaveOccurred())
		defer client.ConfigSet(ctx, "notify-keyspace-events", previousNotifyConfig)

		const (
			hashKey         = "skn:hash"
			field           = "field-alpha"
			hexpireChannel  = "__subkeyevent@0__:hexpire"
			expiredChannel  = "__subkeyevent@0__:hexpired"
			expectedPayload = "8:skn:hash|11:field-alpha"
		)
		Expect(client.Del(ctx, hashKey).Err()).NotTo(HaveOccurred())

		pubsub := client.Subscribe(ctx, hexpireChannel, expiredChannel)
		defer pubsub.Close()

		for _, expected := range []redis.Subscription{
			{Kind: "subscribe", Channel: hexpireChannel, Count: 1},
			{Kind: "subscribe", Channel: expiredChannel, Count: 2},
		} {
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(msgi).To(Equal(&expected))
		}

		Expect(client.HSet(ctx, hashKey, field, "value").Err()).NotTo(HaveOccurred())
		res, err := client.HPExpire(ctx, hashKey, 50*time.Millisecond, field).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal([]int64{1}))

		seen := make(map[string]string)
		deadline := time.Now().Add(10 * time.Second)

		for time.Now().Before(deadline) &&
			(seen[hexpireChannel] == "" || seen[expiredChannel] == "") {
			msgi, err := pubsub.ReceiveTimeout(ctx, 500*time.Millisecond)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				Expect(err).NotTo(HaveOccurred())
			}

			msg, ok := msgi.(*redis.Message)
			if !ok {
				continue
			}
			if msg.Channel != hexpireChannel && msg.Channel != expiredChannel {
				continue
			}

			Expect(msg.Pattern).To(BeEmpty())
			Expect(msg.Payload).To(Equal(expectedPayload))
			Expect(msg.PayloadSlice).To(BeEmpty())
			seen[msg.Channel] = msg.Payload
		}

		Expect(seen).To(HaveKeyWithValue(hexpireChannel, expectedPayload))
		Expect(seen).To(HaveKeyWithValue(expiredChannel, expectedPayload))
	})

	It("should pub/sub channels", func() {
		channels, err := client.PubSubChannels(ctx, "mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		pubsub := client.Subscribe(ctx, "mychannel", "mychannel2")
		defer pubsub.Close()

		// sleep a bit to make sure redis knows about the subscriptions
		time.Sleep(10 * time.Millisecond)

		channels, err = client.PubSubChannels(ctx, "mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(ConsistOf([]string{"mychannel", "mychannel2"}))

		channels, err = client.PubSubChannels(ctx, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		channels, err = client.PubSubChannels(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(channels)).To(BeNumerically(">=", 2))
	})

	It("should sharded pub/sub channels", func() {
		channels, err := client.PubSubShardChannels(ctx, "mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		pubsub := client.SSubscribe(ctx, "mychannel", "mychannel2")
		defer pubsub.Close()

		// Let Redis process the ssubscribe command.
		time.Sleep(10 * time.Millisecond)

		channels, err = client.PubSubShardChannels(ctx, "mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(ConsistOf([]string{"mychannel", "mychannel2"}))

		channels, err = client.PubSubShardChannels(ctx, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		channels, err = client.PubSubShardChannels(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(channels)).To(BeNumerically(">=", 2))

		nums, err := client.PubSubShardNumSub(ctx, "mychannel", "mychannel2", "mychannel3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(nums).To(Equal(map[string]int64{
			"mychannel":  1,
			"mychannel2": 1,
			"mychannel3": 0,
		}))
	})

	It("should return the numbers of subscribers", func() {
		pubsub := client.Subscribe(ctx, "mychannel", "mychannel2")
		defer pubsub.Close()

		// sleep a bit to make sure redis knows about the subscriptions
		time.Sleep(10 * time.Millisecond)
		channels, err := client.PubSubNumSub(ctx, "mychannel", "mychannel2", "mychannel3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(Equal(map[string]int64{
			"mychannel":  1,
			"mychannel2": 1,
			"mychannel3": 0,
		}))
	})

	It("should return the numbers of subscribers by pattern", func() {
		num, err := client.PubSubNumPat(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(0)))

		pubsub := client.PSubscribe(ctx, "*")
		defer pubsub.Close()

		// sleep a bit to make sure redis knows about the subscriptions
		time.Sleep(10 * time.Millisecond)
		num, err = client.PubSubNumPat(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(1)))
	})

	It("should pub/sub", func() {
		pubsub := client.Subscribe(ctx, "mychannel", "mychannel2")
		defer pubsub.Close()

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("subscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("subscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(2))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err.(net.Error).Timeout()).To(Equal(true))
			Expect(msgi).NotTo(HaveOccurred())
		}

		n, err := client.Publish(ctx, "mychannel", "hello").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))

		n, err = client.Publish(ctx, "mychannel2", "hello2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))

		Expect(pubsub.Unsubscribe(ctx, "mychannel", "mychannel2")).NotTo(HaveOccurred())

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			msg := msgi.(*redis.Message)
			Expect(msg.Channel).To(Equal("mychannel"))
			Expect(msg.Payload).To(Equal("hello"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			msg := msgi.(*redis.Message)
			Expect(msg.Channel).To(Equal("mychannel2"))
			Expect(msg.Payload).To(Equal("hello2"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("unsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("unsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(0))
		}

		stats := client.PoolStats()
		Expect(stats.Misses).To(Equal(uint32(1)))
	})

	It("should sharded pub/sub", func() {
		pubsub := client.SSubscribe(ctx, "mychannel", "mychannel2")
		defer pubsub.Close()

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("ssubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("ssubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(2))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err.(net.Error).Timeout()).To(Equal(true))
			Expect(msgi).NotTo(HaveOccurred())
		}

		n, err := client.SPublish(ctx, "mychannel", "hello").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))

		n, err = client.SPublish(ctx, "mychannel2", "hello2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))

		Expect(pubsub.SUnsubscribe(ctx, "mychannel", "mychannel2")).NotTo(HaveOccurred())

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			msg := msgi.(*redis.Message)
			Expect(msg.Channel).To(Equal("mychannel"))
			Expect(msg.Payload).To(Equal("hello"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			msg := msgi.(*redis.Message)
			Expect(msg.Channel).To(Equal("mychannel2"))
			Expect(msg.Payload).To(Equal("hello2"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("sunsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("sunsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(0))
		}

		stats := client.PoolStats()
		Expect(stats.Misses).To(Equal(uint32(1)))
	})

	It("should ping/pong", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		_, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())

		err = pubsub.Ping(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())
		pong := msgi.(*redis.Pong)
		Expect(pong.Payload).To(Equal(""))
	})

	It("should ping/pong with payload", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		_, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())

		err = pubsub.Ping(ctx, "hello")
		Expect(err).NotTo(HaveOccurred())

		msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())
		pong := msgi.(*redis.Pong)
		Expect(pong.Payload).To(Equal("hello"))
	})

	It("should multi-ReceiveMessage", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		subscr, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(subscr).To(Equal(&redis.Subscription{
			Kind:    "subscribe",
			Channel: "mychannel",
			Count:   1,
		}))

		err = client.Publish(ctx, "mychannel", "hello").Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.Publish(ctx, "mychannel", "world").Err()
		Expect(err).NotTo(HaveOccurred())

		msg, err := pubsub.ReceiveMessage(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal("hello"))

		msg, err = pubsub.ReceiveMessage(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal("world"))
	})

	It("returns an error when subscribe fails", func() {
		pubsub := client.Subscribe(ctx)
		defer pubsub.Close()

		pubsub.SetNetConn(&badConn{
			readErr:  io.EOF,
			writeErr: io.EOF,
		})

		err := pubsub.Subscribe(ctx, "mychannel")
		Expect(err).To(MatchError("EOF"))

		err = pubsub.Subscribe(ctx, "mychannel")
		Expect(err).NotTo(HaveOccurred())
	})

	expectReceiveMessageOnError := func(pubsub *redis.PubSub) {
		pubsub.SetNetConn(&badConn{
			readErr:  io.EOF,
			writeErr: io.EOF,
		})

		step := make(chan struct{}, 3)

		go func() {
			defer GinkgoRecover()

			Eventually(step).Should(Receive())
			err := client.Publish(ctx, "mychannel", "hello").Err()
			Expect(err).NotTo(HaveOccurred())
			step <- struct{}{}
		}()

		_, err := pubsub.ReceiveMessage(ctx)
		Expect(err).To(Equal(io.EOF))
		step <- struct{}{}

		msg, err := pubsub.ReceiveMessage(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal("hello"))

		Eventually(step).Should(Receive())
	}

	It("Subscribe should reconnect on ReceiveMessage error", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		subscr, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(subscr).To(Equal(&redis.Subscription{
			Kind:    "subscribe",
			Channel: "mychannel",
			Count:   1,
		}))

		expectReceiveMessageOnError(pubsub)
	})

	It("PSubscribe should reconnect on ReceiveMessage error", func() {
		pubsub := client.PSubscribe(ctx, "mychannel")
		defer pubsub.Close()

		subscr, err := pubsub.ReceiveTimeout(ctx, time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(subscr).To(Equal(&redis.Subscription{
			Kind:    "psubscribe",
			Channel: "mychannel",
			Count:   1,
		}))

		expectReceiveMessageOnError(pubsub)
	})

	It("should return on Close", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer GinkgoRecover()

			wg.Done()
			defer wg.Done()

			_, err := pubsub.ReceiveMessage(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(SatisfyAny(
				Equal("redis: client is closed"),
				ContainSubstring("use of closed network connection"),
			))
		}()

		wg.Wait()
		wg.Add(1)

		Expect(pubsub.Close()).NotTo(HaveOccurred())

		wg.Wait()
	})

	It("should ReceiveMessage without a subscription", func() {
		timeout := 100 * time.Millisecond

		pubsub := client.Subscribe(ctx)
		defer pubsub.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()

			time.Sleep(timeout)

			err := pubsub.Subscribe(ctx, "mychannel")
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(timeout)

			err = client.Publish(ctx, "mychannel", "hello").Err()
			Expect(err).NotTo(HaveOccurred())
		}()

		msg, err := pubsub.ReceiveMessage(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal("hello"))

		wg.Wait()
	})

	It("handles big message payload", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		ch := pubsub.Channel()

		bigVal := bigVal()
		err := client.Publish(ctx, "mychannel", bigVal).Err()
		Expect(err).NotTo(HaveOccurred())

		var msg *redis.Message
		Eventually(ch).Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal(string(bigVal)))
	})

	It("supports concurrent Ping and Receive", func() {
		const N = 100

		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()

			for i := 0; i < N; i++ {
				_, err := pubsub.ReceiveTimeout(ctx, 5*time.Second)
				Expect(err).NotTo(HaveOccurred())
			}
			close(done)
		}()

		for i := 0; i < N; i++ {
			err := pubsub.Ping(ctx)
			Expect(err).NotTo(HaveOccurred())
		}

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			Fail("timeout")
		}
	})

	It("should ClientSetName on PubSub connection", func() {
		pubsub := client.Subscribe(ctx, "test-channel")
		defer pubsub.Close()

		// Wait for subscription to be established
		_, err := pubsub.Receive(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = pubsub.ClientSetName(ctx, "my-subscriber")
		Expect(err).NotTo(HaveOccurred())

		// PubSub.ClientSetName only writes the command; the +OK reply is
		// drained here. This also synchronizes us with the server so that
		// the subsequent CLIENT LIST on a different connection observes
		// the rename (CLIENT LIST may otherwise race the SETNAME packet).
		_, err = pubsub.Receive(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Verify the name is set via CLIENT LIST
		clientList, err := client.ClientList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(clientList).To(ContainSubstring("name=my-subscriber"))
	})

	It("should ChannelMessage", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		// Wait for the SUBSCRIBE confirmation before calling Channel(), so the
		// server is guaranteed to have registered the subscription before we
		// Publish. Once Channel() starts its pump goroutine, all subsequent
		// reads happen there and Subscription frames are filtered out.
		_, err := pubsub.ReceiveTimeout(ctx, 5*time.Second)
		Expect(err).NotTo(HaveOccurred())

		ch := pubsub.Channel(
			redis.WithChannelSize(10),
			redis.WithChannelHealthCheckInterval(time.Second),
		)

		text := "test channel message"
		err = client.Publish(ctx, "mychannel", text).Err()
		Expect(err).NotTo(HaveOccurred())

		var msg *redis.Message
		Eventually(ch, 5*time.Second).Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("mychannel"))
		Expect(msg.Payload).To(Equal(text))
	})

	It("closes Channel when PubSub with sticky error is consumed", func() {
		ring := redis.NewRing(&redis.RingOptions{})
		defer ring.Close()

		// Ring.Subscribe with no channels returns a failedPubSub with stickyErr
		pubsub := ring.Subscribe(ctx)

		ch := pubsub.Channel()
		Eventually(ch, 5*time.Second).Should(BeClosed())

		// Verify range does not hang
		for range ch {
			Fail("should not receive messages from failed pubsub channel")
		}

		Expect(pubsub.Close()).To(Succeed())
	})

	It("closes ChannelWithSubscriptions when PubSub with sticky error is consumed", func() {
		ring := redis.NewRing(&redis.RingOptions{})
		defer ring.Close()

		pubsub := ring.Subscribe(ctx)

		ch := pubsub.ChannelWithSubscriptions()
		Eventually(ch, 5*time.Second).Should(BeClosed())

		for range ch {
			Fail("should not receive messages from failed pubsub channel")
		}

		Expect(pubsub.Close()).To(Succeed())
	})

	It("returns closed channel when calling conflicting Channel methods", func() {
		pubsub := client.Subscribe(ctx, "mychannel")
		defer pubsub.Close()

		ch1 := pubsub.Channel()
		Expect(ch1).NotTo(BeNil())

		// Second call with ChannelWithSubscriptions should return closed channel
		ch2 := pubsub.ChannelWithSubscriptions()
		Eventually(ch2, 5*time.Second).Should(BeClosed())
	})
})

//------------------------------------------------------------------------------
// Regression test for https://github.com/redis/go-redis/issues/3133
//
// Sharded pub/sub (SSUBSCRIBE) on a cluster client must receive messages from
// ALL subscribed channels, including channels whose hash slots are owned by
// different shards than the first subscribed channel. The test runs against
// a fake 3-shard RESP "cluster" (no real Redis needed): each shard owns a
// slot range, answers CLUSTER SLOTS, rejects SSUBSCRIBE/SPUBLISH for slots it
// does not own with MOVED (like a real cluster node), and pushes smessage to
// local subscriber connections on SPUBLISH.
//------------------------------------------------------------------------------

type fakeShardConn struct {
	conn      net.Conn
	wrMu      sync.Mutex
	channels  map[string]struct{}
	schannels map[string]struct{}
}

type fakeShardNode struct {
	cluster   *fakeShardCluster
	ln        net.Listener
	addr      string
	slotStart int
	slotEnd   int

	mu    sync.Mutex
	conns map[*fakeShardConn]struct{}
}

type fakeShardCluster struct {
	nodes []*fakeShardNode
}

func startFakeShardCluster(t *testing.T, shards int) *fakeShardCluster {
	t.Helper()
	c := &fakeShardCluster{}
	per := 16384 / shards
	for i := 0; i < shards; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		n := &fakeShardNode{
			cluster:   c,
			ln:        ln,
			addr:      ln.Addr().String(),
			slotStart: i * per,
			slotEnd:   (i+1)*per - 1,
			conns:     make(map[*fakeShardConn]struct{}),
		}
		c.nodes = append(c.nodes, n)
		go n.acceptLoop()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.ln.Close()
			n.mu.Lock()
			for fc := range n.conns {
				_ = fc.conn.Close()
			}
			n.mu.Unlock()
		}
	})
	return c
}

func (c *fakeShardCluster) clusterSlots() []redis.ClusterSlot {
	slots := make([]redis.ClusterSlot, 0, len(c.nodes))
	for _, n := range c.nodes {
		slots = append(slots, redis.ClusterSlot{
			Start: n.slotStart,
			End:   n.slotEnd,
			Nodes: []redis.ClusterNode{{ID: n.addr, Addr: n.addr}},
		})
	}
	return slots
}

func (c *fakeShardCluster) ownerOfSlot(slot int) *fakeShardNode {
	for _, n := range c.nodes {
		if slot >= n.slotStart && slot <= n.slotEnd {
			return n
		}
	}
	return nil
}

func (n *fakeShardNode) ownsSlot(slot int) bool {
	return slot >= n.slotStart && slot <= n.slotEnd
}

// subscribedCount reports on how many connections of this node the channel
// is currently shard-subscribed.
func (n *fakeShardNode) subscribedCount(ch string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for fc := range n.conns {
		if _, ok := fc.schannels[ch]; ok {
			count++
		}
	}
	return count
}

func (n *fakeShardNode) acceptLoop() {
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			return
		}
		fc := &fakeShardConn{
			conn:      conn,
			channels:  make(map[string]struct{}),
			schannels: make(map[string]struct{}),
		}
		n.mu.Lock()
		n.conns[fc] = struct{}{}
		n.mu.Unlock()
		go n.serve(fc)
	}
}

func readRESPArgs(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("bad command line: %q", line)
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		hdr, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("bad bulk header: %q", hdr)
		}
		size, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func respBulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

func (fc *fakeShardConn) write(s string) {
	fc.wrMu.Lock()
	_, _ = fc.conn.Write([]byte(s))
	fc.wrMu.Unlock()
}

func (n *fakeShardNode) serve(fc *fakeShardConn) {
	defer func() {
		n.mu.Lock()
		delete(n.conns, fc)
		n.mu.Unlock()
		_ = fc.conn.Close()
	}()
	br := bufio.NewReader(fc.conn)
	for {
		args, err := readRESPArgs(br)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		n.handle(fc, args)
	}
}

// movedReply mimics a real cluster node rejecting a command for a slot it
// does not serve.
func (c *fakeShardCluster) movedReply(slot int) string {
	owner := c.ownerOfSlot(slot)
	addr := "127.0.0.1:0"
	if owner != nil {
		addr = owner.addr
	}
	return fmt.Sprintf("-MOVED %d %s\r\n", slot, addr)
}

func (n *fakeShardNode) handle(fc *fakeShardConn, args []string) {
	cmd := strings.ToLower(args[0])
	switch cmd {
	case "hello":
		fc.write("%0\r\n")
	case "ping":
		fc.write("+PONG\r\n")
	case "command":
		fc.write("*0\r\n")
	case "client", "auth", "select", "readonly", "reset":
		fc.write("+OK\r\n")
	case "cluster":
		sub := strings.ToLower(args[1])
		switch sub {
		case "shards":
			fc.write("-ERR unknown subcommand\r\n")
		case "slots":
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(n.cluster.nodes))
			for _, nd := range n.cluster.nodes {
				host, port, _ := net.SplitHostPort(nd.addr)
				fmt.Fprintf(&b, "*3\r\n:%d\r\n:%d\r\n*3\r\n%s%s%s\r\n",
					nd.slotStart, nd.slotEnd,
					respBulk(host), respBulk(port), respBulk(nd.addr))
			}
			fc.write(b.String())
		default:
			fc.write("+OK\r\n")
		}
	case "ssubscribe", "sunsubscribe":
		channels := args[1:]
		if len(channels) == 0 {
			// unsubscribe from all shard channels on this connection
			n.mu.Lock()
			channels = make([]string, 0, len(fc.schannels))
			for ch := range fc.schannels {
				channels = append(channels, ch)
			}
			n.mu.Unlock()
		}
		for _, ch := range channels {
			slot := hashtag.Slot(ch)
			if !n.ownsSlot(slot) {
				fc.write(n.cluster.movedReply(slot))
				continue
			}
			n.mu.Lock()
			if cmd == "ssubscribe" {
				fc.schannels[ch] = struct{}{}
			} else {
				delete(fc.schannels, ch)
			}
			count := len(fc.schannels)
			n.mu.Unlock()
			fc.write(fmt.Sprintf("*3\r\n%s%s:%d\r\n", respBulk(cmd), respBulk(ch), count))
		}
		if len(channels) == 0 {
			n.mu.Lock()
			count := len(fc.schannels)
			n.mu.Unlock()
			fc.write(fmt.Sprintf("*3\r\n%s%s:%d\r\n", respBulk(cmd), respBulk(""), count))
		}
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		// regular pubsub is not used by this test; acknowledge only
		fc.write("+OK\r\n")
	case "spublish":
		ch, payload := args[1], args[2]
		slot := hashtag.Slot(ch)
		if !n.ownsSlot(slot) {
			fc.write(n.cluster.movedReply(slot))
			return
		}
		push := fmt.Sprintf("*3\r\n%s%s%s", respBulk("smessage"), respBulk(ch), respBulk(payload))
		n.mu.Lock()
		var targets []*fakeShardConn
		for sub := range n.conns {
			if _, ok := sub.schannels[ch]; ok {
				targets = append(targets, sub)
			}
		}
		n.mu.Unlock()
		for _, sub := range targets {
			sub.write(push)
		}
		fc.write(fmt.Sprintf(":%d\r\n", len(targets)))
	case "publish":
		fc.write(":0\r\n")
	default:
		fc.write("+OK\r\n")
	}
}

// TestPubSubShardedClusterAllShards is the regression test for issue #3133:
// a single cluster PubSub subscribed via SSubscribe to channels living on
// different shards must receive messages published to every one of them.
func TestPubSubShardedClusterAllShards(t *testing.T) {
	ctx := context.Background()
	cluster := startFakeShardCluster(t, 3)

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{cluster.nodes[0].addr},
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return cluster.clusterSlots(), nil
		},
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()

	// Pick at least one channel per shard so the subscription spans all
	// three slots owners.
	perShard := make(map[string][]string)
	var channels []string
	for i := 1; i <= 200 && len(perShard) < len(cluster.nodes); i++ {
		ch := fmt.Sprintf("ch%d", i)
		owner := cluster.ownerOfSlot(hashtag.Slot(ch))
		if _, ok := perShard[owner.addr]; !ok {
			perShard[owner.addr] = []string{ch}
			channels = append(channels, ch)
		}
	}
	if len(perShard) != len(cluster.nodes) {
		t.Fatalf("could not find channels on all %d shards", len(cluster.nodes))
	}

	pubsub := client.SSubscribe(ctx)
	defer pubsub.Close()
	for _, ch := range channels {
		if err := pubsub.SSubscribe(ctx, ch); err != nil {
			t.Fatalf("SSubscribe(%s): %v", ch, err)
		}
	}

	msgCh := pubsub.Channel()

	// Publish to every subscribed channel; SPUBLISH is routed by the client
	// to the shard owning the channel's slot.
	for i, ch := range channels {
		payload := fmt.Sprintf("hello-%d", i)
		if err := client.SPublish(ctx, ch, payload).Err(); err != nil {
			t.Fatalf("SPublish(%s): %v", ch, err)
		}
	}

	got := make(map[string]string)
	deadline := time.After(10 * time.Second)
	for len(got) < len(channels) {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				t.Fatalf("message channel closed after receiving %v", got)
			}
			got[msg.Channel] = msg.Payload
		case <-deadline:
			t.Fatalf("timed out waiting for messages: got %v, want one message on each of %v", got, channels)
		}
	}
	for i, ch := range channels {
		want := fmt.Sprintf("hello-%d", i)
		if got[ch] != want {
			t.Fatalf("channel %s: got payload %q, want %q", ch, got[ch], want)
		}
	}

	// SUnsubscribe must also reach the shard owning the channel's slot:
	// after unsubscribing from one channel, publishing to all channels again
	// must deliver to the remaining ones only.
	unsub := channels[0]
	if err := pubsub.SUnsubscribe(ctx, unsub); err != nil {
		t.Fatalf("SUnsubscribe(%s): %v", unsub, err)
	}
	// Wait until the owning shard has processed the SUNSUBSCRIBE so the
	// re-publish below cannot race it on a different connection.
	owner := cluster.ownerOfSlot(hashtag.Slot(unsub))
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if owner.subscribedCount(unsub) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := owner.subscribedCount(unsub); n != 0 {
		t.Fatalf("shard %s still has %d subscriptions for %s after SUnsubscribe", owner.addr, n, unsub)
	}
	for i, ch := range channels {
		if err := client.SPublish(ctx, ch, fmt.Sprintf("again-%d", i)).Err(); err != nil {
			t.Fatalf("SPublish(%s): %v", ch, err)
		}
	}
	gotAgain := make(map[string]string)
	deadline = time.After(10 * time.Second)
	for len(gotAgain) < len(channels)-1 {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				t.Fatalf("message channel closed after receiving %v", gotAgain)
			}
			gotAgain[msg.Channel] = msg.Payload
		case <-deadline:
			t.Fatalf("timed out waiting for messages after SUnsubscribe: got %v, want %v", gotAgain, channels[1:])
		}
	}
	if _, ok := gotAgain[unsub]; ok {
		t.Fatalf("received message on unsubscribed channel %s", unsub)
	}
}

// TestPubSubShardedClusterPingDoesNotDeadlock is the regression test for the
// mux Ping deadlock: Ping took c.mu and then locked it again in the mux
// branch, so every cluster PubSub froze on the first health check (the
// default Channel pings 3s after open), blocking Receive, SSubscribe and
// Close on the same mutex.
func TestPubSubShardedClusterPingDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	cluster := startFakeShardCluster(t, 3)

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{cluster.nodes[0].addr},
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return cluster.clusterSlots(), nil
		},
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()

	pubsub := client.SSubscribe(ctx)
	defer pubsub.Close()

	// One channel on each shard so the PubSub holds several mux connections.
	perShard := make(map[string]bool)
	var channels []string
	for i := 1; i <= 200 && len(perShard) < len(cluster.nodes); i++ {
		ch := fmt.Sprintf("pingch%d", i)
		owner := cluster.ownerOfSlot(hashtag.Slot(ch))
		if !perShard[owner.addr] {
			perShard[owner.addr] = true
			channels = append(channels, ch)
		}
	}
	for _, ch := range channels {
		if err := pubsub.SSubscribe(ctx, ch); err != nil {
			t.Fatalf("SSubscribe(%s): %v", ch, err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- pubsub.Ping(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ping: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Ping deadlocked on c.mu in mux mode")
	}

	// Receive must still work after the Ping (no mutex left held).
	for i, ch := range channels {
		if err := client.SPublish(ctx, ch, fmt.Sprintf("ping-%d", i)).Err(); err != nil {
			t.Fatalf("SPublish(%s): %v", ch, err)
		}
	}
	got := make(map[string]string)
	deadline := time.After(10 * time.Second)
	for len(got) < len(channels) {
		select {
		case msg, ok := <-pubsub.Channel():
			if !ok {
				t.Fatalf("message channel closed after receiving %v", got)
			}
			got[msg.Channel] = msg.Payload
		case <-deadline:
			t.Fatalf("timed out waiting for messages after Ping: got %v, want %v", got, channels)
		}
	}
}
