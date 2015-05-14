package redis_test

import (
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("PubSub", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should support pattern matching", func() {
		pubsub := client.PubSub()
		defer func() {
			Expect(pubsub.Close()).NotTo(HaveOccurred())
		}()

		Expect(pubsub.PSubscribe("mychannel*")).NotTo(HaveOccurred())

		pub := client.Publish("mychannel1", "hello")
		Expect(pub.Err()).NotTo(HaveOccurred())
		Expect(pub.Val()).To(Equal(int64(1)))

		Expect(pubsub.PUnsubscribe("mychannel*")).NotTo(HaveOccurred())

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("psubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel*"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.PMessage)
			Expect(subscr.Channel).To(Equal("mychannel1"))
			Expect(subscr.Pattern).To(Equal("mychannel*"))
			Expect(subscr.Payload).To(Equal("hello"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("punsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel*"))
			Expect(subscr.Count).To(Equal(0))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err.(net.Error).Timeout()).To(Equal(true))
			Expect(msgi).NotTo(HaveOccurred())
		}
	})

	It("should pub/sub channels", func() {
		channels, err := client.PubSubChannels("mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		pubsub := client.PubSub()
		defer pubsub.Close()
		Expect(pubsub.Subscribe("mychannel", "mychannel2")).NotTo(HaveOccurred())

		channels, err = client.PubSubChannels("mychannel*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(ConsistOf([]string{"mychannel", "mychannel2"}))

		channels, err = client.PubSubChannels("").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(BeEmpty())

		channels, err = client.PubSubChannels("*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(channels)).To(BeNumerically(">=", 2))
	})

	It("should return the numbers of subscribers", func() {
		pubsub := client.PubSub()
		defer pubsub.Close()
		Expect(pubsub.Subscribe("mychannel", "mychannel2")).NotTo(HaveOccurred())

		channels, err := client.PubSubNumSub("mychannel", "mychannel2", "mychannel3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(channels).To(Equal(map[string]int64{
			"mychannel":  1,
			"mychannel2": 1,
			"mychannel3": 0,
		}))
	})

	It("should return the numbers of subscribers by pattern", func() {
		num, err := client.PubSubNumPat().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(0)))

		pubsub := client.PubSub()
		defer pubsub.Close()
		Expect(pubsub.PSubscribe("*")).NotTo(HaveOccurred())

		num, err = client.PubSubNumPat().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(1)))
	})

	It("should pub/sub", func() {
		pubsub := client.PubSub()
		defer func() {
			Expect(pubsub.Close()).NotTo(HaveOccurred())
		}()

		Expect(pubsub.Subscribe("mychannel", "mychannel2")).NotTo(HaveOccurred())

		pub := client.Publish("mychannel", "hello")
		Expect(pub.Err()).NotTo(HaveOccurred())
		Expect(pub.Val()).To(Equal(int64(1)))

		pub = client.Publish("mychannel2", "hello2")
		Expect(pub.Err()).NotTo(HaveOccurred())
		Expect(pub.Val()).To(Equal(int64(1)))

		Expect(pubsub.Unsubscribe("mychannel", "mychannel2")).NotTo(HaveOccurred())

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("subscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("subscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(2))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Message)
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Payload).To(Equal("hello"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			msg := msgi.(*redis.Message)
			Expect(msg.Channel).To(Equal("mychannel2"))
			Expect(msg.Payload).To(Equal("hello2"))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("unsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel"))
			Expect(subscr.Count).To(Equal(1))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err).NotTo(HaveOccurred())
			subscr := msgi.(*redis.Subscription)
			Expect(subscr.Kind).To(Equal("unsubscribe"))
			Expect(subscr.Channel).To(Equal("mychannel2"))
			Expect(subscr.Count).To(Equal(0))
		}

		{
			msgi, err := pubsub.ReceiveTimeout(time.Second)
			Expect(err.(net.Error).Timeout()).To(Equal(true))
			Expect(msgi).NotTo(HaveOccurred())
		}
	})

})
