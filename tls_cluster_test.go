package redis_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("TLS Cluster", Label("NonRedisEnterprise"), func() {
	var tlsConfig *tls.Config
	var clusterAddrs []string

	BeforeEach(func() {
		var err error
		certDir := "dockers/osscluster-tls/tls"
		tlsConfig, err = loadClusterTLSConfig(certDir)
		Expect(err).NotTo(HaveOccurred())

		// TLS cluster ports from docker-compose.yml
		// TLS_PORT=5430 to avoid conflict with sentinel-cluster (which uses 4430-4432)
		clusterAddrs = []string{
			"localhost:5430",
			"localhost:5431",
			"localhost:5432",
			"localhost:5433",
			"localhost:5434",
			"localhost:5435",
		}
	})

	Describe("Cluster Client with TLS", func() {
		It("should connect to TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should perform basic operations over TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			// SET
			err := client.Set(ctx, "tls_cluster_key", "tls_cluster_value", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			// GET
			val, err := client.Get(ctx, "tls_cluster_key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("tls_cluster_value"))

			// DEL
			deleted, err := client.Del(ctx, "tls_cluster_key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(deleted).To(Equal(int64(1)))
		})

		It("should support pipelining over TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			// Use hash tags to ensure keys go to same slot
			pipe := client.Pipeline()
			setCmd := pipe.Set(ctx, "{tls}pipe_key1", "value1", 0)
			getCmd := pipe.Get(ctx, "{tls}pipe_key1")
			delCmd := pipe.Del(ctx, "{tls}pipe_key1")

			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(setCmd.Err()).NotTo(HaveOccurred())
			Expect(getCmd.Val()).To(Equal("value1"))
			Expect(delCmd.Val()).To(Equal(int64(1)))
		})

		It("should support cluster commands over TLS", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			// CLUSTER INFO
			info, err := client.ClusterInfo(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(ContainSubstring("cluster_state:ok"))

			// CLUSTER NODES
			nodes, err := client.ClusterNodes(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).NotTo(BeEmpty())

			// CLUSTER SLOTS
			slots, err := client.ClusterSlots(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(slots)).To(BeNumerically(">", 0))
		})

		It("should support pub/sub over TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			pubsub := client.Subscribe(ctx, "tls_cluster_channel")
			defer pubsub.Close()

			// Wait for subscription confirmation
			_, err := pubsub.Receive(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Publish a message
			err = client.Publish(ctx, "tls_cluster_channel", "tls_cluster_message").Err()
			Expect(err).NotTo(HaveOccurred())

			// Receive the message
			msg, err := pubsub.ReceiveMessage(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(msg.Channel).To(Equal("tls_cluster_channel"))
			Expect(msg.Payload).To(Equal("tls_cluster_message"))
		})

		It("should handle cluster redirects over TLS", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			// Set multiple keys that will be distributed across cluster
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("tls_redirect_key_%d", i)
				err := client.Set(ctx, key, fmt.Sprintf("value_%d", i), 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify all keys can be retrieved
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("tls_redirect_key_%d", i)
				val, err := client.Get(ctx, key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("value_%d", i)))
			}

			// Cleanup
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("tls_redirect_key_%d", i)
				client.Del(ctx, key)
			}
		})

		It("should connect with InsecureSkipVerify to TLS cluster", func() {
			ctx := context.Background()
			insecureTLSConfig := &tls.Config{
				InsecureSkipVerify: true,
			}

			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: insecureTLSConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should support ForEachShard over TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			var shardCount int64
			err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
				atomic.AddInt64(&shardCount, 1)
				return shard.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(atomic.LoadInt64(&shardCount)).To(BeNumerically(">", 0))
		})

		It("should support ForEachMaster over TLS cluster", func() {
			ctx := context.Background()
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:     clusterAddrs,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// Wait for cluster to be ready
			Eventually(func() error {
				return client.Ping(ctx).Err()
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			var masterCount int64
			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				atomic.AddInt64(&masterCount, 1)
				return master.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(atomic.LoadInt64(&masterCount)).To(BeNumerically(">", 0))
		})
	})

	Describe("TLS Cluster URL Parsing", func() {
		It("should parse rediss:// URL for cluster", func() {
			opt, err := redis.ParseClusterURL("rediss://localhost:16800?addr=localhost:16801&addr=localhost:16802")
			Expect(err).NotTo(HaveOccurred())
			Expect(opt.Addrs).To(ContainElements("localhost:16800", "localhost:16801", "localhost:16802"))
			Expect(opt.TLSConfig).NotTo(BeNil())
			Expect(opt.TLSConfig.ServerName).To(Equal("localhost"))
		})
	})
})

