package redis_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

// loadTLSConfig loads TLS certificates from the docker test environment
func loadTLSConfig(certDir string) (*tls.Config, error) {
	// Load CA cert
	caCert, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Load client cert and key
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(certDir, "client.crt"),
		filepath.Join(certDir, "client.key"),
	)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		ServerName:   "localhost",
		InsecureSkipVerify: true,
	}, nil
}

var _ = Describe("TLS", Label("NonRedisEnterprise"), func() {
	var tlsConfig *tls.Config
	var tlsPort = "6666" // TLS port from docker-compose.yml

	BeforeEach(func() {
		var err error
		// Load TLS config from docker test certificates
		tlsConfig, err = loadTLSConfig("dockers/standalone/tls")
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Standalone Redis with TLS", func() {
		It("should connect with TLS using Options", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should connect with TLS using rediss:// URL", func() {
			ctx := context.Background()
			opt, err := redis.ParseURL("rediss://localhost:" + tlsPort)
			Expect(err).NotTo(HaveOccurred())

			// Override TLS config with our certificates
			opt.TLSConfig = tlsConfig

			client := redis.NewClient(opt)
			defer client.Close()

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should perform basic operations over TLS", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			// SET
			err := client.Set(ctx, "tls_test_key", "tls_test_value", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			// GET
			val, err := client.Get(ctx, "tls_test_key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("tls_test_value"))

			// DEL
			deleted, err := client.Del(ctx, "tls_test_key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(deleted).To(Equal(int64(1)))
		})

		It("should support pipelining over TLS", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			pipe := client.Pipeline()
			setCmd := pipe.Set(ctx, "tls_pipe_key1", "value1", 0)
			getCmd := pipe.Get(ctx, "tls_pipe_key1")
			delCmd := pipe.Del(ctx, "tls_pipe_key1")

			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(setCmd.Err()).NotTo(HaveOccurred())
			Expect(getCmd.Val()).To(Equal("value1"))
			Expect(delCmd.Val()).To(Equal(int64(1)))
		})

		It("should support transactions over TLS", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			err := client.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "tls_tx_key", "tx_value", 0)
					pipe.Get(ctx, "tls_tx_key")
					return nil
				})
				return err
			})
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "tls_tx_key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("tx_value"))

			// Cleanup
			client.Del(ctx, "tls_tx_key")
		})

		It("should support pub/sub over TLS", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
			})
			defer client.Close()

			pubsub := client.Subscribe(ctx, "tls_test_channel")
			defer pubsub.Close()

			// Wait for subscription confirmation
			_, err := pubsub.Receive(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Publish a message
			err = client.Publish(ctx, "tls_test_channel", "tls_test_message").Err()
			Expect(err).NotTo(HaveOccurred())

			// Receive the message
			msg, err := pubsub.ReceiveMessage(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(msg.Channel).To(Equal("tls_test_channel"))
			Expect(msg.Payload).To(Equal("tls_test_message"))
		})

		It("should fail to connect without TLS to TLS port", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: "localhost:" + tlsPort,
				// No TLS config
			})
			defer client.Close()

			err := client.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
		})

		It("should fail to connect with invalid certificates", func() {
			ctx := context.Background()
			invalidTLSConfig := &tls.Config{
				InsecureSkipVerify: false,
				ServerName:         "invalid.example.com",
			}

			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: invalidTLSConfig,
			})
			defer client.Close()

			err := client.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
		})

		It("should connect with InsecureSkipVerify", func() {
			ctx := context.Background()
			insecureTLSConfig := &tls.Config{
				InsecureSkipVerify: true,
			}

			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: insecureTLSConfig,
			})
			defer client.Close()

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should support connection pooling over TLS", func() {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfig,
				PoolSize:  10,
			})
			defer client.Close()

			// Perform multiple operations to use different connections
			for i := 0; i < 20; i++ {
				err := client.Ping(ctx).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			stats := client.PoolStats()
			Expect(stats.Hits).To(BeNumerically(">", 0))
		})
	})

	Describe("TLS Configuration Options", func() {
		It("should respect MinVersion setting", func() {
			ctx := context.Background()
			tlsConfigWithMinVersion := &tls.Config{
				RootCAs:      tlsConfig.RootCAs,
				Certificates: tlsConfig.Certificates,
				ServerName:   "localhost",
		InsecureSkipVerify: true,
				MinVersion:   tls.VersionTLS12,
			}

			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfigWithMinVersion,
			})
			defer client.Close()

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should work with custom cipher suites", func() {
			ctx := context.Background()
			tlsConfigWithCiphers := &tls.Config{
				RootCAs:      tlsConfig.RootCAs,
				Certificates: tlsConfig.Certificates,
				ServerName:   "localhost",
		InsecureSkipVerify: true,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				},
			}

			client := redis.NewClient(&redis.Options{
				Addr:      "localhost:" + tlsPort,
				TLSConfig: tlsConfigWithCiphers,
			})
			defer client.Close()

			val, err := client.Ping(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})
	})
})

