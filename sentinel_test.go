package redis_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sort"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Sentinel PROTO 2", func() {
	var client *redis.Client
	BeforeEach(func() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			MaxRetries:    -1,
			Protocol:      2,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.Close()
	})

	It("should sentinel client PROTO 2", func() {
		val, err := client.Do(ctx, "HELLO").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(ContainElements("proto", int64(2)))
	})
})

var _ = Describe("Sentinel resolution", func() {
	It("should resolve master without context exhaustion", func() {
		shortCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			MaxRetries:    -1,
		})

		err := client.Ping(shortCtx).Err()
		Expect(err).NotTo(HaveOccurred(), "expected master to resolve without context exhaustion")

		_ = client.Close()
	})
})

var _ = Describe("Sentinel", func() {
	var client *redis.Client
	var master *redis.Client
	var sentinel *redis.SentinelClient

	BeforeEach(func() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			ClientName:    "sentinel_hi",
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			MaxRetries:    -1,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel = redis.NewSentinelClient(&redis.Options{
			Addr:       ":" + sentinelPort1,
			MaxRetries: -1,
		})

		addr, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		master = redis.NewClient(&redis.Options{
			Addr:       net.JoinHostPort(addr[0], addr[1]),
			MaxRetries: -1,
		})

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel1.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel2.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel3.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
	})

	AfterEach(func() {
		_ = client.Close()
		_ = master.Close()
		_ = sentinel.Close()
	})

	It("should facilitate failover", func() {
		// Set value on master.
		err := client.Set(ctx, "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify.
		val, err := client.Get(ctx, "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Verify master->slaves sync.
		var slavesAddr []string
		Eventually(func() []string {
			slavesAddr = redis.GetSlavesAddrByName(ctx, sentinel, sentinelName)
			return slavesAddr
		}, "20s", "50ms").Should(HaveLen(2))
		Eventually(func() bool {
			sync := true
			for _, addr := range slavesAddr {
				slave := redis.NewClient(&redis.Options{
					Addr:       addr,
					MaxRetries: -1,
				})
				sync = slave.Get(ctx, "foo").Val() == "master"
				_ = slave.Close()
			}
			return sync
		}, "20s", "50ms").Should(BeTrue())

		// Create subscription.
		pub := client.Subscribe(ctx, "foo")
		ch := pub.Channel()

		// Kill master.
		/*
			err = master.Shutdown(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return master.Ping(ctx).Err()
			}, "20s", "50ms").Should(HaveOccurred())
		*/

		// Check that client picked up new master.
		Eventually(func() string {
			return client.Get(ctx, "foo").Val()
		}, "20s", "100ms").Should(Equal("master"))

		// Check if subscription is renewed.
		var msg *redis.Message
		Eventually(func() <-chan *redis.Message {
			_ = client.Publish(ctx, "foo", "hello").Err()
			return ch
		}, "20s", "100ms").Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
		Expect(pub.Close()).NotTo(HaveOccurred())
	})

	It("supports DB selection", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			DB:            1,
		})
		err := client.Ping(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should sentinel client setname", func() {
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		val, err := client.ClientList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(ContainSubstring("name=sentinel_hi"))
	})

	It("should sentinel client PROTO 3", func() {
		val, err := client.Do(ctx, "HELLO").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
	})
})

var _ = Describe("NewFailoverClusterClient PROTO 2", func() {
	var client *redis.ClusterClient

	BeforeEach(func() {
		client = redis.NewFailoverClusterClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			Protocol:      2,

			RouteRandomly: true,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.Close()
	})

	It("should sentinel cluster PROTO 2", func() {
		_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := client.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainElements("proto", int64(2)))
			return nil
		})
	})
})

var _ = Describe("NewFailoverClusterClient", func() {
	var client *redis.ClusterClient
	var master *redis.Client

	BeforeEach(func() {
		client = redis.NewFailoverClusterClient(&redis.FailoverOptions{
			ClientName:    "sentinel_cluster_hi",
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,

			RouteRandomly: true,
			DB:            1,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel := redis.NewSentinelClient(&redis.Options{
			Addr:       ":" + sentinelPort1,
			MaxRetries: -1,
		})

		addr, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		master = redis.NewClient(&redis.Options{
			Addr:       net.JoinHostPort(addr[0], addr[1]),
			MaxRetries: -1,
		})

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel1.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel2.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel3.Info(ctx).Val()
		}, "20s", "100ms").Should(ContainSubstring("slaves=2"))
	})

	AfterEach(func() {
		_ = client.Close()
		_ = master.Close()
	})

	It("should facilitate failover", func() {
		// Set value.
		err := client.Set(ctx, "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 100; i++ {
			// Verify.
			Eventually(func() string {
				return client.Get(ctx, "foo").Val()
			}, "20s", "1ms").Should(Equal("master"))
		}

		// Create subscription.
		sub := client.Subscribe(ctx, "foo")
		ch := sub.Channel()

		// Kill master.
		/*
			err = master.Shutdown(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return master.Ping(ctx).Err()
			}, "20s", "100ms").Should(HaveOccurred())
		*/

		// Check that client picked up new master.
		Eventually(func() string {
			return client.Get(ctx, "foo").Val()
		}, "20s", "100ms").Should(Equal("master"))

		// Check if subscription is renewed.
		var msg *redis.Message
		Eventually(func() <-chan *redis.Message {
			_ = client.Publish(ctx, "foo", "hello").Err()
			return ch
		}, "20s", "100ms").Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
		Expect(sub.Close()).NotTo(HaveOccurred())

	})

	It("should sentinel cluster client setname", func() {
		err := client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			return c.Ping(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())

		_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := c.ClientList(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainSubstring("name=sentinel_cluster_hi"))
			return nil
		})
	})

	It("should sentinel cluster client db", func() {
		err := client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			return c.Ping(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())

		_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			clientInfo, err := c.ClientInfo(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(clientInfo.DB).To(Equal(1))
			return nil
		})
	})

	It("should sentinel cluster PROTO 3", func() {
		_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := client.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
			return nil
		})
	})
})

var _ = Describe("SentinelAclAuth", func() {
	const (
		aclSentinelUsername = "sentinel-user"
		aclSentinelPassword = "sentinel-pass"
	)

	var client *redis.Client
	var sentinel *redis.SentinelClient
	sentinels := func() []*redis.Client {
		return []*redis.Client{sentinel1, sentinel2, sentinel3}
	}

	BeforeEach(func() {
		authCmd := redis.NewStatusCmd(ctx, "ACL", "SETUSER", aclSentinelUsername, "ON",
			">"+aclSentinelPassword, "-@all", "+auth", "+client|getname", "+client|id", "+client|setname",
			"+command", "+hello", "+ping", "+client|setinfo", "+role", "+sentinel|get-master-addr-by-name", "+sentinel|master",
			"+sentinel|myid", "+sentinel|replicas", "+sentinel|sentinels")

		for _, process := range sentinels() {
			err := process.Process(ctx, authCmd)
			Expect(err).NotTo(HaveOccurred())
		}

		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       sentinelName,
			SentinelAddrs:    sentinelAddrs,
			MaxRetries:       -1,
			SentinelUsername: aclSentinelUsername,
			SentinelPassword: aclSentinelPassword,
		})

		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel = redis.NewSentinelClient(&redis.Options{
			Addr:       sentinelAddrs[0],
			MaxRetries: -1,
			Username:   aclSentinelUsername,
			Password:   aclSentinelPassword,
		})

		_, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		// Wait until sentinels are picked up by each other.
		for _, process := range sentinels() {
			Eventually(func() string {
				return process.Info(ctx).Val()
			}, "20s", "100ms").Should(ContainSubstring("sentinels=3"))
		}
	})

	AfterEach(func() {
		unauthCommand := redis.NewStatusCmd(ctx, "ACL", "DELUSER", aclSentinelUsername)

		for _, process := range sentinels() {
			err := process.Process(ctx, unauthCommand)
			Expect(err).NotTo(HaveOccurred())
		}

		_ = client.Close()
		_ = sentinel.Close()
	})

	It("should still facilitate operations", func() {
		err := client.Set(ctx, "wow", "acl-auth", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := client.Get(ctx, "wow").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("acl-auth"))
	})
})

func TestParseFailoverURL(t *testing.T) {
	cases := []struct {
		url string
		o   *redis.FailoverOptions
		err error
	}{
		{
			url: "redis://localhost:6379?master_name=test",
			o:   &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6379"}, MasterName: "test"},
		},
		{
			url: "redis://localhost:6379/5?master_name=test",
			o:   &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6379"}, MasterName: "test", DB: 5},
		},
		{
			url: "rediss://localhost:6379/5?master_name=test",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6379"}, MasterName: "test", DB: 5,
				TLSConfig: &tls.Config{
					ServerName: "localhost",
				}},
		},
		{
			url: "redis://localhost:6379/5?master_name=test&db=2",
			o:   &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6379"}, MasterName: "test", DB: 2},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&addr=localhost:6381",
			o:   &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379", "localhost:6381"}, DB: 5},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5},
		},
		{
			url: "redis://:bar@localhost:6379/5?addr=localhost:6380",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "", SentinelPassword: "bar", DB: 5},
		},
		{
			url: "redis://foo@localhost:6379/5?addr=localhost:6380",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "", DB: 5},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3s",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3ms",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Millisecond},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo=true",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second, PoolFIFO: true},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo=false",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: 3 * time.Second, PoolFIFO: false},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: 3 * time.Second, PoolFIFO: false},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: 0},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=0",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: -1},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=-1",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: -1},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=-2",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: -1},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: 0},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=0&abc=5",
			o: &redis.FailoverOptions{SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB: 5, DialTimeout: -1},
			err: errors.New("redis: unexpected option: abc"),
		},
		{
			url: "http://google.com",
			err: errors.New("redis: invalid URL scheme: http"),
		},
		{
			url: "redis://localhost/1/2/3/4",
			err: errors.New("redis: invalid URL path: /1/2/3/4"),
		},
		{
			url: "12345",
			err: errors.New("redis: invalid URL scheme: "),
		},
		{
			url: "redis://localhost/database",
			err: errors.New(`redis: invalid database number: "database"`),
		},
	}

	for i := range cases {
		tc := cases[i]
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()

			actual, err := redis.ParseFailoverURL(tc.url)
			if tc.err == nil && err != nil {
				t.Fatalf("unexpected error: %q", err)
				return
			}
			if tc.err != nil && err == nil {
				t.Fatalf("got nil, expected %q", tc.err)
				return
			}
			if tc.err != nil && err != nil {
				if tc.err.Error() != err.Error() {
					t.Fatalf("got %q, expected %q", err, tc.err)
				}
				return
			}
			compareFailoverOptions(t, actual, tc.o)
		})
	}
}

func compareFailoverOptions(t *testing.T, a, e *redis.FailoverOptions) {
	if a.MasterName != e.MasterName {
		t.Errorf("MasterName got %q, want %q", a.MasterName, e.MasterName)
	}
	compareSlices(t, a.SentinelAddrs, e.SentinelAddrs, "SentinelAddrs")
	if a.ClientName != e.ClientName {
		t.Errorf("ClientName got %q, want %q", a.ClientName, e.ClientName)
	}
	if a.SentinelUsername != e.SentinelUsername {
		t.Errorf("SentinelUsername got %q, want %q", a.SentinelUsername, e.SentinelUsername)
	}
	if a.SentinelPassword != e.SentinelPassword {
		t.Errorf("SentinelPassword got %q, want %q", a.SentinelPassword, e.SentinelPassword)
	}
	if a.RouteByLatency != e.RouteByLatency {
		t.Errorf("RouteByLatency got %v, want %v", a.RouteByLatency, e.RouteByLatency)
	}
	if a.RouteRandomly != e.RouteRandomly {
		t.Errorf("RouteRandomly got %v, want %v", a.RouteRandomly, e.RouteRandomly)
	}
	if a.ReplicaOnly != e.ReplicaOnly {
		t.Errorf("ReplicaOnly got %v, want %v", a.ReplicaOnly, e.ReplicaOnly)
	}
	if a.UseDisconnectedReplicas != e.UseDisconnectedReplicas {
		t.Errorf("UseDisconnectedReplicas got %v, want %v", a.UseDisconnectedReplicas, e.UseDisconnectedReplicas)
	}
	if a.Protocol != e.Protocol {
		t.Errorf("Protocol got %v, want %v", a.Protocol, e.Protocol)
	}
	if a.Username != e.Username {
		t.Errorf("Username got %q, want %q", a.Username, e.Username)
	}
	if a.Password != e.Password {
		t.Errorf("Password got %q, want %q", a.Password, e.Password)
	}
	if a.DB != e.DB {
		t.Errorf("DB got %v, want %v", a.DB, e.DB)
	}
	if a.MaxRetries != e.MaxRetries {
		t.Errorf("MaxRetries got %v, want %v", a.MaxRetries, e.MaxRetries)
	}
	if a.MinRetryBackoff != e.MinRetryBackoff {
		t.Errorf("MinRetryBackoff got %v, want %v", a.MinRetryBackoff, e.MinRetryBackoff)
	}
	if a.MaxRetryBackoff != e.MaxRetryBackoff {
		t.Errorf("MaxRetryBackoff got %v, want %v", a.MaxRetryBackoff, e.MaxRetryBackoff)
	}
	if a.DialTimeout != e.DialTimeout {
		t.Errorf("DialTimeout got %v, want %v", a.DialTimeout, e.DialTimeout)
	}
	if a.ReadTimeout != e.ReadTimeout {
		t.Errorf("ReadTimeout got %v, want %v", a.ReadTimeout, e.ReadTimeout)
	}
	if a.WriteTimeout != e.WriteTimeout {
		t.Errorf("WriteTimeout got %v, want %v", a.WriteTimeout, e.WriteTimeout)
	}
	if a.ContextTimeoutEnabled != e.ContextTimeoutEnabled {
		t.Errorf("ContentTimeoutEnabled got %v, want %v", a.ContextTimeoutEnabled, e.ContextTimeoutEnabled)
	}
	if a.PoolFIFO != e.PoolFIFO {
		t.Errorf("PoolFIFO got %v, want %v", a.PoolFIFO, e.PoolFIFO)
	}
	if a.PoolSize != e.PoolSize {
		t.Errorf("PoolSize got %v, want %v", a.PoolSize, e.PoolSize)
	}
	if a.PoolTimeout != e.PoolTimeout {
		t.Errorf("PoolTimeout got %v, want %v", a.PoolTimeout, e.PoolTimeout)
	}
	if a.MinIdleConns != e.MinIdleConns {
		t.Errorf("MinIdleConns got %v, want %v", a.MinIdleConns, e.MinIdleConns)
	}
	if a.MaxIdleConns != e.MaxIdleConns {
		t.Errorf("MaxIdleConns got %v, want %v", a.MaxIdleConns, e.MaxIdleConns)
	}
	if a.MaxActiveConns != e.MaxActiveConns {
		t.Errorf("MaxActiveConns got %v, want %v", a.MaxActiveConns, e.MaxActiveConns)
	}
	if a.ConnMaxIdleTime != e.ConnMaxIdleTime {
		t.Errorf("ConnMaxIdleTime got %v, want %v", a.ConnMaxIdleTime, e.ConnMaxIdleTime)
	}
	if a.ConnMaxLifetime != e.ConnMaxLifetime {
		t.Errorf("ConnMaxLifeTime got %v, want %v", a.ConnMaxLifetime, e.ConnMaxLifetime)
	}
	if a.DisableIdentity != e.DisableIdentity {
		t.Errorf("DisableIdentity got %v, want %v", a.DisableIdentity, e.DisableIdentity)
	}
	if a.IdentitySuffix != e.IdentitySuffix {
		t.Errorf("IdentitySuffix got %v, want %v", a.IdentitySuffix, e.IdentitySuffix)
	}
	if a.UnstableResp3 != e.UnstableResp3 {
		t.Errorf("UnstableResp3 got %v, want %v", a.UnstableResp3, e.UnstableResp3)
	}
	if (a.TLSConfig == nil && e.TLSConfig != nil) || (a.TLSConfig != nil && e.TLSConfig == nil) {
		t.Errorf("TLSConfig error")
	}
	if a.TLSConfig != nil && e.TLSConfig != nil {
		if a.TLSConfig.ServerName != e.TLSConfig.ServerName {
			t.Errorf("TLSConfig.ServerName got %q, want %q", a.TLSConfig.ServerName, e.TLSConfig.ServerName)
		}
	}
}

func compareSlices(t *testing.T, a, b []string, name string) {
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	if len(a) != len(b) {
		t.Errorf("%s got %q, want %q", name, a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("%s got %q, want %q", name, a, b)
		}
	}
}
