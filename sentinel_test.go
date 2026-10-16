package redis_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/pool"
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
		// Wide enough for a loaded CI runner (sentinel query + master dial +
		// handshake over docker networking routinely needs >500ms there),
		// while still far below the retry-storm exhaustion this test pins.
		shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
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
			defer GinkgoRecover()
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
			defer GinkgoRecover()
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
			defer GinkgoRecover()
			clientInfo, err := c.ClientInfo(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(clientInfo.DB).To(Equal(1))
			return nil
		})
	})

	It("should sentinel cluster PROTO 3", func() {
		_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			defer GinkgoRecover()
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

// renaming from TestParseFailoverURL to TestParseSentinelURL
// to be easier to find Failed tests in the test output
func TestParseSentinelURL(t *testing.T) {
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
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6379"}, MasterName: "test", DB: 5,
				TLSConfig: &tls.Config{
					ServerName: "localhost",
				},
			},
		},
		{
			url: "rediss://localhost:6379/5?master_name=test&skip_verify=true",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6379"}, MasterName: "test", DB: 5,
				TLSConfig: &tls.Config{
					ServerName:         "localhost",
					InsecureSkipVerify: true,
				},
			},
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
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5,
			},
		},
		{
			url: "redis://:bar@localhost:6379/5?addr=localhost:6380",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "", SentinelPassword: "bar", DB: 5,
			},
		},
		{
			url: "redis://foo@localhost:6379/5?addr=localhost:6380",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "", DB: 5,
			},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second,
			},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3s",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second,
			},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3ms",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Millisecond,
			},
		},
		{
			url: "redis://foo:bar@localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo=true",
			o: &redis.FailoverOptions{
				SentinelAddrs:    []string{"localhost:6380", "localhost:6379"},
				SentinelUsername: "foo", SentinelPassword: "bar", DB: 5, DialTimeout: 3 * time.Second, PoolFIFO: true,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo=false",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: 3 * time.Second, PoolFIFO: false,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=3&pool_fifo",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: 3 * time.Second, PoolFIFO: false,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: 0,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=0",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: -1,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=-1",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: -1,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=-2",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: -1,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: 0,
			},
		},
		{
			url: "redis://localhost:6379/5?addr=localhost:6380&dial_timeout=0&abc=5",
			o: &redis.FailoverOptions{
				SentinelAddrs: []string{"localhost:6380", "localhost:6379"},
				DB:            5, DialTimeout: -1,
			},
			err: errors.New("redis: unexpected option: abc"),
		},
		{
			url: "rediss://localhost:6379/5?master_name=test&skip_verify=yes",
			err: errors.New(`redis: invalid skip_verify boolean: expected true/false/1/0 or an empty string, got "yes"`),
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
	if a.ConnMaxLifetimeJitter != e.ConnMaxLifetimeJitter {
		t.Errorf("ConnMaxLifetimeJitter got %v, want %v", a.ConnMaxLifetimeJitter, e.ConnMaxLifetimeJitter)
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
	slices.Sort(a)
	slices.Sort(b)
	if len(a) != len(b) {
		t.Errorf("%s got %q, want %q", name, a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("%s got %q, want %q", name, a, b)
		}
	}
}

// countUsableReplicas mirrors the filter applied by parseReplicaAddrs in
// sentinel.go: a replica is usable only when it is not s_down, o_down, or
// disconnected. Plain len() on Sentinel's Replicas reply is not enough —
// post-failover, replicas often linger with the "disconnected" flag for
// several seconds after Sentinel agrees on master/replica counts, and a
// FailoverClient(ReadOnly:true) created in that window will see an empty
// usable list and silently fall back to the master (see RandomReplicaAddr
// in sentinel.go).
func countUsableReplicas(replicas []map[string]string) int {
	usable := 0
	for _, node := range replicas {
		down := false
		for _, flag := range strings.Split(node["flags"], ",") {
			switch flag {
			case "s_down", "o_down", "disconnected":
				down = true
			}
		}
		if !down && node["ip"] != "" && node["port"] != "" {
			usable++
		}
	}
	return usable
}

func waitForSentinelClusterStable() {
	sentinel1 := redis.NewSentinelClient(&redis.Options{
		Addr: ":" + sentinelPort1,
	})
	defer sentinel1.Close()

	sentinel2 := redis.NewSentinelClient(&redis.Options{
		Addr: ":" + sentinelPort2,
	})
	defer sentinel2.Close()

	sentinel3 := redis.NewSentinelClient(&redis.Options{
		Addr: ":" + sentinelPort3,
	})
	defer sentinel3.Close()

	Eventually(func() bool {
		masterInfo1, err1 := sentinel1.Master(ctx, sentinelName).Result()
		masterInfo2, err2 := sentinel2.Master(ctx, sentinelName).Result()
		masterInfo3, err3 := sentinel3.Master(ctx, sentinelName).Result()

		if err1 != nil || err2 != nil || err3 != nil {
			return false
		}
		// Check master ip and port are consistent across all sentinels
		if masterInfo1["ip"] != masterInfo2["ip"] ||
			masterInfo1["port"] != masterInfo2["port"] ||
			masterInfo2["ip"] != masterInfo3["ip"] ||
			masterInfo2["port"] != masterInfo3["port"] {
			return false
		}

		// Each sentinel must report at least 2 usable (non-down,
		// non-disconnected) replicas. Just counting replicas isn't enough:
		// production code filters disconnected replicas out, so a sentinel
		// reporting "2 replicas, both disconnected" yields 0 usable nodes
		// and triggers the silent master fallback.
		replicas1, err1 := sentinel1.Replicas(ctx, sentinelName).Result()
		replicas2, err2 := sentinel2.Replicas(ctx, sentinelName).Result()
		replicas3, err3 := sentinel3.Replicas(ctx, sentinelName).Result()

		if err1 != nil || err2 != nil || err3 != nil {
			return false
		}
		u1, u2, u3 := countUsableReplicas(replicas1), countUsableReplicas(replicas2), countUsableReplicas(replicas3)
		if u1 < 2 || u2 < 2 || u3 < 2 {
			return false
		}
		return u1 == u2 && u2 == u3
	}, "30s", "1s").Should(BeTrue())

	// End-to-end probe: open the same kind of client the post-failover
	// ReadOnly spec uses and confirm it actually routes to a replica.
	// This is the deterministic equivalent of the previous 10-second
	// time.Sleep — succeeds as soon as the precondition is met, instead of
	// hoping 10 seconds is long enough for replicas to come out of the
	// "disconnected" state on every sentinel.
	Eventually(func() bool {
		c := redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName: sentinelName,
			Addrs:      sentinelAddrs,
			ReadOnly:   true,
		})
		defer c.Close()
		if err := c.Ping(ctx).Err(); err != nil {
			return false
		}
		role, err := c.Do(ctx, "ROLE").Result()
		if err != nil {
			return false
		}
		roleSlice, ok := role.([]interface{})
		if !ok || len(roleSlice) == 0 {
			return false
		}
		return roleSlice[0] == "slave"
	}, "30s", "500ms").Should(BeTrue())
}

var _ = Describe("Sentinel Failover with Conns", Serial, Ordered, func() {
	testFailoverWithMaxActiveConns := func(maxActiveConns int) {
		var client *redis.Client
		var sentinel *redis.SentinelClient
		var failoverCloseCount atomic.Int32

		BeforeEach(func() {
			failoverCloseCount.Store(0)

			// Set up metric callback to count CloseReasonFailover
			pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
				ConnectionClosed: func(ctx context.Context, cn *pool.Conn, reason string, err error) {
					fmt.Println("call ConnectionClosed metric callback with reason:", reason)
					if reason == pool.CloseReasonFailover {
						failoverCloseCount.Add(1)
					}
				},
			})

			client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:      sentinelName,
				SentinelAddrs:   sentinelAddrs,
				PoolSize:        1,
				DisableIdentity: true,
				MaxActiveConns:  maxActiveConns,
				ReplicaOnly:     false,
				MaxRetries:      -1,
			})
			Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

			sentinel = redis.NewSentinelClient(&redis.Options{
				Addr:       ":" + sentinelPort1,
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
			_ = sentinel.Close()
			pool.SetAllMetricCallbacks(nil)
		})

		It(fmt.Sprintf("should handle failover with MaxActiveConns=%d", maxActiveConns), func() {
			err := client.Set(ctx, "failover", "100", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			// Sleep 2 second for replication
			time.Sleep(2 * time.Second)

			// Trigger failover
			err = sentinel.Failover(ctx, sentinelName).Err()
			Expect(err).NotTo(HaveOccurred())

			for range 120 {
				masterInfo, _ := sentinel.Master(ctx, sentinelName).Result()
				if !strings.Contains(masterInfo["flags"], "failover") {
					fmt.Println("Failover successful, new master:", masterInfo["ip"]+":"+masterInfo["port"])
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			Eventually(func() int32 {
				return failoverCloseCount.Load()
			}, "5s", "100ms").Should(Equal(int32(1)),
				"Expected exactly 1 CloseReasonFailover metric")

			multi, err := client.Do(ctx, "MULTI").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(multi).To(Equal("OK"))

			incr, err := client.Do(ctx, "INCR", "failover").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(incr).To(Equal("QUEUED"))

			execResult, err := client.Do(ctx, "EXEC").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(execResult).To(Equal([]any{int64(101)}))
		})
	}

	Context("with MaxActiveConns=0", func() {
		testFailoverWithMaxActiveConns(0)
	})

	Context("with MaxActiveConns=1", func() {
		testFailoverWithMaxActiveConns(1)
	})

	Context("with MaxActiveConns=2", func() {
		testFailoverWithMaxActiveConns(2)
	})

	AfterAll(func() {
		fmt.Println("Waiting for sentinel cluster to stabilize after all failover tests...")
		waitForSentinelClusterStable()
		fmt.Println("Sentinel cluster is now stable")
	})
})

// TestSentinelFailover_ReplicaAddrs_NoReplicas verifies that when a
// sentinel-monitored master has zero replicas, the sentinel connection
// is preserved and not torn down. Before the fix, closeSentinel() was
// called when getReplicaAddrs returned an empty list without error,
// causing a continuous rediscovery loop on every subsequent operation.
func TestSentinelFailover_ReplicaAddrs_NoReplicas(t *testing.T) {
	ctx := context.Background()

	// Skip if sentinel infrastructure is not available.
	sentinel0 := redis.NewSentinelClient(&redis.Options{Addr: sentinelAddrs[0], DialTimeout: 2 * time.Second})
	if err := sentinel0.Ping(ctx).Err(); err != nil {
		sentinel0.Close()
		t.Skipf("sentinel not available at %s: %v", sentinelAddrs[0], err)
	}
	sentinel0.Close()

	// Register a temporary master-only name pointing at the standalone Redis
	// instance (redisPort). Unlike sentinelMasterPort which already has two
	// replicas configured, the standalone instance has none, so Sentinel will
	// never discover replicas via INFO REPLICATION.
	masterOnlyName := "master-only-test"
	masterIP := "127.0.0.1"
	masterPort := redisPort
	quorum := "2"

	// Monitor the master-only name on all sentinels.
	for _, addr := range sentinelAddrs {
		sentinel := redis.NewSentinelClient(&redis.Options{Addr: addr})
		err := sentinel.Monitor(ctx, masterOnlyName, masterIP, masterPort, quorum).Err()
		if err != nil && !strings.Contains(err.Error(), "Duplicated") {
			t.Fatalf("SENTINEL MONITOR on %s failed: %v", addr, err)
		}
		sentinel.Close()
	}

	// Clean up: remove the monitored master after the test.
	defer func() {
		for _, addr := range sentinelAddrs {
			sentinel := redis.NewSentinelClient(&redis.Options{Addr: addr})
			_ = sentinel.Remove(ctx, masterOnlyName).Err()
			sentinel.Close()
		}
	}()

	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    masterOnlyName,
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   5 * time.Second,
		ReadTimeout:   5 * time.Second,
	}, sentinelAddrs)
	defer failover.Close()

	// Bootstrap: perform initial master discovery so that a sentinel
	// client is established (setSentinel is called internally).
	addr, err := failover.MasterAddr(ctx)
	if err != nil {
		t.Fatalf("MasterAddr failed: %v", err)
	}
	if addr == "" {
		t.Fatal("MasterAddr returned empty address")
	}

	if !failover.HasSentinel() {
		t.Fatal("expected sentinel to be set after MasterAddr, got nil")
	}

	// Call ReplicaAddrs. With zero replicas, getReplicaAddrs returns an
	// empty list without error. Before the fix, this called closeSentinel()
	// which destroyed the sentinel connection and caused a rediscovery loop.
	replicas, err := failover.ReplicaAddrs(ctx)
	if err != nil {
		t.Fatalf("ReplicaAddrs failed: %v", err)
	}
	if len(replicas) != 0 {
		t.Fatalf("expected zero replicas for master-only setup, got %v", replicas)
	}

	if !failover.HasSentinel() {
		t.Fatal("sentinel was closed after ReplicaAddrs returned zero replicas; " +
			"this causes a continuous rediscovery loop on master-only setups")
	}

	// Call ReplicaAddrs a second time to confirm the sentinel stays
	// alive across repeated calls (no degradation over time).
	_, err = failover.ReplicaAddrs(ctx)
	if err != nil {
		t.Fatalf("second ReplicaAddrs call failed: %v", err)
	}

	if !failover.HasSentinel() {
		t.Fatal("sentinel was closed after second ReplicaAddrs call")
	}
}

// TestSentinelFailover_ClosedDoesNotRebuildSentinel pins the closed guard: after
// Close, MasterAddr and RandomReplicaAddr must return ErrClosed WITHOUT running
// the sentinel discovery fan-out (which would dial and rebuild the sentinel
// client + pubsub). Without the guard the fan-out runs against the unreachable
// address and returns an "all sentinels ... unreachable" error instead — so the
// error value is the oracle. No sentinel infrastructure is needed.
//
// This is the failover leg of the autopipeliner shared-pool close: the failover
// Close hook runs before a pool-sharing wrapper's drain hook, and that drain may
// dial through masterReplicaDialer; the guard stops the dial from resurrecting
// sentinel resources after their only cleanup has run.
func TestSentinelFailover_ClosedDoesNotRebuildSentinel(t *testing.T) {
	ctx := context.Background()
	// Port 1 is reserved/unlistened: a connection attempt fails immediately, so a
	// fan-out that wrongly runs fails fast rather than hanging the test.
	unreachable := []string{"127.0.0.1:1"}

	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "closed-guard-test",
		SentinelAddrs: unreachable,
		DialTimeout:   500 * time.Millisecond,
	}, unreachable)

	if err := failover.Close(); err != nil {
		t.Fatalf("Close with no sentinel: %v", err)
	}

	if _, err := failover.MasterAddr(ctx); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("MasterAddr after Close: want ErrClosed, got %v", err)
	}
	if _, err := failover.RandomReplicaAddr(ctx); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("RandomReplicaAddr after Close: want ErrClosed, got %v", err)
	}
	if failover.HasSentinel() {
		t.Fatal("sentinel client was rebuilt after Close")
	}
	// Close must stay idempotent once closed.
	if err := failover.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func readMockRESPCmd(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, fmt.Errorf("expected bulk header, got %q", hdr)
		}
		length, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

type testMockSentinelServer struct {
	ln     net.Listener
	closed chan struct{}
}

func newTestMockSentinelServer(t *testing.T, masterAddr string) *testMockSentinelServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	s := &testMockSentinelServer{
		ln:     ln,
		closed: make(chan struct{}),
	}
	go s.serve(masterAddr)
	return s
}

func (s *testMockSentinelServer) Addr() string {
	return s.ln.Addr().String()
}

func (s *testMockSentinelServer) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return s.ln.Close()
}

func (s *testMockSentinelServer) serve(masterAddr string) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				select {
				case <-s.closed:
					return
				default:
				}
				cmd, err := readMockRESPCmd(r)
				if err != nil {
					return
				}
				if len(cmd) == 0 {
					continue
				}
				name := strings.ToLower(cmd[0])
				switch name {
				case "hello":
					_, _ = io.WriteString(c, "-ERR unknown command 'HELLO'\r\n")
				case "client":
					_, _ = io.WriteString(c, "+OK\r\n")
				case "ping":
					_, _ = io.WriteString(c, "+PONG\r\n")
				case "subscribe":
					for i := 1; i < len(cmd); i++ {
						ch := cmd[i]
						_, _ = fmt.Fprintf(c, "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(ch), ch, i)
					}
				case "sentinel":
					subcmd := ""
					if len(cmd) > 1 {
						subcmd = strings.ToLower(cmd[1])
					}
					switch subcmd {
					case "get-master-addr-by-name":
						host, port, _ := net.SplitHostPort(masterAddr)
						_, _ = fmt.Fprintf(c, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
					case "sentinels":
						_, _ = io.WriteString(c, "*0\r\n")
					case "replicas":
						_, _ = io.WriteString(c, "*1\r\n*4\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n$4\r\n6380\r\n")
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				default:
					_, _ = io.WriteString(c, "+OK\r\n")
				}
			}
		}(conn)
	}
}

func newTestSilentSentinelServer(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln
}

// TestSentinelFailover_MasterAddr_QueriesNextSentinelOnTimeout tests that when
// the currently cached sentinel becomes unresponsive (drops packets / times out),
// MasterAddr discards the failed sentinel and queries the next available sentinel.
func TestSentinelFailover_MasterAddr_QueriesNextSentinelOnTimeout(t *testing.T) {
	ctx := context.Background()

	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	// Initial discovery: silentLn is first in list but times out; healthyServer responds.
	addr, err := failover.MasterAddr(ctx)
	if err != nil {
		t.Fatalf("initial MasterAddr failed: %v", err)
	}
	if addr != "127.0.0.1:6379" {
		t.Fatalf("want 127.0.0.1:6379, got %s", addr)
	}

	// Verify working sentinel was selected.
	if !failover.HasSentinel() {
		t.Fatal("expected cached sentinel to be set")
	}

	// Now simulate the cached sentinel failing while a second healthy sentinel is available.
	healthyServer2 := newTestMockSentinelServer(t, "127.0.0.1:6380")
	defer healthyServer2.Close()

	// Close the currently selected sentinel to make it fail.
	healthyServer.Close()

	// Update sentinel address list to include healthyServer2.
	newAddrs := []string{healthyServer.Addr(), healthyServer2.Addr()}
	failover2 := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: newAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, newAddrs)
	defer failover2.Close()

	// Manually attach a sentinel pointing to the stopped server.
	deadCli := redis.NewSentinelClient(&redis.Options{
		Addr:        healthyServer.Addr(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	failover2.SetSentinel(deadCli)

	// MasterAddr should detect dead sentinel, close it, rotate addresses, and query healthyServer2.
	addr2, err := failover2.MasterAddr(ctx)
	if err != nil {
		t.Fatalf("failover MasterAddr failed: %v", err)
	}
	if addr2 != "127.0.0.1:6380" {
		t.Fatalf("want 127.0.0.1:6380, got %s", addr2)
	}
	if !failover2.HasSentinel() {
		t.Fatal("expected new cached sentinel to be set")
	}
}

// TestSentinelFailover_MasterAddr_ContextDeadlineExceededClearsCachedSentinel
// tests issue #4065: when dialing/querying the cached sentinel exceeds the caller's
// context deadline, the client closes and clears the cached sentinel rather than
// remaining stalled on the failed address for subsequent calls.
func TestSentinelFailover_MasterAddr_ContextDeadlineExceededClearsCachedSentinel(t *testing.T) {
	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   150 * time.Millisecond,
		ReadTimeout:   150 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	// Pre-set the cached sentinel to the silent server (simulating connection drops after establishment).
	silentCli := redis.NewSentinelClient(&redis.Options{
		Addr:        silentLn.Addr().String(),
		DialTimeout: 150 * time.Millisecond,
		ReadTimeout: 150 * time.Millisecond,
	})
	failover.SetSentinel(silentCli)

	// Call MasterAddr with a context matching DialTimeout.
	callCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := failover.MasterAddr(callCtx)
	if err == nil {
		t.Fatal("expected error on silent sentinel with expired context, got nil")
	}

	// The cached sentinel MUST be closed and cleared despite context deadline expiration.
	if failover.HasSentinel() {
		t.Fatal("expected cached sentinel to be cleared after query failure")
	}

	// The failed sentinel address must be rotated out of index 0.
	addrs := failover.SentinelAddrs()
	if len(addrs) > 0 && addrs[0] == silentLn.Addr().String() {
		t.Fatalf("expected failed sentinel to be rotated from index 0, got %v", addrs)
	}

	// Subsequent call with fresh context should query the healthy sentinel and succeed.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer freshCancel()

	masterAddr, err := failover.MasterAddr(freshCtx)
	if err != nil {
		t.Fatalf("MasterAddr with fresh context failed: %v", err)
	}
	if masterAddr != "127.0.0.1:6379" {
		t.Fatalf("want 127.0.0.1:6379, got %s", masterAddr)
	}
	if !failover.HasSentinel() {
		t.Fatal("expected healthy sentinel to be established as cached sentinel")
	}
}

// TestSentinelFailover_ReplicaAddrs_QueriesNextSentinelOnTimeout tests that
// replica discovery iterates past an unresponsive sentinel to find replicas on
// the next available sentinel address.
func TestSentinelFailover_ReplicaAddrs_QueriesNextSentinelOnTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	replicas, err := failover.ReplicaAddrs(ctx)
	if err != nil {
		t.Fatalf("ReplicaAddrs failed: %v", err)
	}
	if len(replicas) != 1 || replicas[0] != "127.0.0.1:6380" {
		t.Fatalf("want [127.0.0.1:6380], got %v", replicas)
	}
	if !failover.HasSentinel() {
		t.Fatal("expected healthy sentinel to be cached after replica discovery")
	}
}

// TestSentinelFailover_RotateSentinelAddr_SliceIntegrity tests that rotateSentinelAddr
// rotates addresses correctly without dropping elements or creating duplicates.
func TestSentinelFailover_RotateSentinelAddr_SliceIntegrity(t *testing.T) {
	t.Run("rotates head element to tail", func(t *testing.T) {
		initial := []string{"10.0.0.1:26379", "10.0.0.2:26379", "10.0.0.3:26379"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		failover.RotateSentinelAddr("10.0.0.1:26379")
		got := failover.SentinelAddrs()
		want := []string{"10.0.0.2:26379", "10.0.0.3:26379", "10.0.0.1:26379"}
		if !slices.Equal(got, want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("rotates middle element to tail", func(t *testing.T) {
		initial := []string{"10.0.0.1:26379", "10.0.0.2:26379", "10.0.0.3:26379", "10.0.0.4:26379"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		failover.RotateSentinelAddr("10.0.0.2:26379")
		got := failover.SentinelAddrs()
		want := []string{"10.0.0.1:26379", "10.0.0.3:26379", "10.0.0.4:26379", "10.0.0.2:26379"}
		if !slices.Equal(got, want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("rotates tail element to tail (ordering preserved)", func(t *testing.T) {
		initial := []string{"10.0.0.1:26379", "10.0.0.2:26379", "10.0.0.3:26379"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		failover.RotateSentinelAddr("10.0.0.3:26379")
		got := failover.SentinelAddrs()
		want := []string{"10.0.0.1:26379", "10.0.0.2:26379", "10.0.0.3:26379"}
		if !slices.Equal(got, want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("rotates unknown element (falls back to index 0)", func(t *testing.T) {
		initial := []string{"10.0.0.1:26379", "10.0.0.2:26379", "10.0.0.3:26379"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		failover.RotateSentinelAddr("unknown:26379")
		got := failover.SentinelAddrs()
		want := []string{"10.0.0.2:26379", "10.0.0.3:26379", "10.0.0.1:26379"}
		if !slices.Equal(got, want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("single element slice is unchanged", func(t *testing.T) {
		initial := []string{"10.0.0.1:26379"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		failover.RotateSentinelAddr("10.0.0.1:26379")
		got := failover.SentinelAddrs()
		want := []string{"10.0.0.1:26379"}
		if !slices.Equal(got, want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("repeated full rotation cycles preserve all elements with zero drops or duplicates", func(t *testing.T) {
		initial := []string{"s1", "s2", "s3", "s4", "s5"}
		failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: initial,
		}, initial)
		defer failover.Close()

		for cycle := 0; cycle < 20; cycle++ {
			for _, target := range initial {
				failover.RotateSentinelAddr(target)
				current := failover.SentinelAddrs()
				if len(current) != len(initial) {
					t.Fatalf("length changed: want %d, got %d", len(initial), len(current))
				}
				seen := make(map[string]int)
				for _, addr := range current {
					seen[addr]++
				}
				for _, addr := range initial {
					if seen[addr] != 1 {
						t.Fatalf("element %q count is %d (expected 1) in %v", addr, seen[addr], current)
					}
				}
			}
		}
	})
}

// TestSentinelFailover_MasterAddr_ExpiredContextDoesNotTearDownReplacementSentinel
// tests that when a caller's query fails due to an expired context while another
// caller has installed a healthy replacement sentinel, the replacement sentinel
// is not closed or rotated.
func TestSentinelFailover_MasterAddr_ExpiredContextDoesNotTearDownReplacementSentinel(t *testing.T) {
	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	// 1. Point cached sentinel to the dead/silent sentinel.
	deadCli := redis.NewSentinelClient(&redis.Options{
		Addr:        silentLn.Addr().String(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	failover.SetSentinel(deadCli)

	// 2. Replacement healthy sentinel client.
	healthyCli := redis.NewSentinelClient(&redis.Options{
		Addr:        healthyServer.Addr(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})

	// 3. Caller 1 calls MasterAddr with a short context that will expire while querying deadCli.
	queryCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	callerDone := make(chan error, 1)
	go func() {
		_, err := failover.MasterAddr(queryCtx)
		callerDone <- err
	}()

	// 4. Give Caller 1 enough time to start querying deadCli under read lock.
	time.Sleep(20 * time.Millisecond)

	// 5. Concurrently, another caller discovers and installs the healthy replacement sentinel.
	failover.SetSentinel(healthyCli)

	// 6. Wait for Caller 1 to finish (fails due to queryCtx deadline expiration).
	err := <-callerDone
	if err == nil {
		t.Fatal("expected error with expired context, got nil")
	}

	// 7. The healthy replacement sentinel MUST NOT have been torn down.
	if !failover.HasSentinel() {
		t.Fatal("expected replacement sentinel to remain cached")
	}
	if failover.Sentinel() != healthyCli {
		t.Fatal("expected replacement sentinel client pointer to be preserved")
	}

	// 8. Healthy caller should immediately succeed using the cached replacement sentinel.
	freshCtx := context.Background()
	masterAddr, err := failover.MasterAddr(freshCtx)
	if err != nil {
		t.Fatalf("expected fresh MasterAddr to succeed, got %v", err)
	}
	if masterAddr != "127.0.0.1:6379" {
		t.Fatalf("want 127.0.0.1:6379, got %s", masterAddr)
	}
}

// TestSentinelFailover_ReplicaAddrs_ExpiredContextDoesNotTearDownReplacementSentinel
// tests that replicaAddrs does not tear down a newly installed replacement sentinel
// when invoked with an expired context.
func TestSentinelFailover_ReplicaAddrs_ExpiredContextDoesNotTearDownReplacementSentinel(t *testing.T) {
	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	// 1. Point cached sentinel to the dead/silent sentinel.
	deadCli := redis.NewSentinelClient(&redis.Options{
		Addr:        silentLn.Addr().String(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	failover.SetSentinel(deadCli)

	// 2. Replacement healthy sentinel client.
	healthyCli := redis.NewSentinelClient(&redis.Options{
		Addr:        healthyServer.Addr(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})

	// 3. Caller 1 invokes ReplicaAddrs with a short context that expires while querying deadCli.
	queryCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	callerDone := make(chan error, 1)
	go func() {
		_, err := failover.ReplicaAddrs(queryCtx)
		callerDone <- err
	}()

	// 4. Give Caller 1 enough time to start querying deadCli under read lock.
	time.Sleep(20 * time.Millisecond)

	// 5. Concurrently, another caller installs the healthy replacement sentinel.
	failover.SetSentinel(healthyCli)

	// 6. Wait for Caller 1 to finish.
	err := <-callerDone
	if err == nil {
		t.Fatal("expected error with expired context, got nil")
	}

	// 7. The healthy replacement sentinel MUST NOT have been torn down.
	if !failover.HasSentinel() {
		t.Fatal("expected replacement sentinel to remain cached")
	}
	if failover.Sentinel() != healthyCli {
		t.Fatal("expected replacement sentinel pointer to be preserved")
	}

	// 8. Valid caller succeeds with cached sentinel.
	replicas, err := failover.ReplicaAddrs(context.Background())
	if err != nil {
		t.Fatalf("expected ReplicaAddrs to succeed, got %v", err)
	}
	if len(replicas) != 1 || replicas[0] != "127.0.0.1:6380" {
		t.Fatalf("want [127.0.0.1:6380], got %v", replicas)
	}
}

// TestSentinelFailover_ConcurrentFailover_MixedContexts tests concurrent queries
// during failover where multiple callers have expired contexts and others have valid contexts.
// The healthy replacement sentinel must survive and remain cached.
func TestSentinelFailover_ConcurrentFailover_MixedContexts(t *testing.T) {
	silentLn := newTestSilentSentinelServer(t)
	defer silentLn.Close()

	healthyServer := newTestMockSentinelServer(t, "127.0.0.1:6379")
	defer healthyServer.Close()

	sentinelAddrs := []string{silentLn.Addr().String(), healthyServer.Addr()}
	failover := redis.NewTestSentinelFailover(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: sentinelAddrs,
		DialTimeout:   100 * time.Millisecond,
		ReadTimeout:   100 * time.Millisecond,
	}, sentinelAddrs)
	defer failover.Close()

	// Cached sentinel points to dead silent server initially.
	deadCli := redis.NewSentinelClient(&redis.Options{
		Addr:        silentLn.Addr().String(),
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	failover.SetSentinel(deadCli)

	var wg sync.WaitGroup
	numWorkers := 20

	// Half workers have very short/expired contexts, half have long contexts.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var ctx context.Context
			var cancel context.CancelFunc
			if workerID%2 == 0 {
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			}
			defer cancel()

			addr, err := failover.MasterAddr(ctx)
			if err == nil && addr != "127.0.0.1:6379" {
				t.Errorf("worker %d: unexpected master addr: %s", workerID, addr)
			}
		}(i)
	}

	wg.Wait()

	// After all concurrent callers finish, failover must have settled on the healthy sentinel.
	if !failover.HasSentinel() {
		t.Fatal("expected healthy sentinel to remain cached after concurrent storm")
	}

	freshAddr, err := failover.MasterAddr(context.Background())
	if err != nil {
		t.Fatalf("expected MasterAddr to succeed after failover, got %v", err)
	}
	if freshAddr != "127.0.0.1:6379" {
		t.Fatalf("want 127.0.0.1:6379, got %s", freshAddr)
	}
}
