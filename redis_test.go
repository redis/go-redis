package redis_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/redis.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const redisAddr = ":6379"

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gopkg.in/redis.v2")
}

var _ = Describe("Client", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		client.Close()
	})

	It("should ping", func() {
		val, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))
	})

	It("should support custom dialers", func() {
		custom := redis.NewClient(&redis.Options{
			Dialer: func() (net.Conn, error) {
				return net.Dial("tcp", redisAddr)
			},
		})

		val, err := custom.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))
		Expect(custom.Close()).NotTo(HaveOccurred())
	})

	It("should close", func() {
		Expect(client.Close()).NotTo(HaveOccurred())
		err := client.Ping().Err()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
	})

	It("should close pubsub without closing the connection", func() {
		pubsub := client.PubSub()
		Expect(pubsub.Close()).NotTo(HaveOccurred())

		_, err := pubsub.Receive()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close multi without closing the connection", func() {
		multi := client.Multi()
		Expect(multi.Close()).NotTo(HaveOccurred())

		_, err := multi.Exec(func() error {
			multi.Ping()
			return nil
		})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close pipeline without closing the connection", func() {
		pipeline := client.Pipeline()
		Expect(pipeline.Close()).NotTo(HaveOccurred())

		pipeline.Ping()
		_, err := pipeline.Exec()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should support idle-timeouts", func() {
		idle := redis.NewTCPClient(&redis.Options{
			Addr:        redisAddr,
			IdleTimeout: 100 * time.Microsecond,
		})
		defer idle.Close()

		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond)
		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should support DB selection", func() {
		db1 := redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
			DB:   1,
		})
		defer db1.Close()

		Expect(db1.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Set("key", "value", 0).Err()).NotTo(HaveOccurred())

		Expect(client.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Get("key").Val()).To(Equal("value"))
		Expect(db1.FlushDb().Err()).NotTo(HaveOccurred())
	})

})

//------------------------------------------------------------------------------

func BenchmarkRedisPing(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Ping().Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisSet(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", "hello", 0).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisGetNil(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()
	if err := client.FlushDb().Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Get("key").Err(); err != redis.Nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisGet(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()
	if err := client.Set("key", "hello", 0).Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Get("key").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisMGet(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()
	if err := client.MSet("key1", "hello1", "key2", "hello2").Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.MGet("key1", "key2").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetExpire(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", "hello", 0).Err(); err != nil {
				b.Fatal(err)
			}
			if err := client.Expire("key", time.Second).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPipeline(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Set("key", "hello", 0)
				pipe.Expire("key", time.Second)
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

//------------------------------------------------------------------------------

// Replaces ginkgo's Eventually.
func waitForSubstring(fn func() string, substr string, timeout time.Duration) error {
	var s string

	found := make(chan struct{})
	var exit int32
	go func() {
		for atomic.LoadInt32(&exit) == 0 {
			s = fn()
			if strings.Contains(s, substr) {
				found <- struct{}{}
				return
			}
			time.Sleep(timeout / 100)
		}
	}()

	select {
	case <-found:
		return nil
	case <-time.After(timeout):
		atomic.StoreInt32(&exit, 1)
	}
	return fmt.Errorf("%q does not contain %q", s, substr)
}

func execCmd(name string, args ...string) (*os.Process, error) {
	cmd := exec.Command(name, args...)
	if testing.Verbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	return cmd.Process, cmd.Start()
}

func connectTo(port string) (client *redis.Client, err error) {
	client = redis.NewClient(&redis.Options{
		Addr: ":" + port,
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err = client.Ping().Err(); err == nil {
			return client, nil
		}
		time.Sleep(250 * time.Millisecond)
	}

	return nil, err
}

type redisProcess struct {
	*os.Process
	*redis.Client
}

func (p *redisProcess) Close() error {
	p.Client.Close()
	return p.Kill()
}

var (
	redisServerBin, _  = filepath.Abs(filepath.Join(".test", "redis", "src", "redis-server"))
	redisServerConf, _ = filepath.Abs(filepath.Join(".test", "redis.conf"))
)

func redisDir(port string) (string, error) {
	dir, err := filepath.Abs(filepath.Join(".test", "instances", port))
	if err != nil {
		return "", err
	} else if err = os.RemoveAll(dir); err != nil {
		return "", err
	} else if err = os.MkdirAll(dir, 0775); err != nil {
		return "", err
	}
	return dir, nil
}

func startRedis(port string, args ...string) (*redisProcess, error) {
	dir, err := redisDir(port)
	if err != nil {
		return nil, err
	}
	if err = exec.Command("cp", "-f", redisServerConf, dir).Run(); err != nil {
		return nil, err
	}

	baseArgs := []string{filepath.Join(dir, "redis.conf"), "--port", port, "--dir", dir}
	process, err := execCmd(redisServerBin, append(baseArgs, args...)...)
	if err != nil {
		return nil, err
	}

	client, err := connectTo(port)
	if err != nil {
		process.Kill()
		return nil, err
	}
	return &redisProcess{process, client}, err
}

func startSentinel(port, masterName, masterPort string) (*redisProcess, error) {
	dir, err := redisDir(port)
	if err != nil {
		return nil, err
	}
	process, err := execCmd(redisServerBin, os.DevNull, "--sentinel", "--port", port, "--dir", dir)
	if err != nil {
		return nil, err
	}
	client, err := connectTo(port)
	if err != nil {
		process.Kill()
		return nil, err
	}
	for _, cmd := range []*redis.StatusCmd{
		redis.NewStatusCmd("SENTINEL", "MONITOR", masterName, "127.0.0.1", masterPort, "1"),
		redis.NewStatusCmd("SENTINEL", "SET", masterName, "down-after-milliseconds", "500"),
		redis.NewStatusCmd("SENTINEL", "SET", masterName, "failover-timeout", "1000"),
		redis.NewStatusCmd("SENTINEL", "SET", masterName, "parallel-syncs", "1"),
	} {
		client.Process(cmd)
		if err := cmd.Err(); err != nil {
			process.Kill()
			return nil, err
		}
	}
	return &redisProcess{process, client}, nil
}
