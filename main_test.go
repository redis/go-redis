package redis_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

const (
	sentinelName        = "go-redis-test"
	secondaryPortOffset = 2
	sentinelOffset      = 100
	clusterOffset       = 200
	ringOffset          = 300
)

type redisVersions int

const (
	badRedisVersion = iota
	redis60         = 60
	redis70         = 70
	redis72         = 72
)

var (
	redisVersion                                   redisVersions
	redisPort                                      int = 6379
	redisSecondaryPort                             int
	redisAddr                                      string
	ringShard1Port, ringShard2Port, ringShard3Port int
	ringShard1, ringShard2                         *redis.Client
	testFilter                                     = "json"
)

var _ = BeforeSuite(func() {

	var err error

	port := os.Getenv("REDIS_PORT")
	if port != "" {
		redisPort, err = strconv.Atoi(port)
		Expect(err).NotTo(HaveOccurred())
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr != "" {
		redisAddr = addr
	} else {
		redisAddr = "127.0.0.1"
	}

	version := os.Getenv("REDIS_VERSION")
	if version != "" {
		v, err := strconv.Atoi(version)
		Expect(err).NotTo(HaveOccurred())
		switch v {
		case redis60, redis70, redis72:
			redisVersion = redisVersions(v)
		default:
			Expect(fmt.Errorf("%d is not a supported redis version for testing", v)).NotTo(HaveOccurred())
		}
	}

	/*
	   redisSecondaryPort = redisPort + secondaryPortOffset
	   ringShard1Port = redisPort + ringOffset
	   ringShard2Port = ringShard1Port + 1
	   ringShard3Port = ringShard1Port + 2

	   ringShard1, err = connectTo(ringShard1Port)
	   Expect(err).NotTo(HaveOccurred())
	   ringShard2, err = connectTo(ringShard2Port)
	   Expect(err).NotTo(HaveOccurred())
	*/
})

func TestGinkgoSuite(t *testing.T) {

	if os.Getenv("LABEL_FILTER") != "" {
		testFilter = os.Getenv("LABEL_FILTER")
	}

	suiteConfig, _ := GinkgoConfiguration()
	if testFilter != "" {
		suiteConfig.LabelFilter = testFilter
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "go-redis", suiteConfig)
}

//------------------------------------------------------------------------------

func redisOptions() *redis.Options {
	return &redis.Options{
		Addr: fmt.Sprintf("%s:%d", redisAddr, redisPort),

		DialTimeout:           10 * time.Second,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		ContextTimeoutEnabled: true,

		MaxRetries: -1,

		PoolSize:        10,
		PoolTimeout:     30 * time.Second,
		ConnMaxIdleTime: time.Minute,
	}
}

func redisClusterOptions() *redis.ClusterOptions {
	return &redis.ClusterOptions{
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRedirects: 8,

		PoolSize:        10,
		PoolTimeout:     30 * time.Second,
		ConnMaxIdleTime: time.Minute,
	}
}

func redisRingOptions() *redis.RingOptions {
	return &redis.RingOptions{
		Addrs: map[string]string{
			"ringShardOne": fmt.Sprintf(":%d", ringShard1Port),
			"ringShardTwo": fmt.Sprintf(":%d", ringShard2Port),
		},

		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRetries: -1,

		PoolSize:        10,
		PoolTimeout:     30 * time.Second,
		ConnMaxIdleTime: time.Minute,
	}
}

func performAsync(n int, cbs ...func(int)) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	return &wg
}

func perform(n int, cbs ...func(int)) {
	wg := performAsync(n, cbs...)
	wg.Wait()
}

func eventually(fn func() error, timeout time.Duration) error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	exit := make(chan struct{})

	go func() {
		for {
			err := fn()
			if err == nil {
				close(done)
				return
			}

			select {
			case errCh <- err:
			default:
			}

			select {
			case <-exit:
				return
			case <-time.After(timeout / 100):
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		close(exit)
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("timeout after %s without an error", timeout)
		}
	}
}

func execCmd(name string, args ...string) (*os.Process, error) {
	cmd := exec.Command(name, args...)
	if testing.Verbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	return cmd.Process, cmd.Start()
}

func connectTo(port int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:       fmt.Sprintf("%s:%d", redisAddr, port),
		MaxRetries: -1,
	})

	err := eventually(func() error {
		return client.Ping(ctx).Err()
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return client, nil
}

//------------------------------------------------------------------------------

type badConnError string

func (e badConnError) Error() string   { return string(e) }
func (e badConnError) Timeout() bool   { return true }
func (e badConnError) Temporary() bool { return false }

type badConn struct {
	net.TCPConn

	readDelay, writeDelay time.Duration
	readErr, writeErr     error
}

var _ net.Conn = &badConn{}

func (cn *badConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (cn *badConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (cn *badConn) Read([]byte) (int, error) {
	if cn.readDelay != 0 {
		time.Sleep(cn.readDelay)
	}
	if cn.readErr != nil {
		return 0, cn.readErr
	}
	return 0, badConnError("bad connection")
}

func (cn *badConn) Write([]byte) (int, error) {
	if cn.writeDelay != 0 {
		time.Sleep(cn.writeDelay)
	}
	if cn.writeErr != nil {
		return 0, cn.writeErr
	}
	return 0, badConnError("bad connection")
}

//------------------------------------------------------------------------------

type hook struct {
	dialHook            func(hook redis.DialHook) redis.DialHook
	processHook         func(hook redis.ProcessHook) redis.ProcessHook
	processPipelineHook func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook
}

func (h *hook) DialHook(hook redis.DialHook) redis.DialHook {
	if h.dialHook != nil {
		return h.dialHook(hook)
	}
	return hook
}

func (h *hook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	if h.processHook != nil {
		return h.processHook(hook)
	}
	return hook
}

func (h *hook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	if h.processPipelineHook != nil {
		return h.processPipelineHook(hook)
	}
	return hook
}
