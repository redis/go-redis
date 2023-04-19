package redis_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

const (
	redisSecondaryPort = "6381"
)

const (
	ringShard1Port = "6390"
	ringShard2Port = "6391"
	ringShard3Port = "6392"
)

const (
	sentinelName       = "mymaster"
	sentinelMasterPort = "9123"
	sentinelSlave1Port = "9124"
	sentinelSlave2Port = "9125"
	sentinelPort1      = "9126"
	sentinelPort2      = "9127"
	sentinelPort3      = "9128"
)

var redisPort = "6380"
var redisAddr = ":" + redisPort

var (
	sentinelAddrs = []string{":" + sentinelPort1, ":" + sentinelPort2, ":" + sentinelPort3}

	processes map[string]*redisProcess

	redisMain                                      *redisProcess
	ringShard1, ringShard2, ringShard3             *redisProcess
	sentinelMaster, sentinelSlave1, sentinelSlave2 *redisProcess
	sentinel1, sentinel2, sentinel3                *redisProcess
)

var cluster = &clusterScenario{
	ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
	nodeIDs:   make([]string, 6),
	processes: make(map[string]*redisProcess, 6),
	clients:   make(map[string]*redis.Client, 6),
}

func registerProcess(port string, p *redisProcess) {
	if processes == nil {
		processes = make(map[string]*redisProcess)
	}
	processes[port] = p
}

var _ = BeforeSuite(func() {
	addr := os.Getenv("REDIS_PORT")
	if addr != "" {
		redisPort = addr
		redisAddr = ":" + redisPort
	}
	var err error

	redisMain, err = startRedis(redisPort)
	Expect(err).NotTo(HaveOccurred())

	ringShard1, err = startRedis(ringShard1Port)
	Expect(err).NotTo(HaveOccurred())

	ringShard2, err = startRedis(ringShard2Port)
	Expect(err).NotTo(HaveOccurred())

	ringShard3, err = startRedis(ringShard3Port)
	Expect(err).NotTo(HaveOccurred())

	sentinelMaster, err = startRedis(sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinel1, err = startSentinel(sentinelPort1, sentinelName, sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinel2, err = startSentinel(sentinelPort2, sentinelName, sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinel3, err = startSentinel(sentinelPort3, sentinelName, sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinelSlave1, err = startRedis(
		sentinelSlave1Port, "--slaveof", "127.0.0.1", sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinelSlave2, err = startRedis(
		sentinelSlave2Port, "--slaveof", "127.0.0.1", sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	Expect(startCluster(ctx, cluster)).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	Expect(cluster.Close()).NotTo(HaveOccurred())

	for _, p := range processes {
		Expect(p.Close()).NotTo(HaveOccurred())
	}
	processes = nil
})

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "go-redis")
}

//------------------------------------------------------------------------------

func redisOptions() *redis.Options {
	return &redis.Options{
		Addr: redisAddr,
		DB:   15,

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
			"ringShardOne": ":" + ringShard1Port,
			"ringShardTwo": ":" + ringShard2Port,
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

func connectTo(port string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:       ":" + port,
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

type redisProcess struct {
	*os.Process
	*redis.Client
}

func (p *redisProcess) Close() error {
	if err := p.Kill(); err != nil {
		return err
	}

	err := eventually(func() error {
		if err := p.Client.Ping(ctx).Err(); err != nil {
			return nil
		}
		return fmt.Errorf("client %s is not shutdown", p.Options().Addr)
	}, 10*time.Second)
	if err != nil {
		return err
	}

	p.Client.Close()
	return nil
}

var (
	redisServerBin, _    = filepath.Abs(filepath.Join("testdata", "redis", "src", "redis-server"))
	redisServerConf, _   = filepath.Abs(filepath.Join("testdata", "redis", "redis.conf"))
	redisSentinelConf, _ = filepath.Abs(filepath.Join("testdata", "redis", "sentinel.conf"))
)

func redisDir(port string) (string, error) {
	dir, err := filepath.Abs(filepath.Join("testdata", "instances", port))
	if err != nil {
		return "", err
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return "", err
	}
	return dir, nil
}

func startRedis(port string, args ...string) (*redisProcess, error) {
	dir, err := redisDir(port)
	if err != nil {
		return nil, err
	}

	if err := exec.Command("cp", "-f", redisServerConf, dir).Run(); err != nil {
		return nil, err
	}

	baseArgs := []string{filepath.Join(dir, "redis.conf"), "--port", port, "--dir", dir, "--enable-module-command", "yes"}
	process, err := execCmd(redisServerBin, append(baseArgs, args...)...)
	if err != nil {
		return nil, err
	}

	client, err := connectTo(port)
	if err != nil {
		process.Kill()
		return nil, err
	}

	p := &redisProcess{process, client}
	registerProcess(port, p)
	return p, nil
}

func startSentinel(port, masterName, masterPort string) (*redisProcess, error) {
	dir, err := redisDir(port)
	if err != nil {
		return nil, err
	}

	sentinelConf := filepath.Join(dir, "sentinel.conf")
	if err := os.WriteFile(sentinelConf, nil, 0o644); err != nil {
		return nil, err
	}

	process, err := execCmd(redisServerBin, sentinelConf, "--sentinel", "--port", port, "--dir", dir)
	if err != nil {
		return nil, err
	}

	client, err := connectTo(port)
	if err != nil {
		process.Kill()
		return nil, err
	}

	// set down-after-milliseconds=2000
	// link: https://github.com/redis/redis/issues/8607
	for _, cmd := range []*redis.StatusCmd{
		redis.NewStatusCmd(ctx, "SENTINEL", "MONITOR", masterName, "127.0.0.1", masterPort, "2"),
		redis.NewStatusCmd(ctx, "SENTINEL", "SET", masterName, "down-after-milliseconds", "2000"),
		redis.NewStatusCmd(ctx, "SENTINEL", "SET", masterName, "failover-timeout", "1000"),
		redis.NewStatusCmd(ctx, "SENTINEL", "SET", masterName, "parallel-syncs", "1"),
	} {
		client.Process(ctx, cmd)
		if err := cmd.Err(); err != nil {
			process.Kill()
			return nil, fmt.Errorf("%s failed: %w", cmd, err)
		}
	}

	p := &redisProcess{process, client}
	registerProcess(port, p)
	return p, nil
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
