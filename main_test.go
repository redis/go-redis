package redis_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
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
	sentinelName       = "go-redis-test"
	sentinelMasterPort = "9121"
	sentinelSlave1Port = "9122"
	sentinelSlave2Port = "9123"
	sentinelPort1      = "26379"
	sentinelPort2      = "26380"
	sentinelPort3      = "26381"
)

var (
	redisPort = "6380"
	redisAddr = ":" + redisPort
)

var (
	redisStackPort = "6379"
	redisStackAddr = ":" + redisStackPort
)

var (
	sentinelAddrs = []string{"127.0.0.1:" + sentinelPort1, "127.0.0.1:" + sentinelPort2, "127.0.0.1:" + sentinelPort3}

	ringShard1, ringShard2, ringShard3             *redis.Client
	sentinelMaster, sentinelSlave1, sentinelSlave2 *redis.Client
	sentinel1, sentinel2, sentinel3                *redis.Client
)

var cluster = &clusterScenario{
	ports:   []string{"16600", "16601", "16602", "16603", "16604", "16605"},
	nodeIDs: make([]string, 6),
	clients: make(map[string]*redis.Client, 6),
}

var tlsCluster = &clusterScenario{
	ports:   []string{"5430", "5431", "5432", "5433", "5434", "5435"},
	nodeIDs: make([]string, 6),
	clients: make(map[string]*redis.Client, 6),
}

// Redis Software Cluster
var RECluster = false

// Redis Community Edition Docker
var RCEDocker = false

// Notes version of redis we are executing tests against.
// This can be used before we change the bsm fork of ginkgo for one,
// which have support for label sets, so we can filter tests per redis version.
var RedisVersion float64 = 8.4

func SkipBeforeRedisVersion(version float64, msg string) {
	if RedisVersion < version {
		Skip(fmt.Sprintf("(redis version < %f) %s", version, msg))
	}
}

func SkipAfterRedisVersion(version float64, msg string) {
	if RedisVersion > version {
		Skip(fmt.Sprintf("(redis version > %f) %s", version, msg))
	}
}

var _ = BeforeSuite(func() {
	addr := os.Getenv("REDIS_PORT")
	if addr != "" {
		redisPort = addr
		redisAddr = ":" + redisPort
	}
	var err error
	RECluster, _ = strconv.ParseBool(os.Getenv("RE_CLUSTER"))
	RCEDocker, _ = strconv.ParseBool(os.Getenv("RCE_DOCKER"))

	RedisVersion, _ = strconv.ParseFloat(strings.Trim(os.Getenv("REDIS_VERSION"), "\""), 64)

	if RedisVersion == 0 {
		RedisVersion = 8.4
	}

	fmt.Printf("RECluster: %v\n", RECluster)
	fmt.Printf("RCEDocker: %v\n", RCEDocker)
	fmt.Printf("REDIS_VERSION: %.1f\n", RedisVersion)
	fmt.Printf("CLIENT_LIBS_TEST_IMAGE: %v\n", os.Getenv("CLIENT_LIBS_TEST_IMAGE"))
	logging.Disable()

	if RedisVersion < 7.0 || RedisVersion > 9 {
		panic("incorrect or not supported redis version")
	}

	redisPort = redisStackPort
	redisAddr = redisStackAddr
	if !RECluster {
		ringShard1, err = connectTo(ringShard1Port)
		Expect(err).NotTo(HaveOccurred())

		ringShard2, err = connectTo(ringShard2Port)
		Expect(err).NotTo(HaveOccurred())

		ringShard3, err = connectTo(ringShard3Port)
		Expect(err).NotTo(HaveOccurred())

		sentinelMaster, err = connectTo(sentinelMasterPort)
		Expect(err).NotTo(HaveOccurred())

		sentinel1, err = startSentinel(sentinelPort1, sentinelName, sentinelMasterPort)
		Expect(err).NotTo(HaveOccurred())

		sentinel2, err = startSentinel(sentinelPort2, sentinelName, sentinelMasterPort)
		Expect(err).NotTo(HaveOccurred())

		sentinel3, err = startSentinel(sentinelPort3, sentinelName, sentinelMasterPort)
		Expect(err).NotTo(HaveOccurred())

		sentinelSlave1, err = connectTo(sentinelSlave1Port)
		Expect(err).NotTo(HaveOccurred())

		err = sentinelSlave1.SlaveOf(ctx, "127.0.0.1", sentinelMasterPort).Err()
		Expect(err).NotTo(HaveOccurred())

		sentinelSlave2, err = connectTo(sentinelSlave2Port)
		Expect(err).NotTo(HaveOccurred())

		err = sentinelSlave2.SlaveOf(ctx, "127.0.0.1", sentinelMasterPort).Err()
		Expect(err).NotTo(HaveOccurred())

		// populate cluster node information
		Expect(configureClusterTopology(ctx, cluster)).NotTo(HaveOccurred())

		// Initialize TLS cluster if available
		_ = initializeTLSCluster(ctx)
	}
})

var _ = AfterSuite(func() {
	if !RECluster {
		Expect(cluster.Close()).NotTo(HaveOccurred())
		cleanupTLSCluster()
	}
})

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "go-redis")
}

//------------------------------------------------------------------------------

func redisOptions() *redis.Options {
	if RECluster {
		return &redis.Options{
			Addr: redisAddr,
			DB:   0,

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
	return &redis.Options{
		Addr: redisAddr,
		DB:   0,

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
		// start from 1, so we can skip db 0 where such test is executed with
		// select db command
		for i := 1; i <= n; i++ {
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

func connectTo(port string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:       "127.0.0.1:" + port,
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

func startSentinel(port, masterName, masterPort string) (*redis.Client, error) {
	client, err := connectTo(port)
	if err != nil {
		return nil, err
	}

	for _, cmd := range []*redis.StatusCmd{
		redis.NewStatusCmd(ctx, "SENTINEL", "MONITOR", masterName, "127.0.0.1", masterPort, "2"),
	} {
		client.Process(ctx, cmd)
		if err := cmd.Err(); err != nil && !strings.Contains(err.Error(), "ERR Duplicate master name.") {
			return nil, fmt.Errorf("%s failed: %w", cmd, err)
		}
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

// loadClusterTLSConfig loads TLS certificates for cluster tests
func loadClusterTLSConfig(certDir string) (*tls.Config, error) {
	// Check if cert directory exists
	if _, err := os.Stat(certDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("certificate directory does not exist: %s", certDir)
	}

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

// initializeTLSCluster initializes the TLS cluster for testing
func initializeTLSCluster(ctx context.Context) error {
	// Load TLS config
	certDir := "dockers/osscluster-tls/tls"
	_, err := loadClusterTLSConfig(certDir)
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}

	// The TLS cluster is auto-created by the container (REDIS_CLUSTER=yes)
	// Just verify it's ready by checking cluster info
	client := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort("127.0.0.1", tlsCluster.ports[0]),
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	defer client.Close()

	// Wait for cluster to be ready
	err = eventually(func() error {
		info, err := client.ClusterInfo(ctx).Result()
		if err != nil {
			return fmt.Errorf("failed to get cluster info: %w", err)
		}
		if !strings.Contains(info, "cluster_state:ok") {
			return fmt.Errorf("cluster not ready: %s", info)
		}
		return nil
	}, 30*time.Second)
	if err != nil {
		return fmt.Errorf("TLS cluster not ready: %w", err)
	}

	return nil
}

// cleanupTLSCluster cleans up TLS cluster resources
func cleanupTLSCluster() {
	// TLS cluster is auto-managed by the container, no cleanup needed
}
