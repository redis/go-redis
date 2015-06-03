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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

const (
	redisPort          = "6380"
	redisAddr          = ":" + redisPort
	redisSecondaryPort = "6381"
)

const (
	ringShard1Port = "6390"
	ringShard2Port = "6391"
)

const (
	sentinelName       = "mymaster"
	sentinelMasterPort = "8123"
	sentinelSlave1Port = "8124"
	sentinelSlave2Port = "8125"
	sentinelPort       = "8126"
)

var (
	redisMain                                                *redisProcess
	ringShard1, ringShard2                                   *redisProcess
	sentinelMaster, sentinelSlave1, sentinelSlave2, sentinel *redisProcess
)

var cluster = &clusterScenario{
	ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
	nodeIds:   make([]string, 6),
	processes: make(map[string]*redisProcess, 6),
	clients:   make(map[string]*redis.Client, 6),
}

var _ = BeforeSuite(func() {
	var err error

	redisMain, err = startRedis(redisPort)
	Expect(err).NotTo(HaveOccurred())

	ringShard1, err = startRedis(ringShard1Port)
	Expect(err).NotTo(HaveOccurred())

	ringShard2, err = startRedis(ringShard2Port)
	Expect(err).NotTo(HaveOccurred())

	sentinelMaster, err = startRedis(sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinel, err = startSentinel(sentinelPort, sentinelName, sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinelSlave1, err = startRedis(
		sentinelSlave1Port, "--slaveof", "127.0.0.1", sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	sentinelSlave2, err = startRedis(
		sentinelSlave2Port, "--slaveof", "127.0.0.1", sentinelMasterPort)
	Expect(err).NotTo(HaveOccurred())

	Expect(startCluster(cluster)).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	Expect(redisMain.Close()).NotTo(HaveOccurred())

	Expect(ringShard1.Close()).NotTo(HaveOccurred())
	Expect(ringShard2.Close()).NotTo(HaveOccurred())

	Expect(sentinel.Close()).NotTo(HaveOccurred())
	Expect(sentinelSlave1.Close()).NotTo(HaveOccurred())
	Expect(sentinelSlave2.Close()).NotTo(HaveOccurred())
	Expect(sentinelMaster.Close()).NotTo(HaveOccurred())

	Expect(stopCluster(cluster)).NotTo(HaveOccurred())
})

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gopkg.in/redis.v3")
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

//------------------------------------------------------------------------------

type badNetConn struct {
	net.TCPConn
}

var _ net.Conn = &badNetConn{}

func newBadNetConn() net.Conn {
	return &badNetConn{}
}

func (badNetConn) Read([]byte) (int, error) {
	return 0, net.UnknownNetworkError("badNetConn")
}

func (badNetConn) Write([]byte) (int, error) {
	return 0, net.UnknownNetworkError("badNetConn")
}
