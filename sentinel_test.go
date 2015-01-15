package redis_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Sentinel", func() {

	const masterName = "mymaster"
	const masterPort = "8123"
	const sentinelPort = "8124"
	const sentinelConf = `
port ` + sentinelPort + `

sentinel monitor ` + masterName + ` 127.0.0.1 ` + masterPort + ` 1
sentinel down-after-milliseconds ` + masterName + ` 400
sentinel failover-timeout ` + masterName + ` 800
sentinel parallel-syncs ` + masterName + ` 1
`

	var runCmd = func(name string, args ...string) *os.Process {
		cmd := exec.Command(name, args...)
		if false {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}
		err := cmd.Start()
		Expect(err).NotTo(HaveOccurred())
		return cmd.Process
	}

	var connect = func(port string) *redis.Client {
		client := redis.NewTCPClient(&redis.Options{
			Addr: ":" + port,
		})
		Eventually(func() error {
			return client.Ping().Err()
		}, "1s", "100ms").ShouldNot(HaveOccurred())
		return client
	}

	var startMaster = func() (*redis.Client, *os.Process) {
		proc := runCmd("redis-server", "--port", masterPort)
		return connect(masterPort), proc
	}

	var startSlave = func(port string) (*redis.Client, *os.Process) {
		proc := runCmd("redis-server", "--port", port, "--slaveof", "127.0.0.1", masterPort)
		return connect(port), proc
	}

	var startSentinel = func() *os.Process {
		dir, err := ioutil.TempDir("", "sentinel")
		Expect(err).NotTo(HaveOccurred())

		fname := filepath.Join(dir, "sentinel.conf")
		err = ioutil.WriteFile(fname, []byte(sentinelConf), 0664)
		Expect(err).NotTo(HaveOccurred())

		proc := runCmd("redis-server", fname, "--sentinel")
		client := connect(sentinelPort)
		client.Close()
		return proc
	}

	It("should facilitate failover", func() {
		master, mproc := startMaster()
		defer mproc.Kill()
		slave1, sproc1 := startSlave("8125")
		defer sproc1.Kill()
		slave2, sproc2 := startSlave("8126")
		defer sproc2.Kill()
		sntproc := startSentinel()
		defer sntproc.Kill()

		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: []string{":" + sentinelPort},
		})
		defer client.Close()

		// Set value on master, verify
		err := client.Set("foo", "master").Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := master.Get("foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Wait until replicated
		Eventually(func() string {
			return slave1.Get("foo").Val()
		}, "1s", "100ms").Should(Equal("master"))
		Eventually(func() string {
			return slave2.Get("foo").Val()
		}, "1s", "100ms").Should(Equal("master"))

		// Kill master.
		master.Shutdown()
		Eventually(func() error {
			return master.Ping().Err()
		}, "5s", "100ms").Should(HaveOccurred())

		// Wait for Redis sentinel to elect new master.
		Eventually(func() string {
			return slave1.Info().Val() + slave2.Info().Val()
		}, "30s", "500ms").Should(ContainSubstring("role:master"))

		// Check that client picked up new master.
		val, err = client.Get("foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))
	})

})
