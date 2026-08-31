package redis

import (
	"net"
	"time"
)

// probeRedis is the package redis copy of the redis_test helper of the same
// name (the internal tests cannot import the external test package). See
// probe_test.go for the rationale: a fast TCP probe avoids the ~1.7s
// dial-retry + command-retry cost a Ping pays when nothing is listening.
func probeRedis(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return err
	}
	return c.Close()
}
