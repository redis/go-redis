package redis

import (
	"net"
	"time"

	"gopkg.in/redis.v3/internal/pool"
)

type Options struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string
	// host:port address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (net.Conn, error)

	// An optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string
	// A database to be selected after connecting to server.
	DB int64

	// The maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int

	// Sets the deadline for establishing new connections. If reached,
	// dial will fail with a timeout.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Sets the deadline for socket reads. If reached, commands will
	// fail with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Sets the deadline for socket writes. If reached, commands will
	// fail with a timeout instead of blocking.
	WriteTimeout time.Duration

	// The maximum number of socket connections.
	// Default is 10 connections.
	PoolSize int
	// Specifies amount of time client waits for connection if all
	// connections are busy before returning an error.
	// Default is 1 second.
	PoolTimeout time.Duration
	// Specifies amount of time after which client closes idle
	// connections. Should be less than server's timeout.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// The frequency of idle checks.
	// Default is 1 minute.
	IdleCheckFrequency time.Duration
}

func (opt *Options) getNetwork() string {
	if opt.Network == "" {
		return "tcp"
	}
	return opt.Network
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	if opt.Dialer != nil {
		return opt.Dialer
	}
	return func() (net.Conn, error) {
		return net.DialTimeout(opt.getNetwork(), opt.Addr, opt.getDialTimeout())
	}
}

func (opt *Options) getPoolSize() int {
	if opt.PoolSize == 0 {
		return 10
	}
	return opt.PoolSize
}

func (opt *Options) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *Options) getPoolTimeout() time.Duration {
	if opt.PoolTimeout == 0 {
		return 1 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

func (opt *Options) getIdleCheckFrequency() time.Duration {
	if opt.IdleCheckFrequency == 0 {
		return time.Minute
	}
	return opt.IdleCheckFrequency
}

func newConnPool(opt *Options) *pool.ConnPool {
	return pool.NewConnPool(
		opt.getDialer(),
		opt.getPoolSize(),
		opt.getPoolTimeout(),
		opt.getIdleTimeout(),
		opt.getIdleCheckFrequency(),
	)
}

// PoolStats contains pool state information and accumulated stats.
type PoolStats struct {
	Requests uint32 // number of times a connection was requested by the pool
	Hits     uint32 // number of times free connection was found in the pool
	Waits    uint32 // number of times the pool had to wait for a connection
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // the number of total connections in the pool
	FreeConns  uint32 // the number of free connections in the pool
}
