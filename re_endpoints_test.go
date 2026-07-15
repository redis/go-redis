package redis_test

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// reDatabaseEndpoint mirrors a raw_endpoints entry from the env0 endpoints config.
type reDatabaseEndpoint struct {
	DNSName string `json:"dns_name"`
	Port    int    `json:"port"`
}

// reDatabaseConfig is a single database entry in the env0 endpoints config file
// referenced by REDIS_ENDPOINTS_CONFIG_PATH.
type reDatabaseConfig struct {
	Username     string               `json:"username"`
	Password     string               `json:"password"`
	TLS          bool                 `json:"tls"`
	RawEndpoints []reDatabaseEndpoint `json:"raw_endpoints"`
	Endpoints    []string             `json:"endpoints"`
}

// reConnection holds the resolved connection parameters for the Redis Enterprise
// database the suite runs against.
type reConnection struct {
	Addr     string
	Username string
	Password string
	TLS      bool
}

// reConn is populated in BeforeSuite when running against a Redis Enterprise
// cluster provisioned outside the test process (RE_CLUSTER=true and
// REDIS_ENDPOINTS_CONFIG_PATH pointing at the env0 endpoints file).
var reConn *reConnection

// loadREConnection reads REDIS_ENDPOINTS_CONFIG_PATH and resolves the database to
// run the Redis Enterprise suite against. RE_DB_NAME selects a named database;
// when empty the first database in the config is used.
func loadREConnection() (*reConnection, error) {
	path := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	if path == "" {
		return nil, fmt.Errorf("REDIS_ENDPOINTS_CONFIG_PATH is not set")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}

	var databases map[string]reDatabaseConfig
	if err := json.Unmarshal(content, &databases); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if len(databases) == 0 {
		return nil, fmt.Errorf("no databases found in %s", path)
	}

	name := os.Getenv("RE_DB_NAME")
	var db reDatabaseConfig
	if name != "" {
		found, ok := databases[name]
		if !ok {
			return nil, fmt.Errorf("database %q not found in %s", name, path)
		}
		db = found
	} else {
		// Map iteration order is undefined, so pick the first database by sorted
		// name to make the default selection deterministic across runs.
		names := make([]string, 0, len(databases))
		for n := range databases {
			names = append(names, n)
		}
		sort.Strings(names)
		db = databases[names[0]]
	}

	host, port, tlsEnabled, err := resolveREEndpoint(db)
	if err != nil {
		return nil, err
	}

	return &reConnection{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Username: db.Username,
		Password: db.Password,
		TLS:      tlsEnabled,
	}, nil
}

// resolveREEndpoint extracts host, port and TLS from a database config, preferring
// raw_endpoints (which carry the resolvable DNS name and port) over the endpoint URLs.
func resolveREEndpoint(db reDatabaseConfig) (string, int, bool, error) {
	tlsEnabled := db.TLS
	if len(db.RawEndpoints) > 0 {
		// Fall through to the endpoint URLs when the raw endpoint carries no
		// DNS name, so a sparse config fails loudly instead of dialing ":port".
		if ep := db.RawEndpoints[0]; ep.DNSName != "" {
			return ep.DNSName, ep.Port, tlsEnabled, nil
		}
	}
	if len(db.Endpoints) > 0 {
		u, err := url.Parse(db.Endpoints[0])
		if err != nil {
			return "", 0, false, fmt.Errorf("failed to parse endpoint %q: %w", db.Endpoints[0], err)
		}
		if u.Scheme == "rediss" {
			tlsEnabled = true
		}
		// The env0 endpoints format may omit the port; fall back to the scheme
		// default (6379 for redis, 6380 for rediss), matching maintnotifications/e2e.
		port := 6379
		if u.Scheme == "rediss" {
			port = 6380
		}
		if p := u.Port(); p != "" {
			parsed, err := strconv.Atoi(p)
			if err != nil {
				return "", 0, false, fmt.Errorf("invalid port in endpoint %q: %w", db.Endpoints[0], err)
			}
			port = parsed
		}
		return u.Hostname(), port, tlsEnabled, nil
	}
	return "", 0, false, fmt.Errorf("no endpoints found in database configuration")
}

// A remote Redis Enterprise endpoint has higher latency than a local server, so
// the overlays below use the same relaxed timeouts as redisOptions() instead of
// the ~3s defaults; otherwise module/search/timeseries helpers can flake on
// slower commands.
const (
	reDialTimeout  = 10 * time.Second
	reReadTimeout  = 30 * time.Second
	reWriteTimeout = 30 * time.Second
)

// applyREConnection overlays the resolved Redis Enterprise credentials and TLS
// settings onto opt. It is a no-op when reConn has not been populated.
func applyREConnection(opt *redis.Options) {
	if reConn == nil {
		return
	}
	opt.Addr = reConn.Addr
	opt.Username = reConn.Username
	opt.Password = reConn.Password
	if reConn.TLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	opt.DialTimeout = reDialTimeout
	opt.ReadTimeout = reReadTimeout
	opt.WriteTimeout = reWriteTimeout
	opt.ContextTimeoutEnabled = true
}

// applyREConnectionUniversal overlays the resolved Redis Enterprise endpoint,
// credentials and TLS settings onto a UniversalOptions. It is a no-op when
// reConn has not been populated.
func applyREConnectionUniversal(opt *redis.UniversalOptions) {
	if reConn == nil {
		return
	}
	opt.Addrs = []string{reConn.Addr}
	opt.Username = reConn.Username
	opt.Password = reConn.Password
	if reConn.TLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	opt.DialTimeout = reDialTimeout
	opt.ReadTimeout = reReadTimeout
	opt.WriteTimeout = reWriteTimeout
	opt.ContextTimeoutEnabled = true
}

// reNewClient builds a *redis.Client for the configured endpoint, applying the
// Redis Enterprise connection details (host, credentials, TLS) when running
// against an external cluster via REDIS_ENDPOINTS_CONFIG_PATH.
func reNewClient(protocol int, unstableResp3 bool) *redis.Client {
	opt := &redis.Options{
		Addr:          redisAddr,
		Protocol:      protocol,
		UnstableResp3: unstableResp3,
	}
	applyREConnection(opt)
	return redis.NewClient(opt)
}
