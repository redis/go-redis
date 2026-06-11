package multidb

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPingHealthCheck(t *testing.T) {
	t.Run("CheckHealth returns true for healthy client", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		ctx := context.Background()
		if err := client.Ping(ctx).Err(); err != nil {
			t.Skipf("Redis not available: %v", err)
		}

		hc := NewPingHealthCheck()
		if ok, err := hc.CheckHealth(ctx, client); !ok {
			t.Errorf("expected CheckHealth to return true for healthy client, err=%v", err)
		}
	})

	t.Run("CheckHealth returns false for unreachable client", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{
			Addr:        "localhost:59999", // unlikely to be running
			DialTimeout: 100 * time.Millisecond,
		})
		defer client.Close()

		hc := NewPingHealthCheck()
		ctx := context.Background()

		if ok, err := hc.CheckHealth(ctx, client); ok {
			t.Error("expected CheckHealth to return false for unreachable client")
		} else if err == nil {
			t.Error("expected CheckHealth to return a non-nil error for unreachable client")
		}
	})

	t.Run("CheckClusterHealth returns false+error for unreachable/empty cluster", func(t *testing.T) {
		// A cluster with no reachable shards must not be reported as trivially
		// healthy: CheckClusterHealth must return (false, err) rather than
		// (true, nil) when no shard was actually pinged.
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       []string{"localhost:59999"},
			DialTimeout: 100 * time.Millisecond,
		})
		defer client.Close()

		hc := NewPingHealthCheck()
		if ok, err := hc.CheckClusterHealth(context.Background(), client); ok {
			t.Error("expected CheckClusterHealth to return false for an empty/unreachable cluster")
		} else if err == nil {
			t.Error("expected CheckClusterHealth to return a non-nil error for an empty/unreachable cluster")
		}
	})
}

// mockHealthCheck is a test helper that returns a configurable result
type mockHealthCheck struct {
	healthy bool
}

func (m *mockHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	return m.healthy, nil
}

func (m *mockHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	return m.healthy, nil
}

// --- LagAwareHealthCheck Tests ---

func TestLagAwareHealthCheck(t *testing.T) {
	t.Run("NewLagAwareHealthCheck with defaults", func(t *testing.T) {
		hc := NewLagAwareHealthCheck()

		if hc.restAPIPort != DefaultRESTAPIPort {
			t.Errorf("expected restAPIPort=%d, got %d", DefaultRESTAPIPort, hc.restAPIPort)
		}
		if hc.lagTolerance != DefaultLagTolerance {
			t.Errorf("expected lagTolerance=%d, got %d", DefaultLagTolerance, hc.lagTolerance)
		}
		if hc.httpClient == nil {
			t.Error("expected httpClient to be set")
		}
	})

	t.Run("NewLagAwareHealthCheck with options", func(t *testing.T) {
		hc := NewLagAwareHealthCheck(
			WithLagAwareBaseURL("https://example.com"),
			WithLagAwareRESTAPIPort(8443),
			WithLagAwareTolerance(1000),
			WithLagAwareBasicAuth("user", "pass"),
		)

		if hc.baseURL != "https://example.com" {
			t.Errorf("expected baseURL=https://example.com, got %s", hc.baseURL)
		}
		if hc.restAPIPort != 8443 {
			t.Errorf("expected restAPIPort=8443, got %d", hc.restAPIPort)
		}
		if hc.lagTolerance != 1000 {
			t.Errorf("expected lagTolerance=1000, got %d", hc.lagTolerance)
		}
		if hc.username != "user" || hc.password != "pass" {
			t.Error("expected basic auth to be set")
		}
	})

	t.Run("CheckHealth returns false when REST API is unreachable", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		// Use a mock HTTP client that always fails
		hc := NewLagAwareHealthCheck(
			WithLagAwareHTTPClient(&mockHTTPClient{err: context.DeadlineExceeded}),
		)

		ctx := context.Background()
		if ok, _ := hc.CheckHealth(ctx, client); ok {
			t.Error("expected CheckHealth to return false when REST API is unreachable")
		}
	})

	t.Run("bdbMatchesHost matches DNS name", func(t *testing.T) {
		hc := NewLagAwareHealthCheck()
		bdb := bdbInfo{
			UID: 1,
			Endpoints: []bdbEndpoint{
				{DNSName: "redis.example.com", Addr: []string{"10.0.0.1"}},
			},
		}

		if !hc.bdbMatchesHost(bdb, "redis.example.com") {
			t.Error("expected bdbMatchesHost to match DNS name")
		}
		if !hc.bdbMatchesHost(bdb, "10.0.0.1") {
			t.Error("expected bdbMatchesHost to match address")
		}
		if hc.bdbMatchesHost(bdb, "other.example.com") {
			t.Error("expected bdbMatchesHost to not match different host")
		}
	})

	t.Run("TLS options are applied", func(t *testing.T) {
		// Test InsecureSkipVerify
		hc := NewLagAwareHealthCheck(
			WithLagAwareInsecureSkipVerify(),
		)
		if hc.tlsConfig == nil {
			t.Fatal("expected tlsConfig to be set")
		}
		if !hc.tlsConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify to be true")
		}

		// Test RootCAs with PEM data
		caPEM := []byte(`-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpegAzYCMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnVu
dXNlZDAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnVudXNlZDBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96WoVCH9xgnLRkMz8pN
2FteamOrPwGMKfkMqF+EAlyH3/wMP0luxSK8BOxdBz0SSlmj2PJwqFcF2rXmVykv
AgMBAAGjUzBRMB0GA1UdDgQWBBQK7ULMHX4ELihB4Bsg+caBRgLsVzAfBgNVHSME
GDAWgBQK7ULMHX4ELihB4Bsg+caBRgLsVzAPBgNVHRMBAf8EBTADAQH/MA0GCSqG
SIb3DQEBCwUAA0EA0FH0N5LT0Y6P6iKv9eDLqE8n6kWUKFq3V6sNqJBUzBuV5IpM
H8PD6BY8JK7P5K8K0K8K0K8K0K8K0K8K0K8K0A==
-----END CERTIFICATE-----`)
		hc2 := NewLagAwareHealthCheck(
			WithLagAwareRootCAs(caPEM),
		)
		if hc2.tlsConfig == nil {
			t.Fatal("expected tlsConfig to be set")
		}
		if hc2.tlsConfig.RootCAs == nil {
			t.Error("expected RootCAs to be set")
		}
	})
}

func TestLagAwareHostFromAddr(t *testing.T) {
	tests := []struct {
		addr     string
		wantHost string
		wantOK   bool
	}{
		{"localhost:6379", "localhost", true},
		{"10.0.0.1:6379", "10.0.0.1", true},
		{"redis.example.com:9443", "redis.example.com", true},
		{"[::1]:6379", "::1", true},
		{"[2001:db8::1]:6379", "2001:db8::1", true},
		{"localhost", "localhost", true},
		{"", "", false},
		{"/tmp/redis.sock", "", false},
		{"unix:///tmp/redis.sock", "", false},
	}
	for _, tc := range tests {
		host, ok := hostFromAddr(tc.addr)
		if ok != tc.wantOK || host != tc.wantHost {
			t.Errorf("hostFromAddr(%q) = (%q, %v), want (%q, %v)",
				tc.addr, host, ok, tc.wantHost, tc.wantOK)
		}
	}
}

func TestLagAwareConfigErrorFailsHealthCheck(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// An invalid PEM records a config error, which must fail health checks.
	hc := NewLagAwareHealthCheck(
		WithLagAwareRootCAs([]byte("not a valid pem")),
	)
	if hc.configErr == nil {
		t.Fatal("expected configErr to be set for invalid root CA PEM")
	}
	if ok, err := hc.CheckHealth(context.Background(), client); ok {
		t.Error("expected CheckHealth to return false when config error is set")
	} else if err == nil {
		t.Error("expected CheckHealth to surface the config error")
	}
}

func TestLagAwareTLSConfigIsCloned(t *testing.T) {
	caller := &tls.Config{}
	hc := NewLagAwareHealthCheck(
		WithLagAwareTLSConfig(caller),
		WithLagAwareInsecureSkipVerify(),
	)
	if !hc.tlsConfig.InsecureSkipVerify {
		t.Error("expected health check TLS config to have InsecureSkipVerify set")
	}
	if caller.InsecureSkipVerify {
		t.Error("expected caller TLS config to be left unmodified")
	}
	if hc.tlsConfig == caller {
		t.Error("expected health check to hold a clone, not the caller's TLS config")
	}
}

// mockHTTPClient is a mock HTTP client for testing.
type mockHTTPClient struct {
	response *http.Response
	err      error
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

// urlCapturingHTTPClient records the URLs it is asked to fetch and always
// fails the request, so the caller's CheckHealth returns early. It is used to
// assert how the base URL is constructed without needing a live REST API.
type urlCapturingHTTPClient struct {
	urls []string
}

func (c *urlCapturingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.urls = append(c.urls, req.URL.String())
	return nil, context.DeadlineExceeded
}

func TestLagAwareIPv6BaseURL(t *testing.T) {
	// An IPv6 Redis address must produce a bracketed, parseable HTTPS base URL
	// (https://[::1]:9443/...), not the malformed https://::1:9443/...
	capture := &urlCapturingHTTPClient{}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(capture))

	client := redis.NewClient(&redis.Options{Addr: "[::1]:6379"})
	defer client.Close()

	if ok, _ := hc.CheckHealth(context.Background(), client); ok {
		t.Fatal("expected CheckHealth to fail with the capturing client")
	}
	if len(capture.urls) == 0 {
		t.Fatal("expected at least one REST API request")
	}
	got := capture.urls[0]
	wantPrefix := "https://[::1]:9443/v1/bdbs"
	if got != wantPrefix {
		t.Errorf("IPv6 base URL = %q, want %q", got, wantPrefix)
	}
	// The URL must be parseable and round-trip the IPv6 host with brackets.
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("constructed URL %q is not parseable: %v", got, err)
	}
	if u.Hostname() != "::1" {
		t.Errorf("parsed hostname = %q, want ::1", u.Hostname())
	}
}

func TestLagAwareCheckHealthReturnsError(t *testing.T) {
	// CheckHealth must surface the underlying failure (here: the HTTP error)
	// so health-check metrics can record why the check was unhealthy.
	hc := NewLagAwareHealthCheck(
		WithLagAwareHTTPClient(&mockHTTPClient{err: context.DeadlineExceeded}),
	)
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	ok, err := hc.CheckHealth(context.Background(), client)
	if ok {
		t.Error("expected CheckHealth to return false")
	}
	if err == nil {
		t.Error("expected CheckHealth to return a non-nil error")
	}
}

func TestLagAwareUnusableAddrReturnsError(t *testing.T) {
	// A unix-socket address cannot yield a REST API host; CheckHealth must
	// report this as an error rather than a silent false.
	hc := NewLagAwareHealthCheck()
	client := redis.NewClient(&redis.Options{Network: "unix", Addr: "/tmp/redis.sock"})
	defer client.Close()

	ok, err := hc.CheckHealth(context.Background(), client)
	if ok {
		t.Error("expected CheckHealth to return false for unix-socket address")
	}
	if err == nil {
		t.Error("expected CheckHealth to return an error for unix-socket address")
	}
}

func TestGetConfigClampsInvalidValues(t *testing.T) {
	// A configurable check returning non-positive Probes/Timeout (or negative
	// Delay) must be clamped to defaults so probe runners stay robust.
	hc := &configReturningCheck{cfg: HealthCheckConfig{Probes: 0, Timeout: 0, Delay: -1}}
	got := getConfig(hc)
	if got.Probes != DefaultHealthCheckProbes {
		t.Errorf("Probes = %d, want clamped to %d", got.Probes, DefaultHealthCheckProbes)
	}
	if got.Timeout != DefaultHealthCheckTimeout {
		t.Errorf("Timeout = %v, want clamped to %v", got.Timeout, DefaultHealthCheckTimeout)
	}
	if got.Delay != DefaultHealthCheckDelay {
		t.Errorf("Delay = %v, want clamped to %v", got.Delay, DefaultHealthCheckDelay)
	}

	// A check with Probes=0 must not be treated as trivially healthy.
	policy := NewHealthyAllPolicy()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()
	bad := &countingCheck{}
	bad.cfg = HealthCheckConfig{Probes: 0, Timeout: time.Second}
	policy.Execute(context.Background(), []redis.MultiDBHealthCheck{bad}, client)
	if bad.calls == 0 {
		t.Error("expected at least one probe call after clamping Probes=0 to default")
	}
}

// configReturningCheck is a ConfigurableHealthCheck that returns a fixed config.
type configReturningCheck struct {
	cfg HealthCheckConfig
}

func (c *configReturningCheck) Config() HealthCheckConfig { return c.cfg }
func (c *configReturningCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	return true, nil
}
func (c *configReturningCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	return true, nil
}

// countingCheck records how many probe calls it received.
type countingCheck struct {
	cfg   HealthCheckConfig
	calls int
}

func (c *countingCheck) Config() HealthCheckConfig { return c.cfg }
func (c *countingCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	c.calls++
	return true, nil
}
func (c *countingCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	c.calls++
	return true, nil
}

// --- Health Check Policy Tests ---

// sequenceHealthCheck returns different results for each probe call
type sequenceHealthCheck struct {
	results []bool
	index   int
	config  HealthCheckConfig
}

func newSequenceHealthCheck(results []bool) *sequenceHealthCheck {
	return &sequenceHealthCheck{
		results: results,
		config: HealthCheckConfig{
			Probes:  len(results),
			Delay:   0,
			Timeout: 3 * time.Second,
		},
	}
}

func (s *sequenceHealthCheck) Config() HealthCheckConfig {
	return s.config
}

func (s *sequenceHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	if s.index >= len(s.results) {
		return false, nil
	}
	result := s.results[s.index]
	s.index++
	return result, nil
}

func (s *sequenceHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	return s.CheckHealth(ctx, nil)
}

func TestHealthCheckPolicies(t *testing.T) {
	t.Run("HealthyAllPolicy requires all probes to pass", func(t *testing.T) {
		policy := NewHealthyAllPolicy()
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		// All probes pass
		hc := newSequenceHealthCheck([]bool{true, true, true})
		checks := []redis.MultiDBHealthCheck{hc}
		if !policy.Execute(context.Background(), checks, client) {
			t.Error("expected all passing probes to return true")
		}

		// One probe fails
		hc = newSequenceHealthCheck([]bool{true, false, true})
		checks = []redis.MultiDBHealthCheck{hc}
		if policy.Execute(context.Background(), checks, client) {
			t.Error("expected one failing probe to return false")
		}
	})

	t.Run("HealthyMajorityPolicy requires majority of probes to pass", func(t *testing.T) {
		policy := NewHealthyMajorityPolicy()
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		// 3 probes: 2 pass, 1 fails - should succeed (majority)
		hc := newSequenceHealthCheck([]bool{true, false, true})
		checks := []redis.MultiDBHealthCheck{hc}
		if !policy.Execute(context.Background(), checks, client) {
			t.Error("expected 2/3 passing probes to return true")
		}

		// 3 probes: 1 passes, 2 fail - should fail
		hc = newSequenceHealthCheck([]bool{true, false, false})
		checks = []redis.MultiDBHealthCheck{hc}
		if policy.Execute(context.Background(), checks, client) {
			t.Error("expected 1/3 passing probes to return false")
		}
	})

	t.Run("HealthyAnyPolicy requires at least one probe to pass", func(t *testing.T) {
		policy := NewHealthyAnyPolicy()
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		// First probe fails, second passes - should succeed
		hc := newSequenceHealthCheck([]bool{false, true, false})
		checks := []redis.MultiDBHealthCheck{hc}
		if !policy.Execute(context.Background(), checks, client) {
			t.Error("expected one passing probe to return true")
		}

		// All probes fail - should fail
		hc = newSequenceHealthCheck([]bool{false, false, false})
		checks = []redis.MultiDBHealthCheck{hc}
		if policy.Execute(context.Background(), checks, client) {
			t.Error("expected no passing probes to return false")
		}
	})

	t.Run("Empty checks return true for all policies", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()
		ctx := context.Background()

		var checks []redis.MultiDBHealthCheck

		if !NewHealthyAllPolicy().Execute(ctx, checks, client) {
			t.Error("HealthyAllPolicy should return true for empty checks")
		}
		if !NewHealthyMajorityPolicy().Execute(ctx, checks, client) {
			t.Error("HealthyMajorityPolicy should return true for empty checks")
		}
		if !NewHealthyAnyPolicy().Execute(ctx, checks, client) {
			t.Error("HealthyAnyPolicy should return true for empty checks")
		}
	})

	t.Run("Multiple health checks all must pass", func(t *testing.T) {
		policy := NewHealthyAllPolicy()
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer client.Close()

		// Two health checks, both pass
		checks := []redis.MultiDBHealthCheck{
			newSequenceHealthCheck([]bool{true, true, true}),
			newSequenceHealthCheck([]bool{true, true, true}),
		}
		if !policy.Execute(context.Background(), checks, client) {
			t.Error("expected both health checks to pass")
		}

		// Two health checks, one fails
		checks = []redis.MultiDBHealthCheck{
			newSequenceHealthCheck([]bool{true, true, true}),
			newSequenceHealthCheck([]bool{true, false, true}), // fails with AllPolicy
		}
		if policy.Execute(context.Background(), checks, client) {
			t.Error("expected one failing health check to return false")
		}
	})
}

func TestHealthCheckConfig(t *testing.T) {
	t.Run("DefaultHealthCheckConfig has correct values", func(t *testing.T) {
		cfg := DefaultHealthCheckConfig()
		if cfg.Probes != DefaultHealthCheckProbes {
			t.Errorf("expected Probes=%d, got %d", DefaultHealthCheckProbes, cfg.Probes)
		}
		if cfg.Delay != DefaultHealthCheckDelay {
			t.Errorf("expected Delay=%v, got %v", DefaultHealthCheckDelay, cfg.Delay)
		}
		if cfg.Timeout != DefaultHealthCheckTimeout {
			t.Errorf("expected Timeout=%v, got %v", DefaultHealthCheckTimeout, cfg.Timeout)
		}
	})

	t.Run("WithProbes sets probes", func(t *testing.T) {
		hc := NewPingHealthCheck(WithProbes(5))
		if hc.Config().Probes != 5 {
			t.Errorf("expected Probes=5, got %d", hc.Config().Probes)
		}
	})

	t.Run("WithDelay sets delay", func(t *testing.T) {
		hc := NewPingHealthCheck(WithDelay(100 * time.Millisecond))
		if hc.Config().Delay != 100*time.Millisecond {
			t.Errorf("expected Delay=100ms, got %v", hc.Config().Delay)
		}
	})

	t.Run("WithTimeout sets timeout", func(t *testing.T) {
		hc := NewPingHealthCheck(WithTimeout(5 * time.Second))
		if hc.Config().Timeout != 5*time.Second {
			t.Errorf("expected Timeout=5s, got %v", hc.Config().Timeout)
		}
	})
}

// panicHealthCheck panics on every probe, simulating a buggy check.
type panicHealthCheck struct{}

func (panicHealthCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	panic("panicHealthCheck: boom")
}
func (panicHealthCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	panic("panicHealthCheck: boom")
}

func TestRunChecksRecoversFromPanic(t *testing.T) {
	// A panicking check must be treated as unhealthy rather than dropping its
	// result: otherwise the consumer could return true after fewer than
	// len(checks) results.
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	checks := []redis.MultiDBHealthCheck{
		newSequenceHealthCheck([]bool{true, true, true}),
		panicHealthCheck{},
	}
	if NewHealthyAllPolicy().Execute(context.Background(), checks, client) {
		t.Error("expected a panicking health check to make the policy report unhealthy")
	}
}

func TestLagAwareHTTPClientTimeout(t *testing.T) {
	t.Run("defaults to DefaultHTTPTimeout", func(t *testing.T) {
		hc := NewLagAwareHealthCheck()
		client, ok := hc.httpClient.(*http.Client)
		if !ok {
			t.Fatalf("expected default *http.Client, got %T", hc.httpClient)
		}
		if client.Timeout != DefaultHTTPTimeout {
			t.Errorf("http client timeout = %v, want %v", client.Timeout, DefaultHTTPTimeout)
		}
	})

	t.Run("honors a larger probe timeout", func(t *testing.T) {
		hc := NewLagAwareHealthCheck(
			WithLagAwareHealthCheckConfig(WithTimeout(30 * time.Second)),
		)
		client, ok := hc.httpClient.(*http.Client)
		if !ok {
			t.Fatalf("expected default *http.Client, got %T", hc.httpClient)
		}
		if client.Timeout < 30*time.Second {
			t.Errorf("http client timeout = %v, want >= 30s", client.Timeout)
		}
	})
}

func TestLagAwareTLSConfigIsCloned(t *testing.T) {
	// The transport must use a clone of the supplied tls.Config so a later
	// mutation by the caller cannot race the transport's use of it.
	cfg := &tls.Config{InsecureSkipVerify: true}
	hc := NewLagAwareHealthCheck(WithLagAwareTLSConfig(cfg))

	client, ok := hc.httpClient.(*http.Client)
	if !ok {
		t.Fatalf("expected default *http.Client, got %T", hc.httpClient)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if transport.TLSClientConfig == cfg {
		t.Error("expected transport TLS config to be a clone, not the same pointer")
	}
	if transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Error("expected cloned TLS config to preserve InsecureSkipVerify")
	}
}
