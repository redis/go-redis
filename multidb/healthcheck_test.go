package multidb

import (
	"context"
	"net/http"
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
		if !hc.CheckHealth(ctx, client) {
			t.Error("expected CheckHealth to return true for healthy client")
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

		if hc.CheckHealth(ctx, client) {
			t.Error("expected CheckHealth to return false for unreachable client")
		}
	})
}

// mockHealthCheck is a test helper that returns a configurable result
type mockHealthCheck struct {
	healthy bool
}

func (m *mockHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) bool {
	return m.healthy
}

func (m *mockHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) bool {
	return m.healthy
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
		if hc.CheckHealth(ctx, client) {
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
	if hc.CheckHealth(context.Background(), client) {
		t.Error("expected CheckHealth to return false when config error is set")
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

func (s *sequenceHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) bool {
	if s.index >= len(s.results) {
		return false
	}
	result := s.results[s.index]
	s.index++
	return result
}

func (s *sequenceHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) bool {
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
