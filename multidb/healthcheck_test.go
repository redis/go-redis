package multidb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
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
			Addr:        "localhost:0", // port 0: dial fails deterministically
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
			Addrs:       []string{"localhost:0"},
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
				{DNSName: "redis.example.com", Addr: []string{"10.0.0.1"}, Port: 12000},
			},
		}

		if !hc.bdbMatchesHost(bdb, "redis.example.com", 0, nil) {
			t.Error("expected bdbMatchesHost to match DNS name without a port")
		}
		if !hc.bdbMatchesHost(bdb, "redis.example.com", 12000, nil) {
			t.Error("expected bdbMatchesHost to match DNS name with matching port")
		}
		if hc.bdbMatchesHost(bdb, "redis.example.com", 12001, nil) {
			t.Error("expected bdbMatchesHost to reject a same-host different-port database")
		}
		if !hc.bdbMatchesHost(bdb, "Redis.EXAMPLE.com", 12000, nil) {
			t.Error("expected bdbMatchesHost to match DNS name case-insensitively")
		}
		if !hc.bdbMatchesHost(bdb, "10.0.0.1", 12000, nil) {
			t.Error("expected bdbMatchesHost to match address")
		}
		if hc.bdbMatchesHost(bdb, "other.example.com", 0, nil) {
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

// trackingBody records whether a response body was read to EOF and closed.
type trackingBody struct {
	r       io.Reader
	drained bool
	closed  bool
}

func (b *trackingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.drained = true
	}
	return n, err
}

func (b *trackingBody) Close() error { b.closed = true; return nil }

type stubHTTPClient struct{ resp *http.Response }

func (c *stubHTTPClient) Do(*http.Request) (*http.Response, error) { return c.resp, nil }

func TestLagAwareDrainsMalformedBDBResponse(t *testing.T) {
	// A 200 with undecodable JSON must still be drained: a misbehaving admin
	// API that keeps producing bad responses sits in a health-check loop,
	// and an undrained body makes the transport burn a new TCP/TLS
	// connection per probe instead of reusing the keep-alive one.
	body := &trackingBody{r: strings.NewReader("not json at all, with trailing bytes the decoder never touches")}
	hc := NewLagAwareHealthCheck(
		WithLagAwareHTTPClient(&stubHTTPClient{resp: &http.Response{StatusCode: 200, Body: body}}),
	)

	if _, err := hc.getBDBs(context.Background(), "http://cluster.example/v1/bdbs"); err == nil {
		t.Fatal("expected a decode error for malformed JSON")
	}
	if !body.drained {
		t.Error("malformed response body was not drained — the keep-alive connection cannot be reused")
	}
	if !body.closed {
		t.Error("response body was not closed")
	}
}

func TestLagAwareBDBMatchesEquivalentIPText(t *testing.T) {
	hc := NewLagAwareHealthCheck()
	bdb := bdbInfo{
		UID: 7,
		Endpoints: []bdbEndpoint{
			{DNSName: "db.example.com", Addr: []string{"2001:db8::1", "10.0.0.1"}, Port: 6379},
		},
	}

	// IPv6 text is not canonical: the client's configured address and the
	// REST API's JSON can spell the same IP differently. Equivalent
	// representations must match, or a healthy member reports unavailable.
	for _, host := range []string{"2001:0db8::1", "2001:db8:0:0:0:0:0:1", "2001:db8::1"} {
		if !hc.bdbMatchesHost(bdb, host, 6379, nil) {
			t.Errorf("bdbMatchesHost(%q) = false, want true (equivalent IPv6 text)", host)
		}
	}
	if !hc.bdbMatchesHost(bdb, "10.0.0.1", 6379, nil) {
		t.Error("bdbMatchesHost(10.0.0.1) = false, want true")
	}
	if hc.bdbMatchesHost(bdb, "2001:db8::2", 6379, nil) {
		t.Error("bdbMatchesHost(2001:db8::2) = true, want false (different IP)")
	}
}

func TestLagAwareIsFailbackOnly(t *testing.T) {
	// The lag-aware REST check may only gate routing traffic TO a member
	// (candidate probes, auto-fallback, initial selection): replication lag
	// on the member already serving traffic is not an eviction signal, so
	// the client's background loop must be able to identify the check and
	// skip it for the current active. Failover rides on traffic signals.
	hc := NewLagAwareHealthCheck()
	if !hc.FailbackOnly() {
		t.Error("LagAwareHealthCheck.FailbackOnly() = false, want true")
	}
}

func TestLagAwareHostPortFromAddr(t *testing.T) {
	tests := []struct {
		addr     string
		wantHost string
		wantPort int
		wantOK   bool
	}{
		{"localhost:6379", "localhost", 6379, true},
		{":6379", "localhost", 6379, true},
		{"10.0.0.1:6379", "10.0.0.1", 6379, true},
		{"redis.example.com:9443", "redis.example.com", 9443, true},
		{"[::1]:6379", "::1", 6379, true},
		{"[2001:db8::1]:6379", "2001:db8::1", 6379, true},
		// Service-name ports are valid dial targets; they must resolve (via
		// the local services database) instead of degrading to the port-0
		// wildcard, which would defeat port disambiguation for Enterprise
		// endpoints sharing one DNS name.
		{"redis.example.com:https", "redis.example.com", 443, true},
		{"localhost", "localhost", 0, true},
		{"[::1]", "::1", 0, true},
		{"[2001:db8::1]", "2001:db8::1", 0, true},
		{"", "", 0, false},
		{"/tmp/redis.sock", "", 0, false},
		{"unix:///tmp/redis.sock", "", 0, false},
	}
	for _, tc := range tests {
		host, port, ok := hostPortFromAddr(tc.addr)
		if ok != tc.wantOK || host != tc.wantHost || port != tc.wantPort {
			t.Errorf("hostPortFromAddr(%q) = (%q, %d, %v), want (%q, %d, %v)",
				tc.addr, host, port, ok, tc.wantHost, tc.wantPort, tc.wantOK)
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

func TestLagAwareTLSConfigOptionIsCloned(t *testing.T) {
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

// genTestCAPEM builds a throwaway self-signed CA certificate at runtime so
// tests can exercise real PEM parsing without a checked-in fixture.
func genTestCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "multidb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestLagAwareTLSConfigDoesNotAliasCallerPool(t *testing.T) {
	// tls.Config.Clone is shallow: RootCAs is a shared *x509.CertPool. A CA
	// appended by a later option must land in the health check's own pool,
	// never in the caller's.
	callerPool := x509.NewCertPool()
	caller := &tls.Config{RootCAs: callerPool}
	hc := NewLagAwareHealthCheck(
		WithLagAwareTLSConfig(caller),
		WithLagAwareRootCAs(genTestCAPEM(t)),
	)
	if hc.configErr != nil {
		t.Fatalf("unexpected config error: %v", hc.configErr)
	}
	if !callerPool.Equal(x509.NewCertPool()) {
		t.Error("appending root CAs after WithLagAwareTLSConfig mutated the caller's RootCAs pool")
	}
	if hc.tlsConfig.RootCAs.Equal(x509.NewCertPool()) {
		t.Error("expected the appended CA to land in the health check's own pool")
	}
}

func TestLagAwareTLSConfigDoesNotAliasCallerCertificates(t *testing.T) {
	// The shallow clone also shares the Certificates backing array; with
	// spare capacity, a later append would write into the caller's array.
	caller := &tls.Config{Certificates: make([]tls.Certificate, 1, 4)}
	caller.Certificates[0] = tls.Certificate{Certificate: [][]byte{[]byte("caller")}}
	hidden := caller.Certificates[:2]

	hc := NewLagAwareHealthCheck(WithLagAwareTLSConfig(caller))
	hc.tlsConfig.Certificates = append(hc.tlsConfig.Certificates, tls.Certificate{
		Certificate: [][]byte{[]byte("healthcheck")},
	})

	if hidden[1].Certificate != nil {
		t.Error("appending a certificate wrote into the caller's Certificates backing array")
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

// scriptedHTTPClient records URLs and plays back canned responses in order —
// a nil entry means a transport error — failing any request beyond the script.
type scriptedHTTPClient struct {
	urls      []string
	responses []*http.Response
}

func (c *scriptedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.urls = append(c.urls, req.URL.String())
	if len(c.responses) == 0 {
		return nil, context.DeadlineExceeded
	}
	resp := c.responses[0]
	c.responses = c.responses[1:]
	if resp == nil {
		return nil, context.DeadlineExceeded
	}
	return resp, nil
}

// drainTrackingBody reports whether the response body was read to EOF before
// being closed — the precondition for HTTP keep-alive connection reuse.
type drainTrackingBody struct {
	r      *strings.Reader
	sawEOF bool
}

func (b *drainTrackingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.sawEOF = true
	}
	return n, err
}

func (b *drainTrackingBody) Close() error { return nil }

func TestLagAwareClusterTriesAllSeedAddresses(t *testing.T) {
	bdbList := `[{"uid": 3, "endpoints": [{"dns_name": "seed2.example.com", "addr": [], "port": 6379}]}]`
	capture := &scriptedHTTPClient{responses: []*http.Response{
		nil, // seed1's REST API is unreachable
		{StatusCode: 200, Body: io.NopCloser(strings.NewReader(bdbList))},
		{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))},
	}}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(capture))
	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"seed1.example.com:6379", "seed2.example.com:6379"},
	})
	defer cc.Close()

	// The cluster can be perfectly healthy while the first seed is down; the
	// lag check must try the remaining seeds instead of failing on Addrs[0].
	ok, err := hc.CheckClusterHealth(context.Background(), cc)
	if !ok || err != nil {
		t.Fatalf("CheckClusterHealth = (%v, %v), want healthy via the second seed", ok, err)
	}
}

// hostScriptedHTTPClient plays back canned responses per host, in order. The
// lag check discovers masters concurrently (ForEachMaster), so the order in
// which hosts are probed is not fixed; keying by host keeps the script
// deterministic while the per-host sequence (bdbs, then availability) is.
type hostScriptedHTTPClient struct {
	responses map[string][]*http.Response // keyed by URL hostname
}

func (c *hostScriptedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	rs := c.responses[host]
	if len(rs) == 0 {
		return nil, context.DeadlineExceeded
	}
	c.responses[host] = rs[1:]
	if rs[0] == nil {
		return nil, context.DeadlineExceeded
	}
	return rs[0], nil
}

// TestLagAwareClusterRequiresEveryMaster pins the verdict rule for a cluster
// member whose masters were discovered: EVERY routed master must pass, like
// the cluster PING check. Commands route by key to all masters, so one
// lagging or unavailable master exposes stale data or failures for its slots
// no matter how healthy the others are. (The seed fallback keeps any-passes:
// see TestLagAwareClusterTriesAllSeedAddresses.)
func TestLagAwareClusterRequiresEveryMaster(t *testing.T) {
	ok200 := func(body string) *http.Response {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}
	}
	bdb := func(uid int, host string) string {
		return fmt.Sprintf(`[{"uid": %d, "endpoints": [{"dns_name": %q, "addr": [], "port": 6379}]}]`, uid, host)
	}
	// Two masters, supplied as topology so no Redis is dialed.
	newCluster := func() *redis.ClusterClient {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{"m1.example.com:6379"},
			ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
				return []redis.ClusterSlot{
					{Start: 0, End: 8191, Nodes: []redis.ClusterNode{{Addr: "m1.example.com:6379"}}},
					{Start: 8192, End: 16383, Nodes: []redis.ClusterNode{{Addr: "m2.example.com:6379"}}},
				}, nil
			},
		})
	}

	t.Run("one master not passing fails the member", func(t *testing.T) {
		client := &hostScriptedHTTPClient{responses: map[string][]*http.Response{
			"m1.example.com": {ok200(bdb(1, "m1.example.com")), ok200(`{}`)},
			"m2.example.com": {ok200(bdb(2, "m2.example.com")), {StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{}`))}},
		}}
		hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(client))
		cc := newCluster()
		defer cc.Close()
		ok, err := hc.CheckClusterHealth(context.Background(), cc)
		if ok || err == nil {
			t.Fatalf("CheckClusterHealth = (%v, %v), want unhealthy: one routed master did not pass", ok, err)
		}
	})

	t.Run("every master passing is healthy", func(t *testing.T) {
		client := &hostScriptedHTTPClient{responses: map[string][]*http.Response{
			"m1.example.com": {ok200(bdb(1, "m1.example.com")), ok200(`{}`)},
			"m2.example.com": {ok200(bdb(2, "m2.example.com")), ok200(`{}`)},
		}}
		hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(client))
		cc := newCluster()
		defer cc.Close()
		ok, err := hc.CheckClusterHealth(context.Background(), cc)
		if !ok || err != nil {
			t.Fatalf("CheckClusterHealth = (%v, %v), want healthy", ok, err)
		}
	})
}

func TestLagAwareDrainsErrorResponseBody(t *testing.T) {
	// Repeated failing probes (401/503 loops) must also reuse the REST
	// connection: error bodies need draining exactly like success bodies.
	body := &drainTrackingBody{r: strings.NewReader(`{"error_code":"unauthorized"}`)}
	capture := &scriptedHTTPClient{responses: []*http.Response{
		{StatusCode: 401, Body: body},
	}}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(capture))
	client := redis.NewClient(&redis.Options{Addr: "redis.example.com:6379"})
	defer client.Close()

	if ok, err := hc.CheckHealth(context.Background(), client); ok || err == nil {
		t.Fatalf("CheckHealth = (%v, %v), want unhealthy with an error", ok, err)
	}
	if !body.sawEOF {
		t.Error("non-2xx response body not drained before return")
	}
}

// A configured hostname may be a DNS alias of the endpoint's canonical
// dns_name. The REST API reports the canonical name and the addresses, so the
// match must accept an alias that resolves to one of the reported addresses.
func TestLagAwareMatchesDNSAlias(t *testing.T) {
	bdbList := `[{"uid": 7, "endpoints": [{"dns_name": "redis-canonical.example.com", "addr": ["10.0.0.9"], "port": 6379}]}]`
	newClient := func() *scriptedHTTPClient {
		return &scriptedHTTPClient{responses: []*http.Response{
			{StatusCode: 200, Body: io.NopCloser(strings.NewReader(bdbList))},
			{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))},
		}}
	}
	client := redis.NewClient(&redis.Options{Addr: "alias.example.com:6379"})
	defer client.Close()

	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(newClient()))
	hc.lookupHost = func(_ context.Context, host string) ([]string, error) {
		if host != "alias.example.com" {
			t.Fatalf("resolved %q, want the configured host", host)
		}
		return []string{"10.0.0.9"}, nil
	}
	if ok, err := hc.CheckHealth(context.Background(), client); !ok || err != nil {
		t.Fatalf("CheckHealth = (%v, %v), want healthy: the alias resolves to the endpoint's address", ok, err)
	}

	// An alias that resolves elsewhere still does not match.
	hc = NewLagAwareHealthCheck(WithLagAwareHTTPClient(newClient()))
	hc.lookupHost = func(context.Context, string) ([]string, error) { return []string{"10.0.0.250"}, nil }
	if ok, err := hc.CheckHealth(context.Background(), client); ok || err == nil {
		t.Fatalf("CheckHealth = (%v, %v), want no matching bdb", ok, err)
	}
}

// With a fixed base URL every per-master call would hit the same /v1/local/
// endpoint: the cluster check runs once instead of once per master.
func TestLagAwareFixedBaseURLChecksClusterOnce(t *testing.T) {
	ok200 := func(body string) *http.Response {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}
	}
	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"m1.example.com:6379"},
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{
				{Start: 0, End: 8191, Nodes: []redis.ClusterNode{{Addr: "m1.example.com:6379"}}},
				{Start: 8192, End: 16383, Nodes: []redis.ClusterNode{{Addr: "m2.example.com:6379"}}},
			}, nil
		},
	})
	defer cc.Close()
	// Exactly one round of REST calls is scripted for the fixed host; a second
	// master's round would run out of responses and fail the check.
	client := &hostScriptedHTTPClient{responses: map[string][]*http.Response{
		"admin.example.com": {
			ok200(`[{"uid": 1, "endpoints": [{"dns_name": "m1.example.com", "addr": [], "port": 6379}, {"dns_name": "m2.example.com", "addr": [], "port": 6379}]}]`),
			ok200(`{}`),
		},
	}}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(client), WithLagAwareBaseURL("https://admin.example.com:9443"))
	ok, err := hc.CheckClusterHealth(context.Background(), cc)
	if !ok || err != nil {
		t.Fatalf("CheckClusterHealth = (%v, %v), want healthy after ONE check against the fixed URL", ok, err)
	}
	if left := len(client.responses["admin.example.com"]); left != 0 {
		t.Fatalf("%d scripted responses unused: the check did not run", left)
	}
}

func TestLagAwareDrainsAvailabilityBody(t *testing.T) {
	bdbList := `[{"uid": 7, "endpoints": [{"dns_name": "redis.example.com", "addr": [], "port": 6379}]}]`
	avail := &drainTrackingBody{r: strings.NewReader(`{"status":"ok"}`)}
	capture := &scriptedHTTPClient{responses: []*http.Response{
		{StatusCode: 200, Body: io.NopCloser(strings.NewReader(bdbList))},
		{StatusCode: 200, Body: avail},
	}}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(capture))
	client := redis.NewClient(&redis.Options{Addr: "redis.example.com:6379"})
	defer client.Close()

	if ok, err := hc.CheckHealth(context.Background(), client); !ok || err != nil {
		t.Fatalf("CheckHealth = (%v, %v), want healthy", ok, err)
	}
	if !avail.sawEOF {
		t.Error("availability response body not drained before close: the keep-alive connection cannot be reused")
	}
}

func TestLagAwareChecksLocalEndpointAvailability(t *testing.T) {
	bdbList := `[{"uid": 7, "endpoints": [{"dns_name": "redis.example.com", "addr": ["10.0.0.1"], "port": 6379}]}]`
	capture := &scriptedHTTPClient{responses: []*http.Response{
		{StatusCode: 200, Body: io.NopCloser(strings.NewReader(bdbList))},
		{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))},
	}}
	hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(capture))
	client := redis.NewClient(&redis.Options{Addr: "redis.example.com:6379"})
	defer client.Close()

	ok, err := hc.CheckHealth(context.Background(), client)
	if !ok || err != nil {
		t.Fatalf("CheckHealth = (%v, %v), want healthy", ok, err)
	}
	if len(capture.urls) != 2 {
		t.Fatalf("expected 2 REST calls, got %v", capture.urls)
	}
	// The availability probe must target the LOCAL ENDPOINT of the matched
	// database: the database-level check reports healthy as long as ANY
	// endpoint is up (without the OSS cluster API), which can mask an outage
	// of the endpoint this member actually uses.
	want := "https://redis.example.com:9443/v1/local/bdbs/7/endpoint/availability?extend_check=lag&availability_lag_tolerance_ms=5000"
	if got := capture.urls[1]; got != want {
		t.Errorf("availability URL = %q, want %q", got, want)
	}
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
	// fields keeps frequent probes cheap: the matcher only needs uid and
	// endpoints, not full database configs.
	want := "https://[::1]:9443/v1/bdbs?fields=uid,endpoints"
	if got != want {
		t.Errorf("IPv6 base URL = %q, want %q", got, want)
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

// latePassCheck ignores ctx, sleeps, then reports healthy: a check whose
// client has no context timeouts enabled and answers after the check's own
// Timeout.
type latePassCheck struct {
	cfg   HealthCheckConfig
	sleep time.Duration
}

func (c *latePassCheck) Config() HealthCheckConfig { return c.cfg }
func (c *latePassCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	time.Sleep(c.sleep)
	return true, nil
}

func (c *latePassCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	time.Sleep(c.sleep)
	return true, nil
}

// A pass that arrives after the check's Timeout must not count as healthy:
// the timeout is the bound on how long a healthy member may take to answer.
func TestPoliciesRejectPassAfterCheckTimeout(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()
	for name, p := range map[string]interface {
		Execute(context.Context, []redis.MultiDBHealthCheck, *redis.Client) bool
	}{
		"all":      NewHealthyAllPolicy(),
		"majority": NewHealthyMajorityPolicy(),
		"any":      NewHealthyAnyPolicy(),
	} {
		t.Run(name, func(t *testing.T) {
			late := &latePassCheck{cfg: HealthCheckConfig{Probes: 1, Timeout: 30 * time.Millisecond}, sleep: 150 * time.Millisecond}
			if p.Execute(context.Background(), []redis.MultiDBHealthCheck{late}, client) {
				t.Fatal("a pass that arrived after the check's Timeout was accepted as healthy")
			}
			inTime := &latePassCheck{cfg: HealthCheckConfig{Probes: 1, Timeout: time.Second}}
			if !p.Execute(context.Background(), []redis.MultiDBHealthCheck{inTime}, client) {
				t.Fatal("an in-time pass was rejected")
			}
		})
	}
}
