package multidb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default values for LagAwareHealthCheck
const (
	DefaultRESTAPIPort  = 9443
	DefaultLagTolerance = 5000 // milliseconds
	DefaultHTTPTimeout  = 10 * time.Second
)

// LagAwareHealthCheck checks database health via Redis Enterprise REST API.
// It verifies that the database is healthy based on replication lag tolerance.
type LagAwareHealthCheck struct {
	config       HealthCheckConfig
	baseURL      string
	restAPIPort  int
	lagTolerance int
	httpClient   HTTPClient
	username     string
	password     string
	tlsConfig    *tls.Config
	// configErr records the first error encountered while applying options
	// (e.g. an invalid PEM or unreadable cert file). When set, health checks
	// fail fast rather than silently running with an incomplete TLS config.
	configErr error
}

// HTTPClient is an interface for making HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// LagAwareHealthCheckOption is a functional option for LagAwareHealthCheck.
type LagAwareHealthCheckOption func(*LagAwareHealthCheck)

// WithLagAwareHealthCheckConfig applies generic HealthCheckOption values
// (e.g. WithProbes, WithDelay, WithTimeout) to a LagAwareHealthCheck.
func WithLagAwareHealthCheckConfig(opts ...HealthCheckOption) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		for _, opt := range opts {
			opt(&h.config)
		}
	}
}

// WithLagAwareBaseURL sets the base URL for the REST API. By default the
// check derives it from the member's own database host, which the local
// endpoint-availability probe relies on: the /v1/local/ API answers for the
// node that serves the request. A custom base URL must therefore point at
// (or resolve to) the cluster node hosting this member's endpoint — routing
// it through a shared admin address, proxy, or load balancer makes the check
// report the availability of whichever node the URL happens to reach.
func WithLagAwareBaseURL(baseURL string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.baseURL = baseURL }
}

// WithLagAwareRESTAPIPort sets the REST API port (default: 9443).
func WithLagAwareRESTAPIPort(port int) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.restAPIPort = port }
}

// WithLagAwareTolerance sets the lag tolerance in milliseconds (default: 5000).
func WithLagAwareTolerance(toleranceMS int) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.lagTolerance = toleranceMS }
}

// WithLagAwareHTTPClient sets a custom HTTP client.
func WithLagAwareHTTPClient(client HTTPClient) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.httpClient = client }
}

// WithLagAwareBasicAuth sets basic authentication credentials.
func WithLagAwareBasicAuth(username, password string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.username = username; h.password = password }
}

// WithLagAwareTLSConfig sets a custom TLS configuration. The config is cloned so
// later options (and the health check itself) never mutate the caller's value.
// A nil config clears any previously set TLS configuration.
func WithLagAwareTLSConfig(cfg *tls.Config) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		c := cfg.Clone() // Clone(nil) returns nil
		if c != nil {
			// Clone is shallow for RootCAs and shares the Certificates
			// backing array; copy both so later options that append CAs or
			// client certificates cannot reach the caller's values.
			if c.RootCAs != nil {
				c.RootCAs = c.RootCAs.Clone()
			}
			if c.Certificates != nil {
				c.Certificates = append([]tls.Certificate(nil), c.Certificates...)
			}
		}
		h.tlsConfig = c
	}
}

// WithLagAwareInsecureSkipVerify disables TLS certificate verification.
func WithLagAwareInsecureSkipVerify() LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		h.tlsConfig.InsecureSkipVerify = true
	}
}

// WithLagAwareRootCAs sets the root CA certificates for TLS verification.
// If the PEM data cannot be parsed, the error is recorded and subsequent
// health checks fail rather than silently running without the CAs.
func WithLagAwareRootCAs(certPEM []byte) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		if h.tlsConfig.RootCAs == nil {
			h.tlsConfig.RootCAs = x509.NewCertPool()
		}
		if !h.tlsConfig.RootCAs.AppendCertsFromPEM(certPEM) {
			h.setConfigErr(fmt.Errorf("multidb: failed to parse root CA PEM"))
		}
	}
}

// WithLagAwareRootCAsFromFile loads root CA certificates from a PEM file.
// If the file cannot be read or parsed, the error is recorded and subsequent
// health checks fail rather than silently running without the CAs.
func WithLagAwareRootCAsFromFile(caFile string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		certPEM, err := os.ReadFile(caFile)
		if err != nil {
			h.setConfigErr(fmt.Errorf("multidb: failed to read root CA file %q: %w", caFile, err))
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		if h.tlsConfig.RootCAs == nil {
			h.tlsConfig.RootCAs = x509.NewCertPool()
		}
		if !h.tlsConfig.RootCAs.AppendCertsFromPEM(certPEM) {
			h.setConfigErr(fmt.Errorf("multidb: failed to parse root CA PEM from file %q", caFile))
		}
	}
}

// WithLagAwareClientCert sets the client certificate for mutual TLS (mTLS).
// If the key pair cannot be loaded, the error is recorded and subsequent
// health checks fail rather than silently disabling mTLS.
func WithLagAwareClientCert(certPEM, keyPEM []byte) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			h.setConfigErr(fmt.Errorf("multidb: failed to load client certificate: %w", err))
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		h.tlsConfig.Certificates = append(h.tlsConfig.Certificates, cert)
	}
}

// WithLagAwareClientCertFromFiles loads client cert and key from files.
// If the key pair cannot be loaded, the error is recorded and subsequent
// health checks fail rather than silently disabling mTLS.
func WithLagAwareClientCertFromFiles(certFile, keyFile string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			h.setConfigErr(fmt.Errorf("multidb: failed to load client certificate from files: %w", err))
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		h.tlsConfig.Certificates = append(h.tlsConfig.Certificates, cert)
	}
}

// setConfigErr records the first configuration error encountered.
func (h *LagAwareHealthCheck) setConfigErr(err error) {
	if h.configErr == nil {
		h.configErr = err
	}
}

// NewLagAwareHealthCheck creates a new LagAwareHealthCheck.
//
// Generic health check settings (probes, delay, timeout) can be supplied via
// WithLagAwareHealthCheckConfig.
func NewLagAwareHealthCheck(opts ...LagAwareHealthCheckOption) *LagAwareHealthCheck {
	h := &LagAwareHealthCheck{
		config:       DefaultHealthCheckConfig(),
		restAPIPort:  DefaultRESTAPIPort,
		lagTolerance: DefaultLagTolerance,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.httpClient == nil {
		transport, ok := http.DefaultTransport.(*http.Transport)
		if ok {
			transport = transport.Clone()
		} else {
			transport = &http.Transport{}
		}
		if h.tlsConfig != nil {
			// Clone so a caller-supplied tls.Config that is shared or later
			// mutated cannot race with the transport's use of it.
			transport.TLSClientConfig = h.tlsConfig.Clone()
		}
		// Honor a configured probe timeout larger than the default so REST
		// calls are not capped below the probe budget; the request context
		// still bounds each individual call.
		httpTimeout := DefaultHTTPTimeout
		if h.config.Timeout > httpTimeout {
			httpTimeout = h.config.Timeout
		}
		h.httpClient = &http.Client{Timeout: httpTimeout, Transport: transport}
	}
	return h
}

func (h *LagAwareHealthCheck) Config() HealthCheckConfig { return h.config }

// FailbackOnly reports that this check's verdict may only gate routing
// traffic TO a member — candidate probes before failover, auto-fallback and
// initial selection — and must never evict the current active. Replication
// lag is a fail-back criterion (a recovered member must not take traffic
// back before it has caught up); on the member already serving traffic it
// is not an availability signal, and the REST API may itself be unreachable
// precisely when the member is failing. Failover decisions ride on traffic
// signals (the failure detector and circuit breaker) and liveness checks.
// The MultiDB background health loop skips FailbackOnly checks when probing
// the active member; if that leaves no check, it runs the default PING
// instead, so the active always keeps a liveness probe. A member with only
// FailbackOnly checks is therefore PING-gated while active and lag-gated as
// a failover or fallback candidate.
func (h *LagAwareHealthCheck) FailbackOnly() bool { return true }

// hostPortFromAddr is hostFromAddr plus the numeric Redis port (0 when the
// address carries none). The port disambiguates Redis Enterprise databases
// that share a DNS name but listen on different ports.
func hostPortFromAddr(addr string) (string, int, bool) {
	if addr == "" {
		return "", 0, false
	}
	if strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "unix://") {
		return "", 0, false
	}
	if host, portStr, err := net.SplitHostPort(addr); err == nil {
		port, aerr := strconv.Atoi(portStr)
		if aerr != nil {
			// Service-name ports ("redis.example.com:redis") are valid dial
			// targets; resolve them through the local services database so
			// Enterprise endpoints sharing one DNS name still disambiguate
			// by port. An unresolvable name degrades to 0 (host-only match),
			// as before.
			if p, lerr := net.LookupPort("tcp", portStr); lerr == nil {
				port = p
			}
		}
		if host == "" {
			// The ":6379" shorthand means localhost; an empty host would
			// otherwise produce an unusable https://:9443 REST base URL.
			host = "localhost"
		}
		return host, port, true
	}
	// No port present; treat the whole value as the host. Strip any surrounding
	// IPv6 brackets (e.g. "[::1]") so callers can re-bracket the host via
	// net.JoinHostPort without producing a doubly-bracketed "[[::1]]:9443".
	if strings.HasPrefix(addr, "[") && strings.HasSuffix(addr, "]") {
		addr = addr[1 : len(addr)-1]
	}
	return addr, 0, true
}

// CheckHealth performs a single REST API health check probe. It returns
// (false, err) with the error that made the database unhealthy (config error,
// an unusable address, or a REST API failure) so callers can record it.
func (h *LagAwareHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	if h.configErr != nil {
		return false, h.configErr
	}
	host, port, ok := hostPortFromAddr(client.Options().Addr)
	if !ok {
		return false, fmt.Errorf("multidb: cannot derive REST API host from address %q", client.Options().Addr)
	}
	return h.checkLagHealth(ctx, host, port)
}

// CheckClusterHealth performs a single REST API health check probe. The
// cluster's currently discovered shard addresses are tried first, then the
// configured seeds: the cluster can be healthy and routing through live
// nodes while the (possibly stale) startup seeds are unreachable.
func (h *LagAwareHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	if h.configErr != nil {
		return false, h.configErr
	}
	opts := client.Options()

	var mu sync.Mutex
	var addrs []string
	seen := map[string]bool{}
	add := func(addr string) {
		mu.Lock()
		if !seen[addr] {
			seen[addr] = true
			addrs = append(addrs, addr)
		}
		mu.Unlock()
	}
	// Best effort: with no loaded cluster state this errors and the seeds
	// below are the only candidates. ForEachShard runs concurrently, and it
	// may attempt a synchronous topology reload first — bound it to a slice
	// of the probe budget so a hung reload through a dead node cannot spend
	// the whole health-check context before the REST calls run.
	shardCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		shardCtx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Until(deadline)/4))
		defer cancel()
	}
	// Masters only, matching the PING check: MultiDB rejects the
	// replica-routing options so member traffic never reaches replicas, and
	// a replica node's local endpoint availability must not mark a candidate
	// healthy while the master it would actually route to is unavailable or
	// lagging.
	ferr := client.ForEachMaster(shardCtx, func(_ context.Context, master *redis.Client) error {
		add(master.Options().Addr)
		return nil
	})
	// Seeds are the bootstrap fallback only: when the topology could not be
	// enumerated (ForEachMaster errored → no loaded state) OR no masters were
	// found. When masters ARE known, use only them — opts.Addrs may include
	// replica seeds, and a replica passing the lag check must not mark the
	// member healthy while the masters it would route to are down or lagging
	// (matching the masters-only intent above). ForEachMaster is synchronous,
	// so len(addrs) here is the count of unique masters it found.
	if ferr != nil || len(addrs) == 0 {
		for _, addr := range opts.Addrs {
			add(addr)
		}
	}
	if len(addrs) == 0 {
		return false, fmt.Errorf("multidb: cluster client has no addresses")
	}

	var lastErr error
	for _, addr := range addrs {
		host, port, ok := hostPortFromAddr(addr)
		if !ok {
			lastErr = fmt.Errorf("multidb: cannot derive REST API host from address %q", addr)
			continue
		}
		healthy, err := h.checkLagHealth(ctx, host, port)
		if healthy {
			return true, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return false, lastErr
}

func (h *LagAwareHealthCheck) checkLagHealth(ctx context.Context, dbHost string, dbPort int) (bool, error) {
	baseURL := strings.TrimRight(h.baseURL, "/")
	if baseURL == "" {
		// net.JoinHostPort brackets IPv6 literals so the URL is valid
		// (e.g. https://[::1]:9443 rather than https://::1:9443).
		hostPort := net.JoinHostPort(dbHost, strconv.Itoa(h.restAPIPort))
		baseURL = fmt.Sprintf("https://%s", hostPort)
	}
	// fields keeps frequent probes cheap: matching only needs uid and
	// endpoints, not full database configs.
	bdbs, err := h.getBDBs(ctx, fmt.Sprintf("%s/v1/bdbs?fields=uid,endpoints", baseURL))
	if err != nil {
		return false, err
	}
	var uid int
	found := false
	for _, bdb := range bdbs {
		if h.bdbMatchesHost(bdb, dbHost, dbPort) {
			uid = bdb.UID
			found = true
			break
		}
	}
	if !found {
		return false, fmt.Errorf("multidb: no matching bdb found for host %q", dbHost)
	}
	// Endpoint-level, not database-level, availability: without the OSS
	// cluster API the database check reports healthy while ANY endpoint is
	// up, which can mask an outage of the endpoint this member uses. The
	// /v1/local/ form is answered by the node the request reaches (the
	// member's own host unless a custom base URL points elsewhere) and does
	// not redirect to the primary node.
	url := fmt.Sprintf("%s/v1/local/bdbs/%d/endpoint/availability?extend_check=lag&availability_lag_tolerance_ms=%d",
		baseURL, uid, h.lagTolerance)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	if h.username != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	// Drain the (small) body so the keep-alive connection is reusable: this
	// check runs on every health-check tick, and an undrained body forces a
	// fresh TCP/TLS handshake per probe. Bounded in case a proxy misbehaves.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("multidb: availability check returned status %d", resp.StatusCode)
	}
	return true, nil
}

type bdbInfo struct {
	UID       int           `json:"uid"`
	Endpoints []bdbEndpoint `json:"endpoints"`
}

type bdbEndpoint struct {
	DNSName string   `json:"dns_name"`
	Addr    []string `json:"addr"`
	Port    int      `json:"port"`
}

func (h *LagAwareHealthCheck) getBDBs(ctx context.Context, url string) ([]bdbInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if h.username != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Drain the error body too: failing probe loops (401/503) must not
		// burn a TCP/TLS handshake per attempt.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("multidb: REST API returned status %d", resp.StatusCode)
	}
	var bdbs []bdbInfo
	if err := json.NewDecoder(resp.Body).Decode(&bdbs); err != nil {
		// Drain the malformed body too: a misbehaving admin API that keeps
		// answering 200 with undecodable JSON sits in a health-check loop,
		// and an undrained body burns a TCP/TLS handshake per probe.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, err
	}
	// Drain any trailing bytes so the keep-alive connection is reusable.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return bdbs, nil
}

// bdbMatchesHost reports whether a database endpoint matches the client's
// host and, when both sides carry one, its Redis port — several Redis
// Enterprise databases can share a DNS name and differ only by port.
func (h *LagAwareHealthCheck) bdbMatchesHost(bdb bdbInfo, host string, port int) bool {
	hostIP := net.ParseIP(host)
	for _, ep := range bdb.Endpoints {
		if port != 0 && ep.Port != 0 && ep.Port != port {
			continue
		}
		// DNS resolution is case-insensitive, so the configured host must
		// match the REST API's dns_name regardless of case.
		if strings.EqualFold(ep.DNSName, host) {
			return true
		}
		for _, addr := range ep.Addr {
			if addr == host {
				return true
			}
			// IPv6 text is not canonical ("2001:0db8::1" vs "2001:db8::1"):
			// compare parsed IPs so equivalent spellings on the two sides do
			// not report a healthy member as unavailable.
			if hostIP != nil {
				if epIP := net.ParseIP(addr); epIP != nil && epIP.Equal(hostIP) {
					return true
				}
			}
		}
	}
	return false
}
