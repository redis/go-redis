package multidb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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
}

// HTTPClient is an interface for making HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// LagAwareHealthCheckOption is a functional option for LagAwareHealthCheck.
type LagAwareHealthCheckOption func(*LagAwareHealthCheck)

// WithLagAwareBaseURL sets the base URL for the REST API.
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

// WithLagAwareTLSConfig sets a custom TLS configuration.
func WithLagAwareTLSConfig(cfg *tls.Config) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) { h.tlsConfig = cfg }
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
func WithLagAwareRootCAs(certPEM []byte) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		if h.tlsConfig.RootCAs == nil {
			h.tlsConfig.RootCAs = x509.NewCertPool()
		}
		h.tlsConfig.RootCAs.AppendCertsFromPEM(certPEM)
	}
}

// WithLagAwareRootCAsFromFile loads root CA certificates from a PEM file.
func WithLagAwareRootCAsFromFile(caFile string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		certPEM, err := os.ReadFile(caFile)
		if err != nil {
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		if h.tlsConfig.RootCAs == nil {
			h.tlsConfig.RootCAs = x509.NewCertPool()
		}
		h.tlsConfig.RootCAs.AppendCertsFromPEM(certPEM)
	}
}

// WithLagAwareClientCert sets the client certificate for mutual TLS (mTLS).
func WithLagAwareClientCert(certPEM, keyPEM []byte) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		h.tlsConfig.Certificates = append(h.tlsConfig.Certificates, cert)
	}
}

// WithLagAwareClientCertFromFiles loads client cert and key from files.
func WithLagAwareClientCertFromFiles(certFile, keyFile string) LagAwareHealthCheckOption {
	return func(h *LagAwareHealthCheck) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return
		}
		if h.tlsConfig == nil {
			h.tlsConfig = &tls.Config{}
		}
		h.tlsConfig.Certificates = append(h.tlsConfig.Certificates, cert)
	}
}

// NewLagAwareHealthCheck creates a new LagAwareHealthCheck.
func NewLagAwareHealthCheck(opts ...interface{}) *LagAwareHealthCheck {
	h := &LagAwareHealthCheck{
		config:       DefaultHealthCheckConfig(),
		restAPIPort:  DefaultRESTAPIPort,
		lagTolerance: DefaultLagTolerance,
	}
	for _, opt := range opts {
		switch o := opt.(type) {
		case LagAwareHealthCheckOption:
			o(h)
		case HealthCheckOption:
			o(&h.config)
		}
	}
	if h.httpClient == nil {
		transport := &http.Transport{}
		if h.tlsConfig != nil {
			transport.TLSClientConfig = h.tlsConfig
		}
		h.httpClient = &http.Client{Timeout: DefaultHTTPTimeout, Transport: transport}
	}
	return h
}

func (h *LagAwareHealthCheck) Config() HealthCheckConfig { return h.config }

// CheckHealth performs a single REST API health check probe.
func (h *LagAwareHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) bool {
	host := client.Options().Addr
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	return h.checkLagHealth(ctx, host)
}

// CheckClusterHealth performs a single REST API health check probe.
func (h *LagAwareHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) bool {
	opts := client.Options()
	if len(opts.Addrs) == 0 {
		return false
	}
	host := opts.Addrs[0]
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	return h.checkLagHealth(ctx, host)
}

func (h *LagAwareHealthCheck) checkLagHealth(ctx context.Context, dbHost string) bool {
	baseURL := h.baseURL
	if baseURL == "" {
		baseURL = fmt.Sprintf("https://%s:%d", dbHost, h.restAPIPort)
	}
	bdbs, err := h.getBDBs(ctx, fmt.Sprintf("%s/v1/bdbs", baseURL))
	if err != nil {
		return false
	}
	var uid int
	found := false
	for _, bdb := range bdbs {
		if h.bdbMatchesHost(bdb, dbHost) {
			uid = bdb.UID
			found = true
			break
		}
	}
	if !found {
		return false
	}
	url := fmt.Sprintf("%s/v1/bdbs/%d/availability?extend_check=lag&availability_lag_tolerance_ms=%d",
		baseURL, uid, h.lagTolerance)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	if h.username != "" && h.password != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

type bdbInfo struct {
	UID       int           `json:"uid"`
	Endpoints []bdbEndpoint `json:"endpoints"`
}

type bdbEndpoint struct {
	DNSName string   `json:"dns_name"`
	Addr    []string `json:"addr"`
}

func (h *LagAwareHealthCheck) getBDBs(ctx context.Context, url string) ([]bdbInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if h.username != "" && h.password != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API returned status %d", resp.StatusCode)
	}
	var bdbs []bdbInfo
	if err := json.NewDecoder(resp.Body).Decode(&bdbs); err != nil {
		return nil, err
	}
	return bdbs, nil
}

func (h *LagAwareHealthCheck) bdbMatchesHost(bdb bdbInfo, host string) bool {
	for _, ep := range bdb.Endpoints {
		if ep.DNSName == host {
			return true
		}
		for _, addr := range ep.Addr {
			if addr == host {
				return true
			}
		}
	}
	return false
}
