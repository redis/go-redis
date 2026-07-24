package redis

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

func TestParseURL(t *testing.T) {
	cases := []struct {
		url string
		o   *Options // expected value
		err error
	}{
		{
			url: "redis://localhost:123/1",
			o:   &Options{Addr: "localhost:123", DB: 1},
		}, {
			url: "redis://localhost:123",
			o:   &Options{Addr: "localhost:123"},
		}, {
			url: "redis://localhost/1",
			o:   &Options{Addr: "localhost:6379", DB: 1},
		}, {
			url: "redis://12345",
			o:   &Options{Addr: "12345:6379"},
		}, {
			// IPv6 literal without a port keeps a single pair of brackets
			url: "redis://[::1]",
			o:   &Options{Addr: "[::1]:6379"},
		}, {
			// IPv6 literal with a port
			url: "redis://[::1]:6380",
			o:   &Options{Addr: "[::1]:6380"},
		}, {
			// IPv6 literal without a port, with a db number
			url: "redis://[2001:db8::1]/2",
			o:   &Options{Addr: "[2001:db8::1]:6379", DB: 2},
		}, {
			url: "rediss://localhost:123",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ /* no deep comparison */ }},
		}, {
			url: "rediss://localhost:123/?skip_verify=true",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{InsecureSkipVerify: true}},
		}, {
			url: "redis://:bar@localhost:123",
			o:   &Options{Addr: "localhost:123", Password: "bar"},
		}, {
			url: "redis://foo@localhost:123",
			o:   &Options{Addr: "localhost:123", Username: "foo"},
		}, {
			url: "redis://foo:bar@localhost:123",
			o:   &Options{Addr: "localhost:123", Username: "foo", Password: "bar"},
		}, {
			// multiple params
			url: "redis://localhost:123/?db=2&read_timeout=2&pool_fifo=true",
			o:   &Options{Addr: "localhost:123", DB: 2, ReadTimeout: 2 * time.Second, PoolFIFO: true},
		}, {
			// special case handling for disabled timeouts
			url: "redis://localhost:123/?db=2&conn_max_idle_time=0",
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: -1},
		}, {
			// negative values disable timeouts as well
			url: "redis://localhost:123/?db=2&conn_max_idle_time=-1",
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: -1},
		}, {
			// a zero or negative duration written with a unit disables the timeout,
			// the same as the plain "0" / "-1" forms above
			url: "redis://localhost:123/?db=2&conn_max_idle_time=0s",
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: -1},
		}, {
			url: "redis://localhost:123/?db=2&conn_max_idle_time=-1s",
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: -1},
		}, {
			// absent timeout values will use defaults
			url: "redis://localhost:123/?db=2&conn_max_idle_time=",
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: 0},
		}, {
			url: "redis://localhost:123/?db=2&conn_max_idle_time", // missing "=" at the end
			o:   &Options{Addr: "localhost:123", DB: 2, ConnMaxIdleTime: 0},
		}, {
			url: "redis://localhost:123/?db=2&client_name=hi", // client name
			o:   &Options{Addr: "localhost:123", DB: 2, ClientName: "hi"},
		}, {
			url: "redis://localhost:123/?db=2&protocol=2", // RESP Protocol
			o:   &Options{Addr: "localhost:123", DB: 2, Protocol: 2},
		}, {
			url: "redis://localhost:123/?max_concurrent_dials=5", // MaxConcurrentDials parameter
			o:   &Options{Addr: "localhost:123", MaxConcurrentDials: 5},
		}, {
			url: "redis://localhost:123/?max_concurrent_dials=0", // MaxConcurrentDials zero value
			o:   &Options{Addr: "localhost:123", MaxConcurrentDials: 0},
		}, {
			url: "redis://localhost:123/?conn_max_lifetime=1h&conn_max_lifetime_jitter=6m",
			o:   &Options{Addr: "localhost:123", ConnMaxLifetime: time.Hour, ConnMaxLifetimeJitter: 6 * time.Minute},
		}, {
			// jitter > lifetime should be capped
			url: "redis://localhost:123/?conn_max_lifetime=30m&conn_max_lifetime_jitter=1h",
			o:   &Options{Addr: "localhost:123", ConnMaxLifetime: 30 * time.Minute, ConnMaxLifetimeJitter: 30 * time.Minute},
		}, {
			// jitter without lifetime should be capped to 0
			url: "redis://localhost:123/?conn_max_lifetime_jitter=6m",
			o:   &Options{Addr: "localhost:123", ConnMaxLifetimeJitter: 0},
		}, {
			url: "unix:///tmp/redis.sock",
			o:   &Options{Addr: "/tmp/redis.sock"},
		}, {
			url: "unix://foo:bar@/tmp/redis.sock",
			o:   &Options{Addr: "/tmp/redis.sock", Username: "foo", Password: "bar"},
		}, {
			url: "unix://foo:bar@/tmp/redis.sock?db=3",
			o:   &Options{Addr: "/tmp/redis.sock", Username: "foo", Password: "bar", DB: 3},
		}, {
			// invalid db format
			url: "unix://foo:bar@/tmp/redis.sock?db=test",
			err: errors.New(`redis: invalid database number: strconv.Atoi: parsing "test": invalid syntax`),
		}, {
			// invalid int value
			url: "redis://localhost/?pool_size=five",
			err: errors.New(`redis: invalid pool_size number: strconv.Atoi: parsing "five": invalid syntax`),
		}, {
			// invalid bool value
			url: "redis://localhost/?pool_fifo=yes",
			err: errors.New(`redis: invalid pool_fifo boolean: expected true/false/1/0 or an empty string, got "yes"`),
		}, {
			// it returns first error
			url: "redis://localhost/?db=foo&pool_size=five",
			err: errors.New(`redis: invalid database number: strconv.Atoi: parsing "foo": invalid syntax`),
		}, {
			url: "redis://localhost/?abc=123",
			err: errors.New("redis: unexpected option: abc"),
		}, {
			url: "redis://foo@localhost/?username=bar",
			err: errors.New("redis: unexpected option: username"),
		}, {
			url: "redis://localhost/?wrte_timout=10s&abc=123",
			err: errors.New("redis: unexpected option: abc, wrte_timout"),
		}, {
			url: "http://google.com",
			err: errors.New("redis: invalid URL scheme: http"),
		}, {
			url: "redis://localhost/1/2/3/4",
			err: errors.New("redis: invalid URL path: /1/2/3/4"),
		}, {
			url: "12345",
			err: errors.New("redis: invalid URL scheme: "),
		}, {
			url: "redis://localhost/iamadatabase",
			err: errors.New(`redis: invalid database number: "iamadatabase"`),
		},
	}

	for i := range cases {
		tc := cases[i]
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()

			actual, err := ParseURL(tc.url)
			if tc.err == nil && err != nil {
				t.Fatalf("unexpected error: %q", err)
				return
			}
			if tc.err != nil && err != nil {
				if tc.err.Error() != err.Error() {
					t.Fatalf("got %q, expected %q", err, tc.err)
				}
				return
			}
			comprareOptions(t, actual, tc.o)
		})
	}
}

func TestParseURLIPv6TLSServerName(t *testing.T) {
	// For rediss:// the TLS SNI ServerName must be the bare IPv6 host, not the
	// bracketed form, otherwise the handshake is attempted with an invalid
	// server name.
	o, err := ParseURL("rediss://[2001:db8::1]")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.Addr != "[2001:db8::1]:6379" {
		t.Errorf("Addr: got %q, want %q", o.Addr, "[2001:db8::1]:6379")
	}
	if o.TLSConfig == nil {
		t.Fatal("expected a TLSConfig for the rediss scheme")
	}
	if got, want := o.TLSConfig.ServerName, "2001:db8::1"; got != want {
		t.Errorf("TLSConfig.ServerName: got %q, want %q", got, want)
	}
}

// writeTempTLSMaterial generates a self-signed ECDSA cert/key pair and CA PEM
// under t.TempDir(). No network is involved; material is only used by ParseURL.
func writeTempTLSMaterial(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"go-redis test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")

	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	if err := certOut.Close(); err != nil {
		t.Fatalf("close cert: %v", err)
	}

	// CA file is the same self-signed cert (acts as trust root).
	if err := os.WriteFile(caFile, mustRead(t, certFile), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatalf("close key: %v", err)
	}

	return certFile, keyFile, caFile
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func TestParseURLTLSOptions(t *testing.T) {
	certFile, keyFile, caFile := writeTempTLSMaterial(t)

	t.Run("rediss skip_verify", func(t *testing.T) {
		o, err := ParseURL("rediss://localhost:123/?skip_verify=true")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig")
		}
		if !o.TLSConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=true")
		}
		if o.TLSConfig.ServerName != "localhost" {
			t.Errorf("ServerName: got %q, want localhost", o.TLSConfig.ServerName)
		}
	})

	t.Run("rediss tls_insecure_skip_verify", func(t *testing.T) {
		o, err := ParseURL("rediss://localhost:123/?tls_insecure_skip_verify=true")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil || !o.TLSConfig.InsecureSkipVerify {
			t.Fatal("expected TLSConfig with InsecureSkipVerify=true")
		}
	})

	t.Run("client cert and key on rediss", func(t *testing.T) {
		u := "rediss://localhost:123/?tls_cert_file=" + url.QueryEscape(certFile) +
			"&tls_key_file=" + url.QueryEscape(keyFile)
		o, err := ParseURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig")
		}
		if len(o.TLSConfig.Certificates) != 1 {
			t.Fatalf("Certificates: got %d, want 1", len(o.TLSConfig.Certificates))
		}
		if o.TLSConfig.ServerName != "localhost" {
			t.Errorf("ServerName: got %q, want localhost", o.TLSConfig.ServerName)
		}
	})

	t.Run("ca file on rediss", func(t *testing.T) {
		u := "rediss://localhost:123/?tls_ca_file=" + url.QueryEscape(caFile)
		o, err := ParseURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil || o.TLSConfig.RootCAs == nil {
			t.Fatal("expected TLSConfig with RootCAs set")
		}
	})

	t.Run("all tls files on rediss", func(t *testing.T) {
		u := "rediss://localhost:123/?tls_cert_file=" + url.QueryEscape(certFile) +
			"&tls_key_file=" + url.QueryEscape(keyFile) +
			"&tls_ca_file=" + url.QueryEscape(caFile) +
			"&tls_insecure_skip_verify=false"
		o, err := ParseURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig")
		}
		if len(o.TLSConfig.Certificates) != 1 {
			t.Fatalf("Certificates: got %d, want 1", len(o.TLSConfig.Certificates))
		}
		if o.TLSConfig.RootCAs == nil {
			t.Fatal("expected RootCAs")
		}
		if o.TLSConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=false")
		}
	})

	t.Run("tls files enable TLS on redis scheme", func(t *testing.T) {
		// Providing cert paths on redis:// (not rediss://) should still build a TLSConfig.
		u := "redis://localhost:123/?tls_cert_file=" + url.QueryEscape(certFile) +
			"&tls_key_file=" + url.QueryEscape(keyFile)
		o, err := ParseURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig when cert files are provided")
		}
		if o.TLSConfig.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion: got %d, want TLS 1.2", o.TLSConfig.MinVersion)
		}
		if o.TLSConfig.ServerName != "localhost" {
			t.Errorf("ServerName: got %q, want localhost", o.TLSConfig.ServerName)
		}
		if len(o.TLSConfig.Certificates) != 1 {
			t.Fatalf("Certificates: got %d, want 1", len(o.TLSConfig.Certificates))
		}
	})

	t.Run("tls_insecure_skip_verify enables TLS on redis scheme", func(t *testing.T) {
		o, err := ParseURL("redis://localhost:123/?tls_insecure_skip_verify=true")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil || !o.TLSConfig.InsecureSkipVerify {
			t.Fatal("expected TLSConfig with InsecureSkipVerify=true")
		}
	})

	t.Run("cert without key", func(t *testing.T) {
		u := "rediss://localhost:123/?tls_cert_file=" + url.QueryEscape(certFile)
		_, err := ParseURL(u)
		if err == nil {
			t.Fatal("expected error when only tls_cert_file is set")
		}
		if !strings.Contains(err.Error(), "tls_cert_file and tls_key_file") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("key without cert", func(t *testing.T) {
		u := "rediss://localhost:123/?tls_key_file=" + url.QueryEscape(keyFile)
		_, err := ParseURL(u)
		if err == nil {
			t.Fatal("expected error when only tls_key_file is set")
		}
		if !strings.Contains(err.Error(), "tls_cert_file and tls_key_file") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing cert file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-cert.pem")
		u := "rediss://localhost:123/?tls_cert_file=" + url.QueryEscape(missing) +
			"&tls_key_file=" + url.QueryEscape(keyFile)
		_, err := ParseURL(u)
		if err == nil {
			t.Fatal("expected error for missing cert file")
		}
		if !strings.Contains(err.Error(), "error loading TLS certificate") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing ca file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-ca.pem")
		u := "rediss://localhost:123/?tls_ca_file=" + url.QueryEscape(missing)
		_, err := ParseURL(u)
		if err == nil {
			t.Fatal("expected error for missing ca file")
		}
		if !strings.Contains(err.Error(), "error loading TLS CA certificate") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid ca pem", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "bad-ca.pem")
		if err := os.WriteFile(bad, []byte("not a pem"), 0o600); err != nil {
			t.Fatal(err)
		}
		u := "rediss://localhost:123/?tls_ca_file=" + url.QueryEscape(bad)
		_, err := ParseURL(u)
		if err == nil {
			t.Fatal("expected error for invalid CA PEM")
		}
		if !strings.Contains(err.Error(), "failed to parse TLS CA certificate") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("cluster url with tls options", func(t *testing.T) {
		u := "rediss://localhost:123?tls_cert_file=" + url.QueryEscape(certFile) +
			"&tls_key_file=" + url.QueryEscape(keyFile) +
			"&skip_verify=true"
		o, err := ParseClusterURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig")
		}
		if len(o.TLSConfig.Certificates) != 1 {
			t.Fatalf("Certificates: got %d, want 1", len(o.TLSConfig.Certificates))
		}
		if !o.TLSConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=true")
		}
	})

	t.Run("failover url with tls options", func(t *testing.T) {
		u := "rediss://localhost:6379/5?master_name=test&tls_ca_file=" + url.QueryEscape(caFile) +
			"&tls_insecure_skip_verify=true"
		o, err := ParseFailoverURL(u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.TLSConfig == nil {
			t.Fatal("expected TLSConfig")
		}
		if o.TLSConfig.RootCAs == nil {
			t.Fatal("expected RootCAs")
		}
		if !o.TLSConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=true")
		}
	})
}

func comprareOptions(t *testing.T, actual, expected *Options) {
	t.Helper()

	if actual.Addr != expected.Addr {
		t.Errorf("got %q, want %q", actual.Addr, expected.Addr)
	}
	if actual.DB != expected.DB {
		t.Errorf("DB: got %q, expected %q", actual.DB, expected.DB)
	}
	if actual.TLSConfig == nil && expected.TLSConfig != nil {
		t.Errorf("got nil TLSConfig, expected a TLSConfig")
	}
	if actual.TLSConfig != nil && expected.TLSConfig == nil {
		t.Errorf("got TLSConfig, expected no TLSConfig")
	}
	if actual.Username != expected.Username {
		t.Errorf("Username: got %q, expected %q", actual.Username, expected.Username)
	}
	if actual.Password != expected.Password {
		t.Errorf("Password: got %q, expected %q", actual.Password, expected.Password)
	}
	if actual.MaxRetries != expected.MaxRetries {
		t.Errorf("MaxRetries: got %v, expected %v", actual.MaxRetries, expected.MaxRetries)
	}
	if actual.MinRetryBackoff != expected.MinRetryBackoff {
		t.Errorf("MinRetryBackoff: got %v, expected %v", actual.MinRetryBackoff, expected.MinRetryBackoff)
	}
	if actual.MaxRetryBackoff != expected.MaxRetryBackoff {
		t.Errorf("MaxRetryBackoff: got %v, expected %v", actual.MaxRetryBackoff, expected.MaxRetryBackoff)
	}
	if actual.DialTimeout != expected.DialTimeout {
		t.Errorf("DialTimeout: got %v, expected %v", actual.DialTimeout, expected.DialTimeout)
	}
	if actual.ReadTimeout != expected.ReadTimeout {
		t.Errorf("ReadTimeout: got %v, expected %v", actual.ReadTimeout, expected.ReadTimeout)
	}
	if actual.WriteTimeout != expected.WriteTimeout {
		t.Errorf("WriteTimeout: got %v, expected %v", actual.WriteTimeout, expected.WriteTimeout)
	}
	if actual.PoolFIFO != expected.PoolFIFO {
		t.Errorf("PoolFIFO: got %v, expected %v", actual.PoolFIFO, expected.PoolFIFO)
	}
	if actual.PoolSize != expected.PoolSize {
		t.Errorf("PoolSize: got %v, expected %v", actual.PoolSize, expected.PoolSize)
	}
	if actual.PoolTimeout != expected.PoolTimeout {
		t.Errorf("PoolTimeout: got %v, expected %v", actual.PoolTimeout, expected.PoolTimeout)
	}
	if actual.MinIdleConns != expected.MinIdleConns {
		t.Errorf("MinIdleConns: got %v, expected %v", actual.MinIdleConns, expected.MinIdleConns)
	}
	if actual.MaxIdleConns != expected.MaxIdleConns {
		t.Errorf("MaxIdleConns: got %v, expected %v", actual.MaxIdleConns, expected.MaxIdleConns)
	}
	if actual.ConnMaxIdleTime != expected.ConnMaxIdleTime {
		t.Errorf("ConnMaxIdleTime: got %v, expected %v", actual.ConnMaxIdleTime, expected.ConnMaxIdleTime)
	}
	if actual.ConnMaxLifetime != expected.ConnMaxLifetime {
		t.Errorf("ConnMaxLifetime: got %v, expected %v", actual.ConnMaxLifetime, expected.ConnMaxLifetime)
	}
	if actual.ConnMaxLifetimeJitter != expected.ConnMaxLifetimeJitter {
		t.Errorf("ConnMaxLifetimeJitter: got %v, expected %v", actual.ConnMaxLifetimeJitter, expected.ConnMaxLifetimeJitter)
	}
	if actual.MaxConcurrentDials != expected.MaxConcurrentDials {
		t.Errorf("MaxConcurrentDials: got %v, expected %v", actual.MaxConcurrentDials, expected.MaxConcurrentDials)
	}
}

// Test ReadTimeout option initialization, including special values -1 and 0.
// And also test behaviour of WriteTimeout option, when it is not explicitly set and use
// ReadTimeout value.
func TestReadTimeoutOptions(t *testing.T) {
	testDataInputOutputMap := map[time.Duration]time.Duration{
		-1: 0 * time.Second,
		0:  5 * time.Second,
		1:  1 * time.Nanosecond,
		3:  3 * time.Nanosecond,
	}

	for in, out := range testDataInputOutputMap {
		o := &Options{ReadTimeout: in}
		o.init()
		if o.ReadTimeout != out {
			t.Errorf("got %d instead of %d as ReadTimeout option", o.ReadTimeout, out)
		}

		if o.WriteTimeout != o.ReadTimeout {
			t.Errorf("got %d instead of %d as WriteTimeout option", o.WriteTimeout, o.ReadTimeout)
		}
	}
}

// Pin the retry backoff defaults shared by Options, ClusterOptions and
// RingOptions, including the -1 escape hatch.
func TestRetryBackoffOptions(t *testing.T) {
	check := func(t *testing.T, kind string, min, max time.Duration) {
		t.Helper()
		if min != 10*time.Millisecond {
			t.Errorf("%s: got %s as default MinRetryBackoff, want 10ms", kind, min)
		}
		if max != time.Second {
			t.Errorf("%s: got %s as default MaxRetryBackoff, want 1s", kind, max)
		}
	}

	o := &Options{}
	o.init()
	check(t, "Options", o.MinRetryBackoff, o.MaxRetryBackoff)

	co := &ClusterOptions{}
	co.init()
	check(t, "ClusterOptions", co.MinRetryBackoff, co.MaxRetryBackoff)

	ro := &RingOptions{}
	ro.init()
	check(t, "RingOptions", ro.MinRetryBackoff, ro.MaxRetryBackoff)

	disabled := &Options{MinRetryBackoff: -1, MaxRetryBackoff: -1}
	disabled.init()
	if disabled.MinRetryBackoff != 0 || disabled.MaxRetryBackoff != 0 {
		t.Errorf("-1 must disable backoff, got min=%s max=%s",
			disabled.MinRetryBackoff, disabled.MaxRetryBackoff)
	}
}

func TestClusterStateReloadIntervalOption(t *testing.T) {
	o := &ClusterOptions{}
	o.init()
	if o.ClusterStateReloadInterval != 60*time.Second {
		t.Errorf("got %s as default ClusterStateReloadInterval, want 60s",
			o.ClusterStateReloadInterval)
	}

	o = &ClusterOptions{ClusterStateReloadInterval: 5 * time.Minute}
	o.init()
	if o.ClusterStateReloadInterval != 5*time.Minute {
		t.Errorf("explicit ClusterStateReloadInterval overridden: got %s",
			o.ClusterStateReloadInterval)
	}
}

// The keep-alive policy is shared by the default dialer (options.go) and the
// sentinel master/replica dialer (sentinel.go); pin it so a change shows up
// in review instead of drifting silently.
func TestDefaultKeepAliveConfig(t *testing.T) {
	want := net.KeepAliveConfig{
		Enable:   true,
		Idle:     30 * time.Second,
		Interval: 5 * time.Second,
		Count:    3,
	}
	if defaultKeepAliveConfig != want {
		t.Errorf("defaultKeepAliveConfig = %+v, want %+v", defaultKeepAliveConfig, want)
	}
}

func TestProtocolOptions(t *testing.T) {
	testCasesMap := map[int]int{
		0: 3,
		1: 3,
		2: 2,
		3: 3,
	}

	o := &Options{}
	o.init()
	if o.Protocol != 3 {
		t.Errorf("got %d instead of %d as protocol option", o.Protocol, 3)
	}

	for set, want := range testCasesMap {
		o := &Options{Protocol: set}
		o.init()
		if o.Protocol != want {
			t.Errorf("got %d instead of %d as protocol option", o.Protocol, want)
		}
	}
}

func TestMaxConcurrentDialsOptions(t *testing.T) {
	// Test cases for MaxConcurrentDials initialization logic
	testCases := []struct {
		name                    string
		poolSize                int
		maxConcurrentDials      int
		expectedConcurrentDials int
	}{
		// Edge cases and invalid values - negative/zero values set to PoolSize
		{
			name:                    "negative value gets set to pool size",
			poolSize:                10,
			maxConcurrentDials:      -1,
			expectedConcurrentDials: 10, // negative values are set to PoolSize
		},
		// Zero value tests - MaxConcurrentDials should be set to PoolSize
		{
			name:                    "zero value with positive pool size",
			poolSize:                1,
			maxConcurrentDials:      0,
			expectedConcurrentDials: 1, // MaxConcurrentDials = PoolSize when 0
		},
		// Explicit positive value tests
		{
			name:                    "explicit value within limit",
			poolSize:                10,
			maxConcurrentDials:      3,
			expectedConcurrentDials: 3, // should remain unchanged when < PoolSize
		},
		// Capping tests - values exceeding PoolSize should be capped
		{
			name:                    "value exceeding pool size",
			poolSize:                5,
			maxConcurrentDials:      10,
			expectedConcurrentDials: 5, // should be capped at PoolSize
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := &Options{
				PoolSize:           tc.poolSize,
				MaxConcurrentDials: tc.maxConcurrentDials,
			}
			opts.init()

			if opts.MaxConcurrentDials != tc.expectedConcurrentDials {
				t.Errorf("MaxConcurrentDials: got %v, expected %v (PoolSize=%v)",
					opts.MaxConcurrentDials, tc.expectedConcurrentDials, opts.PoolSize)
			}

			// Ensure MaxConcurrentDials never exceeds PoolSize (for all inputs)
			if opts.MaxConcurrentDials > opts.PoolSize {
				t.Errorf("MaxConcurrentDials (%v) should not exceed PoolSize (%v)",
					opts.MaxConcurrentDials, opts.PoolSize)
			}

			// Ensure MaxConcurrentDials is always positive (for all inputs)
			if opts.MaxConcurrentDials <= 0 {
				t.Errorf("MaxConcurrentDials should be positive, got %v", opts.MaxConcurrentDials)
			}
		})
	}
}

func TestClusterOptionsDialerRetries(t *testing.T) {
	clusterOpt := &ClusterOptions{
		DialerRetries:      10,
		DialerRetryTimeout: 200 * time.Millisecond,
	}

	opt := clusterOpt.clientOptions()

	if opt.DialerRetries != 10 {
		t.Errorf("expected DialerRetries=10, got %d", opt.DialerRetries)
	}
	if opt.DialerRetryTimeout != 200*time.Millisecond {
		t.Errorf("expected DialerRetryTimeout=200ms, got %v", opt.DialerRetryTimeout)
	}
}

func TestRingOptionsDialerRetries(t *testing.T) {
	ringOpt := &RingOptions{
		DialerRetries:      10,
		DialerRetryTimeout: 200 * time.Millisecond,
	}

	opt := ringOpt.clientOptions()

	if opt.DialerRetries != 10 {
		t.Errorf("expected DialerRetries=10, got %d", opt.DialerRetries)
	}
	if opt.DialerRetryTimeout != 200*time.Millisecond {
		t.Errorf("expected DialerRetryTimeout=200ms, got %v", opt.DialerRetryTimeout)
	}
}

func TestFailoverOptionsDialerRetries(t *testing.T) {
	failoverOpt := &FailoverOptions{
		DialerRetries:      10,
		DialerRetryTimeout: 200 * time.Millisecond,
	}

	opt := failoverOpt.clientOptions()

	if opt.DialerRetries != 10 {
		t.Errorf("expected DialerRetries=10, got %d", opt.DialerRetries)
	}
	if opt.DialerRetryTimeout != 200*time.Millisecond {
		t.Errorf("expected DialerRetryTimeout=200ms, got %v", opt.DialerRetryTimeout)
	}

	// Also verify sentinelOptions passes them through
	sentinelOpt := failoverOpt.sentinelOptions("localhost:26379")
	if sentinelOpt.DialerRetries != 10 {
		t.Errorf("expected sentinel DialerRetries=10, got %d", sentinelOpt.DialerRetries)
	}
	if sentinelOpt.DialerRetryTimeout != 200*time.Millisecond {
		t.Errorf("expected sentinel DialerRetryTimeout=200ms, got %v", sentinelOpt.DialerRetryTimeout)
	}
}

// TestOptionsCloneMaintNotificationsRace verifies that cloning options via
// baseClient is safe when initConn concurrently writes MaintNotificationsConfig.Mode.
// Run with -race to detect the data race.
func TestOptionsCloneMaintNotificationsRace(t *testing.T) {
	opt := &Options{
		Addr: "localhost:6379",
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeAuto,
		},
	}

	bc := baseClient{opt: opt}

	var wg sync.WaitGroup
	const iterations = 1000

	// Writer: simulates initConn toggling MaintNotificationsConfig.Mode under optLock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			bc.optLock.Lock()
			bc.opt.MaintNotificationsConfig.Mode = maintnotifications.ModeDisabled
			bc.optLock.Unlock()

			bc.optLock.Lock()
			bc.opt.MaintNotificationsConfig.Mode = maintnotifications.ModeAuto
			bc.optLock.Unlock()
		}
	}()

	// Reader: simulates newTx / withTimeout calling cloneOpt() (acquires RLock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			cloned := bc.cloneOpt()
			_ = cloned
		}
	}()

	wg.Wait()
}
