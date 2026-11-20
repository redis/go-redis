package redis

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseClusterURLTLSMinVersion(t *testing.T) {
	for _, url := range []string{
		"rediss://node-a.example:6379?addr=node-b.example:6379",
		"rediss://127.0.0.1:6379",
		"rediss://[::1]:6379",
	} {
		t.Run(url, func(t *testing.T) {
			opt, err := ParseClusterURL(url)
			if err != nil {
				t.Fatal(err)
			}
			if opt.TLSConfig == nil {
				t.Fatal("expected TLS config for rediss URL")
			}
			if got := opt.TLSConfig.MinVersion; got != tls.VersionTLS12 {
				t.Errorf("MinVersion = %d, want %d", got, tls.VersionTLS12)
			}
			if opt.TLSConfig.InsecureSkipVerify {
				t.Error("certificate verification must remain enabled")
			}
		})
	}

	opt, err := ParseClusterURL("redis://localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	if opt.TLSConfig != nil {
		t.Error("redis URL must not enable TLS")
	}
}

func TestParseClusterURLTLSNodeVerification(t *testing.T) {
	seed := newClusterTLSTestServer(t, "localhost")
	next := newClusterTLSTestServer(t, "127.0.0.1")
	roots := x509.NewCertPool()
	roots.AddCert(seed.Certificate())
	roots.AddCert(next.Certificate())
	_, port, err := net.SplitHostPort(seed.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	url := "rediss://" + net.JoinHostPort("localhost", port) + "?addr=" + next.Listener.Addr().String()

	for _, tc := range []struct {
		name       string
		query      string
		serverName string
		untrusted  bool
	}{
		{name: "per node host", query: "&tls_verify_node_host=true"},
		{name: "explicit server name", serverName: "cluster.example"},
		{name: "wrong server name", serverName: "wrong.example"},
		{name: "untrusted certificate", query: "&tls_verify_node_host=true", untrusted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt, err := ParseClusterURL(url + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			opt.TLSConfig.RootCAs = roots
			if tc.untrusted {
				opt.TLSConfig.RootCAs = x509.NewCertPool()
			}
			if tc.serverName != "" {
				opt.TLSConfig.ServerName = tc.serverName
			}
			originalServerName := opt.TLSConfig.ServerName
			opt.init()

			for _, addr := range opt.Addrs {
				node := newClusterNodeWithNodeAddress(opt, addr, addr, nil)
				defer node.Close()
				conn, err := node.Client.Options().Dialer(context.Background(), "tcp", addr)
				if err == nil {
					conn.Close()
				}
				switch {
				case tc.serverName == "wrong.example":
					var hostErr x509.HostnameError
					if !errors.As(err, &hostErr) {
						t.Errorf("dial %s: got %v, want hostname verification error", addr, err)
					}
				case tc.untrusted:
					var authorityErr x509.UnknownAuthorityError
					if !errors.As(err, &authorityErr) {
						t.Errorf("dial %s: got %v, want untrusted certificate error", addr, err)
					}
				default:
					if err != nil {
						t.Errorf("dial %s: %v", addr, err)
					}
				}
			}
			if opt.TLSConfig.ServerName != originalServerName {
				t.Error("dialing a node mutated the shared TLS config")
			}
		})
	}
}

// Many clusters use one certificate for the cluster DNS name and announce
// their nodes by IP. By default every node is verified against the host of the
// first URL, so a node dialed by IP must still accept that certificate.
func TestParseClusterURLTLSSharedCertificate(t *testing.T) {
	certificate, leaf := newClusterTLSTestCertificate(t, "localhost")
	seed := newClusterTLSTestServerWithCertificate(t, certificate)
	next := newClusterTLSTestServerWithCertificate(t, certificate)
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	_, port, err := net.SplitHostPort(seed.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	nextAddr := next.Listener.Addr().String()
	url := "rediss://" + net.JoinHostPort("localhost", port) + "?addr=" + nextAddr

	for _, tc := range []struct {
		name          string
		query         string
		nextHostError bool
	}{
		{name: "default"},
		{name: "per node host", query: "&tls_verify_node_host=true", nextHostError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt, err := ParseClusterURL(url + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			opt.TLSConfig.RootCAs = roots
			opt.init()

			for _, addr := range opt.Addrs {
				node := newClusterNodeWithNodeAddress(opt, addr, addr, nil)
				defer node.Close()
				conn, err := node.Client.Options().Dialer(context.Background(), "tcp", addr)
				if err == nil {
					conn.Close()
				}
				if tc.nextHostError && addr == nextAddr {
					var hostErr x509.HostnameError
					if !errors.As(err, &hostErr) {
						t.Errorf("dial %s: got %v, want hostname verification error", addr, err)
					}
					continue
				}
				if err != nil {
					t.Errorf("dial %s: %v", addr, err)
				}
			}
		})
	}
}

// Each node certificate covers its own host and a shared DNS name, but not the
// other node's host. No external Redis server is needed.
func newClusterTLSTestServer(t *testing.T, host string) *httptest.Server {
	t.Helper()
	certificate, _ := newClusterTLSTestCertificate(t, "cluster.example", host)
	return newClusterTLSTestServerWithCertificate(t, certificate)
}

// newClusterTLSTestCertificate returns a self-signed certificate that is valid
// only for the given hosts. An IP host becomes an IP SAN, other hosts DNS SANs.
func newClusterTLSTestCertificate(t *testing.T, hosts ...string) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey, Leaf: leaf}, leaf
}

func newClusterTLSTestServerWithCertificate(t *testing.T, certificate tls.Certificate) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	server.Listener.Close()
	server.Listener = listener
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}
