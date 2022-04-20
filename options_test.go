//go:build go1.7
// +build go1.7

package redis

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestParseURL(t *testing.T) {
	certPem := []byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`)
	keyPem := []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`)
	testCert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		t.Fatal(err)
	}

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
			url: "rediss://localhost:123",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ServerName: "localhost"}},
		}, {
			url: "rediss://localhost:123?ServerName=abc&TLSMinVersion=1&TLSMaxVersion=3&TLSInsecureSkipVerify=true",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ServerName: "abc", MinVersion: 1, MaxVersion: 3, InsecureSkipVerify: true}},
		}, {
			url: "rediss://localhost:123?TLSCertPEMFile=./testdata/testcert.pem&TLSKeyPEMFile=./testdata/testkey.pem",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ServerName: "localhost", Certificates: []tls.Certificate{testCert}}},
		}, {
			url: "rediss://localhost:123?TLSCertPEMFile=./testdata/doesnotexist.pem&TLSKeyPEMFile=./testdata/testkey.pem",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ServerName: "abc"}},
			err: errors.New("redis: Error loading X509 Key Pair: open ./testdata/doesnotexist.pem: no such file or directory"),
		}, {
			url: "rediss://localhost:123?TLSCertPEMFile=./testdata/testcert.pem",
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ServerName: "abc"}},
			err: errors.New("redis: TLSCertPEMFile and TLSKeyPEMFile URL parameters must be both set or both omitted"),
		}, {
			url: "rediss://localhost:123?TLSKeyPEMFile=./testdata/testkey.pem",
			err: errors.New("redis: TLSCertPEMFile and TLSKeyPEMFile URL parameters must be both set or both omitted"),
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
			url: "redis://localhost:123/?db=2&idle_timeout=0",
			o:   &Options{Addr: "localhost:123", DB: 2, IdleTimeout: -1},
		}, {
			// negative values disable timeouts as well
			url: "redis://localhost:123/?db=2&idle_timeout=-1",
			o:   &Options{Addr: "localhost:123", DB: 2, IdleTimeout: -1},
		}, {
			// absent timeout values will use defaults
			url: "redis://localhost:123/?db=2&idle_timeout=",
			o:   &Options{Addr: "localhost:123", DB: 2, IdleTimeout: 0},
		}, {
			url: "redis://localhost:123/?db=2&idle_timeout", // missing "=" at the end
			o:   &Options{Addr: "localhost:123", DB: 2, IdleTimeout: 0},
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
	if actual.MinIdleConns != expected.MinIdleConns {
		t.Errorf("MinIdleConns: got %v, expected %v", actual.MinIdleConns, expected.MinIdleConns)
	}
	if actual.MaxConnAge != expected.MaxConnAge {
		t.Errorf("MaxConnAge: got %v, expected %v", actual.MaxConnAge, expected.MaxConnAge)
	}
	if actual.PoolTimeout != expected.PoolTimeout {
		t.Errorf("PoolTimeout: got %v, expected %v", actual.PoolTimeout, expected.PoolTimeout)
	}
	if actual.IdleTimeout != expected.IdleTimeout {
		t.Errorf("IdleTimeout: got %v, expected %v", actual.IdleTimeout, expected.IdleTimeout)
	}
	if actual.IdleCheckFrequency != expected.IdleCheckFrequency {
		t.Errorf("IdleCheckFrequency: got %v, expected %v", actual.IdleCheckFrequency, expected.IdleCheckFrequency)
	}

	if (actual.TLSConfig == nil) != (expected.TLSConfig == nil) {
		t.Errorf("TLSConfig nil: got %v, expected %v", actual.TLSConfig == nil, expected.TLSConfig == nil)
	}

	if (actual.TLSConfig != nil) && (expected.TLSConfig != nil) {
		if actual.TLSConfig.MinVersion != expected.TLSConfig.MinVersion {
			t.Errorf("TLSConfig.MinVersion: got %v, expected %v", actual.TLSConfig.MinVersion, expected.TLSConfig.MinVersion)
		}

		if actual.TLSConfig.MaxVersion != expected.TLSConfig.MaxVersion {
			t.Errorf("TLSConfig.MaxVersion: got %v, expected %v", actual.TLSConfig.MaxVersion, expected.TLSConfig.MaxVersion)
		}

		if actual.TLSConfig.ServerName != expected.TLSConfig.ServerName {
			t.Errorf("TLSConfig.ServerName: got %v, expected %v", actual.TLSConfig.ServerName, expected.TLSConfig.ServerName)
		}

		if actual.TLSConfig.InsecureSkipVerify != expected.TLSConfig.InsecureSkipVerify {
			t.Errorf("TLSConfig.InsecureSkipVerify: got %v, expected %v", actual.TLSConfig.InsecureSkipVerify, expected.TLSConfig.InsecureSkipVerify)
		}

		if len(actual.TLSConfig.Certificates) != len(expected.TLSConfig.Certificates) {
			t.Errorf("TLSConfig.Certificates: got %v, expected %v", actual.TLSConfig.Certificates, expected.TLSConfig.Certificates)
		}

		for i, actualCert := range actual.TLSConfig.Certificates {
			expectedCert := expected.TLSConfig.Certificates[i]
			if !actualCert.Leaf.Equal(expectedCert.Leaf) {
				t.Errorf("TLSConfig.Certificates[%d].Leaf: got %v, expected %v", i, actual.TLSConfig.Certificates, expected.TLSConfig.Certificates)
			}
		}
	}
}

// Test ReadTimeout option initialization, including special values -1 and 0.
// And also test behaviour of WriteTimeout option, when it is not explicitly set and use
// ReadTimeout value.
func TestReadTimeoutOptions(t *testing.T) {
	testDataInputOutputMap := map[time.Duration]time.Duration{
		-1: 0 * time.Second,
		0:  3 * time.Second,
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
