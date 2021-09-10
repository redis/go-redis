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
			o:   &Options{Addr: "localhost:123", TLSConfig: &tls.Config{ /* no deep comparison */ }},
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
			url: "unix:///tmp/redis.sock",
			o:   &Options{Addr: "/tmp/redis.sock"},
		}, {
			url: "unix://foo:bar@/tmp/redis.sock",
			o:   &Options{Addr: "/tmp/redis.sock", Username: "foo", Password: "bar"},
		}, {
			url: "unix://foo:bar@/tmp/redis.sock?db=3",
			o:   &Options{Addr: "/tmp/redis.sock", Username: "foo", Password: "bar", DB: 3},
		}, {
			url: "unix://foo:bar@/tmp/redis.sock?db=test",
			err: errors.New(`redis: invalid database number: strconv.Atoi: parsing "test": invalid syntax`),
		}, {
			url: "redis://localhost/?abc=123",
			err: errors.New("redis: no options supported"),
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
		t.Errorf("got %q, expected %q", actual.DB, expected.DB)
	}
	if actual.TLSConfig == nil && expected.TLSConfig != nil {
		t.Errorf("got nil TLSConfig, expected a TLSConfig")
	}
	if actual.TLSConfig != nil && expected.TLSConfig == nil {
		t.Errorf("got TLSConfig, expected no TLSConfig")
	}
	if actual.Username != expected.Username {
		t.Errorf("got %q, expected %q", actual.Username, expected.Username)
	}
	if actual.Password != expected.Password {
		t.Errorf("got %q, expected %q", actual.Password, expected.Password)
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
