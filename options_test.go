// +build go1.7

package redis

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseURL(t *testing.T) {
	// It's supposed that path to the redis socket looks like `/tmp/redis.sock`
	// but we have to use existing file just in order do not create a new one
	// during tests. Thus we use `options.go`, file we are testing.
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("unexpected error: %q", err)
		return
	}
	redisSocketPath := filepath.Join(dir, "options.go")

	cases := []struct {
		u    string
		addr string
		db   int
		tls  bool
		err  error
		user string
		pass string
	}{
		{
			"redis://localhost:123/1",
			"localhost:123",
			1, false, nil,
			"", "",
		},
		{
			"redis://localhost:123",
			"localhost:123",
			0, false, nil,
			"", "",
		},
		{
			"redis://localhost/1",
			"localhost:6379",
			1, false, nil,
			"", "",
		},
		{
			"redis://12345",
			"12345:6379",
			0, false, nil,
			"", "",
		},
		{
			"rediss://localhost:123",
			"localhost:123",
			0, true, nil,
			"", "",
		},
		{
			"redis://:bar@localhost:123",
			"localhost:123",
			0, false, nil,
			"", "bar",
		},
		{
			"redis://foo@localhost:123",
			"localhost:123",
			0, false, nil,
			"foo", "",
		},
		{
			"redis://foo:bar@localhost:123",
			"localhost:123",
			0, false, nil,
			"foo", "bar",
		},
		{
			fmt.Sprintf("unix://%s", redisSocketPath),
			redisSocketPath,
			0, false, nil,
			"", "",
		},
		{
			fmt.Sprintf("unix://%s", "/this/file/does/not/exist.oops"),
			"",
			0, false, fmt.Errorf("invalid redis socket path: %s",
				"stat /this/file/does/not/exist.oops: no such file or directory"),
			"", "",
		},
		{
			fmt.Sprintf("unix://foo:bar@%s", redisSocketPath),
			redisSocketPath,
			0, false, nil,
			"foo", "bar",
		},
		{
			fmt.Sprintf("unix://foo:bar@%s?db=3", redisSocketPath),
			redisSocketPath,
			3, false, nil,
			"foo", "bar",
		},
		{
			fmt.Sprintf("unix://foo:bar@%s?db=test", redisSocketPath),
			redisSocketPath,
			0, false, errors.New("redis: invalid database number: strconv.Atoi: parsing \"test\": invalid syntax"),
			"", "",
		},
		{
			"redis://localhost/?abc=123",
			"",
			0, false, errors.New("redis: no options supported"),
			"", "",
		},
		{
			"http://google.com",
			"",
			0, false, errors.New("redis: invalid URL scheme: http"),
			"", "",
		},
		{
			"redis://localhost/1/2/3/4",
			"",
			0, false, errors.New("redis: invalid URL path: /1/2/3/4"),
			"", "",
		},
		{
			"12345",
			"",
			0, false, errors.New("redis: invalid URL scheme: "),
			"", "",
		},
		{
			"redis://localhost/iamadatabase",
			"",
			0, false, errors.New(`redis: invalid database number: "iamadatabase"`),
			"", "",
		},
	}

	for _, c := range cases {
		t.Run(c.u, func(t *testing.T) {
			o, err := ParseURL(c.u)
			if c.err == nil && err != nil {
				t.Fatalf("unexpected error: %q", err)
				return
			}
			if c.err != nil && err != nil {
				if c.err.Error() != err.Error() {
					t.Fatalf("got %q, expected %q", err, c.err)
				}
				return
			}
			if o.Addr != c.addr {
				t.Errorf("got %q, want %q", o.Addr, c.addr)
			}
			if o.DB != c.db {
				t.Errorf("got %q, expected %q", o.DB, c.db)
			}
			if c.tls && o.TLSConfig == nil {
				t.Errorf("got nil TLSConfig, expected a TLSConfig")
			}
			if o.Username != c.user {
				t.Errorf("got %q, expected %q", o.Username, c.user)
			}
			if o.Password != c.pass {
				t.Errorf("got %q, expected %q", o.Password, c.pass)
			}
		})
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
