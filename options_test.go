// +build go1.7

package redis

import (
	"errors"
	"testing"
)

func TestParseURL(t *testing.T) {
	cases := []struct {
		u      string
		addr   string
		db     int
		dialer bool
		err    error
	}{
		{
			"redis://localhost:123/1",
			"localhost:123",
			1, false, nil,
		},
		{
			"redis://localhost:123",
			"localhost:123",
			0, false, nil,
		},
		{
			"redis://localhost/1",
			"localhost:6379",
			1, false, nil,
		},
		{
			"redis://12345",
			"12345:6379",
			0, false, nil,
		},
		{
			"rediss://localhost:123",
			"localhost:123",
			0, true, nil,
		},
		{
			"redis://localhost/?abc=123",
			"",
			0, false, errors.New("no options supported"),
		},
		{
			"http://google.com",
			"",
			0, false, errors.New("invalid redis URL scheme: http"),
		},
		{
			"redis://localhost/1/2/3/4",
			"",
			0, false, errors.New("invalid redis URL path: /1/2/3/4"),
		},
		{
			"12345",
			"",
			0, false, errors.New("invalid redis URL scheme: "),
		},
		{
			"redis://localhost/iamadatabase",
			"",
			0, false, errors.New("Invalid redis database number: strconv.ParseInt: parsing \"iamadatabase\": invalid syntax"),
		},
	}

	for _, c := range cases {
		t.Run(c.u, func(t *testing.T) {
			o, err := ParseURL(c.u)
			if c.err == nil && err != nil {
				t.Fatalf("Expected err to be nil, but got: '%q'", err)
				return
			}
			if c.err != nil && err != nil {
				if c.err.Error() != err.Error() {
					t.Fatalf("Expected err to be '%q', but got '%q'", c.err, err)
				}
				return
			}
			if o.Addr != c.addr {
				t.Errorf("Expected Addr to be '%s', but got '%s'", c.addr, o.Addr)
			}
			if o.DB != c.db {
				t.Errorf("Expecdted DB to be '%d', but got '%d'", c.db, o.DB)
			}
			if c.dialer && o.Dialer == nil {
				t.Errorf("Expected a Dialer to be set, but isn't")
			}
		})
	}
}
