package redis_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

var (
	errScanFirst  = errors.New("first field")
	errScanSecond = errors.New("second field")
)

type scanFirst string

func (s *scanFirst) ScanRedis(string) error { return errScanFirst }

type scanSecond string

func (s *scanSecond) ScanRedis(string) error { return errScanSecond }

// Several failing fields from HGETALL: every error stays reachable through
// errors.Is, ordered by key, and the result is the same on every call even
// though the reply is a map.
func TestMapStringStringCmdScanJoinsFieldErrors(t *testing.T) {
	type hash struct {
		A scanFirst  `redis:"a"`
		B scanSecond `redis:"b"`
		C string     `redis:"c"`
	}

	val := map[string]string{"a": "x", "b": "y", "c": "3"}
	var first string
	for i := 0; i < 20; i++ {
		var h hash
		err := redis.NewMapStringStringResult(val, nil).Scan(&h)
		if !errors.Is(err, errScanFirst) || !errors.Is(err, errScanSecond) {
			t.Fatalf("got %v, want both field errors", err)
		}
		if !strings.HasPrefix(err.Error(), errScanFirst.Error()) {
			t.Fatalf("got %q, want the error of the smallest key first", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("got %q, want the same error as before: %q", err, first)
		}
		if h.C != "3" {
			t.Fatalf("got C=%q, want 3", h.C)
		}
	}
}

// One failing field from HGETALL: the scanner's error is returned as is.
func TestMapStringStringCmdScanSingleErrorAsIs(t *testing.T) {
	type hash struct {
		A scanFirst `redis:"a"`
		C string    `redis:"c"`
	}
	var h hash
	err := redis.NewMapStringStringResult(map[string]string{"a": "x", "c": "3"}, nil).Scan(&h)
	if err != errScanFirst {
		t.Fatalf("got %v, want the scanner's error unchanged", err)
	}
}

func TestMapStringStringCmdScanBadField(t *testing.T) {
	type hash struct {
		A string `redis:"a"`
		B string `redis:"b"`
		C string `redis:"c"`
		D string `redis:"d"`
		N int    `redis:"n"`
	}

	val := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "n": "abc"}
	for i := 0; i < 20; i++ {
		var h hash
		if err := redis.NewMapStringStringResult(val, nil).Scan(&h); err == nil {
			t.Fatal("expected an error for field n")
		}
		if want := (hash{A: "1", B: "2", C: "3", D: "4"}); h != want {
			t.Fatalf("got %+v, want %+v", h, want)
		}
	}
}

func TestMapStringStringCmdScanBadFieldsSameError(t *testing.T) {
	type hash struct {
		A string `redis:"a"`
		M int    `redis:"m"`
		N int    `redis:"n"`
	}

	val := map[string]string{"a": "1", "m": "xyz", "n": "abc"}
	var first string
	for i := 0; i < 20; i++ {
		var h hash
		err := redis.NewMapStringStringResult(val, nil).Scan(&h)
		if err == nil || !strings.Contains(err.Error(), "hash.M") {
			t.Fatalf("got %v, want the error for field m", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("got %q, want the same error as before: %q", err, first)
		}
		if h.A != "1" {
			t.Fatalf("got A=%q, want 1", h.A)
		}
	}
}
