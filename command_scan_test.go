package redis_test

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

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
