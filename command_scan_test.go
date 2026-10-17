package redis_test

import (
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
