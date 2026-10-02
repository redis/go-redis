package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

// An FD pipeline whose connection fails after an early Redis error returns the
// transport failure, as an ordinary pipeline does: it tells the caller that
// the later commands have no known result. It used to return the early
// WRONGTYPE.
func TestFDPipelineReturnsTransportErrorOverRedisError(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 && name == "get" {
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", false
		}
		if conn == 0 {
			return "", true // SET executed, then the connection drops
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: -1})

	ctx := context.Background()
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, "k")
	pipe.Set(ctx, "k2", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec err=%v, want the transport failure", err)
	}
	if e := get.Err(); e == nil || !strings.Contains(e.Error(), "WRONGTYPE") {
		t.Fatalf("GET err=%v, want its own WRONGTYPE", e)
	}
}

// A retryable first reply must not re-run the batch when a later reply failed
// with a protocol error, which the engine does not replay because that command
// may already have run. The re-run executed SET twice; an ordinary pipeline
// returns the read error and does not retry.
func TestFDPipelineNoRerunAfterUnreplayedFailure(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 && name == "get" {
			return "-LOADING Redis is loading the dataset in memory\r\n", false
		}
		if conn == 0 && name == "set" {
			return "?malformed\r\n", false // SET executed, then a protocol fault
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Get(ctx, "k")
	pipe.Set(ctx, "k2", "v", 0)
	_, err := pipe.Exec(ctx)
	if n := srv.count("set"); n != 1 {
		t.Fatalf("SET executed %d times, want 1", n)
	}
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec err=%v, want the protocol failure", err)
	}
}
