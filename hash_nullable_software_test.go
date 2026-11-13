package redis

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type nullableResultHook struct {
	observed atomic.Int64
	before   atomic.Bool
}

func (h *nullableResultHook) DialHook(next DialHook) DialHook { return next }
func (h *nullableResultHook) inspect(cmd Cmder) {
	if cmd.GetCmdType() != CmdTypeNullableStringSlice {
		return
	}
	_ = cmd.String()
	if value, err := ExtractCommandValue(cmd); err == nil {
		if _, ok := value.(NullableStringSlice); ok {
			h.observed.Add(1)
		}
	}
}
func (h *nullableResultHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if h.before.Load() {
			h.inspect(cmd)
		}
		err := next(ctx, cmd)
		h.inspect(cmd)
		return err
	}
}
func (h *nullableResultHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		if h.before.Load() {
			for _, cmd := range cmds {
				h.inspect(cmd)
			}
		}
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			h.inspect(cmd)
		}
		return err
	}
}

func nullableSoftwareClient(t *testing.T) (*Client, context.Context) {
	t.Helper()
	uri := os.Getenv("REDIS_TYPED_HMGET_URI")
	if uri == "" {
		t.Skip("set REDIS_TYPED_HMGET_URI to an owned verified Software TLS/RESP3 fixture")
	}
	opt, err := ParseURL(uri)
	if err != nil || opt.TLSConfig == nil || opt.Protocol != 3 {
		t.Fatal("strict TLS/RESP3 fixture required", err)
	}
	pem, err := os.ReadFile(os.Getenv("SSL_CERT_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("invalid test CA")
	}
	opt.TLSConfig.RootCAs = pool
	opt.ContextTimeoutEnabled = true
	opt.MaxRetries = -1
	c := NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return c, ctx
}

func TestHMGetStringsSoftware(t *testing.T) {
	c, ctx := nullableSoftwareClient(t)
	key := fmt.Sprintf("typed-hmget:{owned}:%d", time.Now().UnixNano())
	wrong := key + ":wrong"
	t.Cleanup(func() { _ = c.Del(context.Background(), key, wrong).Err() })
	large := strings.Repeat("x\x00\r\n", 4096)
	if err := c.HSet(ctx, key, "empty", "", "binary", large, "id", "9007199254740993", "utf8", "雪").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, wrong, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	fields := []string{"missing", "empty", "binary", "id", "utf8", "empty"}
	want := NullableStringSlice{Values: []string{"", "", large, "9007199254740993", "雪", ""}, Valid: []bool{false, true, true, true, true, true}}
	hook := &nullableResultHook{}
	hook.before.Store(true)
	c.AddHook(hook)
	check := func(cmd *NullableStringSliceCmd) NullableStringSlice {
		t.Helper()
		got, err := cmd.Result()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("typed values/NULL/duplicate order", err)
		}
		return got
	}
	retained := check(c.HMGetStrings(ctx, key, fields...))
	generic, err := c.HMGet(ctx, key, fields...).Result()
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range generic {
		if value == nil {
			if retained.Valid[i] {
				t.Fatal("NULL changed")
			}
			continue
		}
		if !retained.Valid[i] || value != retained.Values[i] {
			t.Fatal("generic and typed differ")
		}
	}
	for _, transactional := range []bool{false, true} {
		var pipe Pipeliner
		if transactional {
			pipe = c.TxPipeline()
		} else {
			pipe = c.Pipeline()
		}
		good := pipe.HMGetStrings(ctx, key, fields...)
		bad := pipe.HMGetStrings(ctx, wrong, "field")
		missing := pipe.HMGetStrings(ctx, key+":absent", "x", "y")
		ping := pipe.Ping(ctx)
		_, err := pipe.Exec(ctx)
		if err == nil || !isRedisError(err) || bad.Err() == nil {
			t.Fatal("wrong-type error lost", err)
		}
		check(good)
		m, err := missing.Result()
		if err != nil || !reflect.DeepEqual(m.Valid, []bool{false, false}) || !reflect.DeepEqual(m.Values, []string{"", ""}) {
			t.Fatal("missing HASH semantics", err)
		}
		if got, err := ping.Result(); err != nil || got != "PONG" {
			t.Fatal("following command misaligned", got, err)
		}
	}
	for _, fullDuplex := range []bool{false, true} {
		// FullDuplex's documented hook contract forbids result reads before
		// next: its reader can already be writing this command concurrently.
		// Both observations remain covered on the ordinary/half-duplex faces.
		hook.before.Store(!fullDuplex)
		ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 64, FullDuplex: fullDuplex})
		if err != nil {
			t.Fatal(err)
		}
		cmds := make([]*NullableStringSliceCmd, 96)
		for i := range cmds {
			cmds[i] = ap.HMGetStrings(ctx, key, fields...)
		}
		for _, cmd := range cmds {
			check(cmd)
		}
		if err := ap.Close(); err != nil {
			t.Fatal(err)
		}
	}
	hook.before.Store(true)
	if !reflect.DeepEqual(retained, want) || hook.observed.Load() == 0 {
		t.Fatal("retained result or observing hooks changed")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.HMGetStrings(cancelled, key, fields...).Result(); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal("client unusable after cancellation", err)
	}
}
