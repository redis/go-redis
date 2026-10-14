package redis

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// warmSpareClient returns a client whose dialer counts dials and, once slow is
// set, takes slowDial per dial.
func warmSpareClient(t *testing.T, minIdle int, slow *atomic.Bool) *Client {
	t.Helper()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client := NewClient(&Options{
		Addr:                 internalTestRedisAddr(),
		PipelineMinIdleConns: minIdle,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if slow.Load() {
				time.Sleep(slowDial)
			}
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	return client
}

const slowDial = 500 * time.Millisecond

// waitFDSession waits until the engine holds a session connection and the
// pipeline pool has want idle connections whose dials have finished. IdleLen
// counts a min-idle connection as soon as its dial starts; Len only once it is
// in the pool, so Len must also reach the held connection plus the idle ones.
func waitFDSession(t *testing.T, ap *AutoPipeliner, client *Client, want int) {
	t.Helper()
	pp := client.loadPipelinePool().pool
	deadline := time.Now().Add(5 * time.Second)
	for ap.fd.curConn.Load() == nil || pp.IdleLen() != want || pp.Len() != want+1 {
		if time.Now().After(deadline) {
			t.Fatalf("session held=%v, pipeline Len=%d IdleLen=%d, want held with %d idle",
				ap.fd.curConn.Load() != nil, pp.Len(), pp.IdleLen(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFDWarmSpareReplacesWithoutDial checks the full-duplex engine keeps an
// idle spare in the pipeline pool, so after its connection fails the
// replacement does not wait for a dial.
func TestFDWarmSpareReplacesWithoutDial(t *testing.T) {
	ctx := context.Background()
	var slow atomic.Bool
	client := warmSpareClient(t, 0, &slow)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Skip("full-duplex engine not enabled for this client")
	}
	if err := ap.Set(ctx, "fdspare:k", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	waitFDSession(t, ap, client, 1)

	// From now on a dial is slow. Break the held connection: the next command
	// fails its write and is replayed on the replacement.
	slow.Store(true)
	_ = ap.fd.curConn.Load().Close()

	start := time.Now()
	if err := ap.Set(ctx, "fdspare:k", "2", 0).Err(); err != nil {
		t.Fatalf("command after connection failure: %v", err)
	}
	if d := time.Since(start); d >= slowDial/2 {
		t.Fatalf("recovery took %v; the replacement dialed instead of leasing the warm spare", d)
	}
	if v, err := client.Get(ctx, "fdspare:k").Result(); err != nil || v != "2" {
		t.Fatalf("get = %q, %v; want \"2\"", v, err)
	}
}

// TestFDWarmSpareOptOut checks a negative PipelineMinIdleConns keeps the
// pipeline pool at zero idle connections under full-duplex.
func TestFDWarmSpareOptOut(t *testing.T) {
	ctx := context.Background()
	var slow atomic.Bool
	client := warmSpareClient(t, -1, &slow)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Skip("full-duplex engine not enabled for this client")
	}
	if err := ap.Set(ctx, "fdspare:o", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	waitFDSession(t, ap, client, 0)
	time.Sleep(50 * time.Millisecond)
	if n := client.loadPipelinePool().pool.IdleLen(); n != 0 {
		t.Fatalf("pipeline IdleLen = %d with PipelineMinIdleConns -1, want 0", n)
	}
}

// TestFDSpareFits checks the spare is only added when it does not take a slot
// an engine needs and the idle limit can hold it next to a returned session
// connection (Codex on #4101).
func TestFDSpareFits(t *testing.T) {
	for _, tc := range []struct {
		poolSize, maxIdle, engines int
		want                       bool
	}{
		{10, 0, 1, true},  // defaults
		{1, 0, 1, false},  // the only slot belongs to the engine
		{2, 0, 2, false},  // NumShards == PipelinePoolSize
		{3, 0, 2, true},   // room for two engines and a spare
		{10, 1, 1, false}, // spare would fill the only idle slot
		{10, 2, 1, true},  // spare + returned session conn
		{10, 2, 2, false}, // two engines need both idle slots
	} {
		if got := fdSpareFits(tc.poolSize, tc.maxIdle, tc.engines); got != tc.want {
			t.Errorf("fdSpareFits(pool=%d, maxIdle=%d, engines=%d) = %v, want %v",
				tc.poolSize, tc.maxIdle, tc.engines, got, tc.want)
		}
	}
}

// TestFDNoSpareWhenItDoesNotFit checks a client whose pipeline pool cannot fit
// a spare gets none, and that the full-duplex session then stays on the
// pipeline pool instead of spilling to the main pool.
func TestFDNoSpareWhenItDoesNotFit(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  func(*Options)
	}{
		{"PipelinePoolSize 1", func(o *Options) { o.PipelinePoolSize = 1 }},
		{"MaxIdleConns 1", func(o *Options) { o.MaxIdleConns = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if err := probeRedis(internalTestRedisAddr()); err != nil {
				t.Skipf("no redis: %v", err)
			}
			opt := &Options{Addr: internalTestRedisAddr()}
			tc.opt(opt)
			client := NewClient(opt)
			defer client.Close()
			if err := client.Ping(ctx).Err(); err != nil {
				t.Skipf("no redis: %v", err)
			}
			ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()
			if ap.fd == nil {
				t.Skip("full-duplex engine not enabled for this client")
			}
			if err := ap.Set(ctx, "fdspare:fit", "1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			waitFDSession(t, ap, client, 0)
			time.Sleep(50 * time.Millisecond)
			pp := client.loadPipelinePool().pool
			if n := pp.IdleLen(); n != 0 {
				t.Fatalf("pipeline IdleLen = %d, want 0 (no spare)", n)
			}
			if ap.fd.curConnSpilled.Load() {
				t.Fatal("full-duplex session spilled to the main pool")
			}
		})
	}
}

// TestPipelineMinIdleConnsWithoutFD checks PipelineMinIdleConns alone keeps
// idle connections in the pipeline pool, and the default keeps none.
func TestPipelineMinIdleConnsWithoutFD(t *testing.T) {
	var slow atomic.Bool
	client := warmSpareClient(t, 2, &slow)
	pp := client.loadPipelinePool().pool
	deadline := time.Now().Add(5 * time.Second)
	for pp.IdleLen() != 2 || pp.Len() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("pipeline Len=%d IdleLen=%d, want 2/2", pp.Len(), pp.IdleLen())
		}
		time.Sleep(time.Millisecond)
	}

	def := warmSpareClient(t, 0, &slow)
	time.Sleep(50 * time.Millisecond)
	if n := def.loadPipelinePool().pool.IdleLen(); n != 0 {
		t.Fatalf("default pipeline IdleLen = %d, want 0", n)
	}
}
