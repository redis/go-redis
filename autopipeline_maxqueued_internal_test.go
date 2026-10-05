package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutoPipelineMaxQueuedCommandsValidate(t *testing.T) {
	if err := (&AutoPipelineOptions{MaxQueuedCommands: -1}).Validate(); err == nil {
		t.Fatal("negative MaxQueuedCommands must be rejected")
	}
	if err := (&AutoPipelineOptions{MaxQueuedCommands: 10, FullDuplex: true}).Validate(); err == nil {
		t.Fatal("MaxQueuedCommands with FullDuplex must be rejected")
	}
	if err := (&AutoPipelineOptions{MaxQueuedCommands: 10}).Validate(); err != nil {
		t.Fatalf("half-duplex MaxQueuedCommands must be valid: %v", err)
	}
}

// maxQueuedTestClient returns a client with a dispatch gate installed. While
// armed, every dispatch (solo, pipelined, diverted) waits on gate, so accepted
// commands stay outstanding.
func maxQueuedTestClient(t *testing.T) (*Client, chan struct{}, *atomic.Bool) {
	t.Helper()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	gate := make(chan struct{})
	armed := &atomic.Bool{}
	client.AddHook(dispatchGateHook{gate: gate, armed: armed})
	return client, gate, armed
}

func waitQueuedZero(t *testing.T, ap *AutoPipeliner) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ap.queued.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("queued = %d, want 0 (slot leak)", ap.queued.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestAutoPipelineMaxQueuedCommandsRejectsAsync fills the limit with commands
// that cannot complete, checks the next ones fail at once with
// ErrAutoPipelineQueueFull, then checks the slots come back.
func TestAutoPipelineMaxQueuedCommandsRejectsAsync(t *testing.T) {
	ctx := context.Background()
	client, gate, armed := maxQueuedTestClient(t)

	const limit = 4
	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{MaxQueuedCommands: limit}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	armed.Store(true)

	// One diverted command (Do runs outside the pipeline) plus queued SETs.
	accepted := []Cmder{ap.Do(ctx, "PING")}
	for i := 0; i < limit-1; i++ {
		accepted = append(accepted, ap.Set(ctx, "mq:k", i, 0))
	}
	if got := ap.queued.Load(); got != limit {
		t.Fatalf("queued = %d, want %d", got, limit)
	}

	// At the limit: queued and diverted submits both fail without blocking.
	rejected := []Cmder{ap.Set(ctx, "mq:k", "x", 0), ap.Do(ctx, "PING")}
	for _, cmd := range rejected {
		if err := cmd.Err(); !errors.Is(err, ErrAutoPipelineQueueFull) {
			t.Fatalf("%s at the limit: err = %v, want ErrAutoPipelineQueueFull", cmd.Name(), err)
		}
	}
	if err := ap.Process(ctx, NewStatusCmd(ctx, "set", "mq:k", "y")); !errors.Is(err, ErrAutoPipelineQueueFull) {
		t.Fatalf("Process at the limit returned %v, want ErrAutoPipelineQueueFull", err)
	}

	armed.Store(false)
	close(gate)
	for _, cmd := range accepted {
		if err := cmd.Err(); err != nil {
			t.Fatalf("accepted %s failed: %v", cmd.Name(), err)
		}
	}
	waitQueuedZero(t, ap)

	if err := ap.Set(ctx, "mq:k", "z", 0).Err(); err != nil {
		t.Fatalf("submit after slots freed: %v", err)
	}
	waitQueuedZero(t, ap)
}

// TestAutoPipelineMaxQueuedCommandsRejectKeepsArrivalsNonNegative checks that
// rejected submits consume announced arrivals but never drive the counter
// negative, so a caller retrying in a loop cannot hide the next wave
// (Cursor Bugbot on #4070).
func TestAutoPipelineMaxQueuedCommandsRejectKeepsArrivalsNonNegative(t *testing.T) {
	ctx := context.Background()
	client, gate, armed := maxQueuedTestClient(t)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{MaxQueuedCommands: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	armed.Store(true)
	held := ap.Set(ctx, "mq:e", "1", 0)
	// Wait until the held command is dispatched: the flusher is then idle and
	// cannot touch expectedArrivals (rejected submits do not wake it).
	deadline := time.Now().Add(5 * time.Second)
	for ap.shards[0].inFlight.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("held command was never dispatched")
		}
		time.Sleep(time.Millisecond)
	}

	ap.expectedArrivals.Store(3)
	for i := 0; i < 2; i++ {
		_ = ap.Set(ctx, "mq:e", "x", 0)
	}
	if got := ap.expectedArrivals.Load(); got != 1 {
		t.Fatalf("expectedArrivals = %d after 2 rejects from 3, want 1", got)
	}
	for i := 0; i < 1000; i++ {
		_ = ap.Set(ctx, "mq:e", "x", 0)
	}
	if got := ap.expectedArrivals.Load(); got != 0 {
		t.Fatalf("expectedArrivals = %d after a retry storm, want 0 (never negative)", got)
	}

	armed.Store(false)
	close(gate)
	if err := held.Err(); err != nil {
		t.Fatalf("held command: %v", err)
	}
	waitQueuedZero(t, ap)
}

// TestAutoPipelineMaxQueuedCommandsBlockingFace checks the limit on the
// blocking face: a caller over the limit fails at once instead of waiting.
func TestAutoPipelineMaxQueuedCommandsBlockingFace(t *testing.T) {
	ctx := context.Background()
	client, gate, armed := maxQueuedTestClient(t)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{MaxQueuedCommands: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	armed.Store(true)

	held := make(chan error, 1)
	go func() { held <- ap.Set(ctx, "mq:b", "1", 0).Err() }()
	deadline := time.Now().Add(5 * time.Second)
	for ap.queued.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first command was never admitted")
		}
		time.Sleep(time.Millisecond)
	}

	if err := ap.Set(ctx, "mq:b", "2", 0).Err(); !errors.Is(err, ErrAutoPipelineQueueFull) {
		t.Fatalf("blocking face over the limit: err = %v, want ErrAutoPipelineQueueFull", err)
	}
	// Commands run outside the pipeline count too (Codex on #4070).
	if err := ap.Do(ctx, "PING").Err(); !errors.Is(err, ErrAutoPipelineQueueFull) {
		t.Fatalf("blocking-face Do over the limit: err = %v, want ErrAutoPipelineQueueFull", err)
	}

	armed.Store(false)
	close(gate)
	if err := <-held; err != nil {
		t.Fatalf("held command: %v", err)
	}
	waitQueuedZero(t, ap)
}

// TestAutoPipelineMaxQueuedCommandsReleasedOnClose closes the pipeliner while
// accepted commands are still held, so they complete through the shutdown
// drain, and checks no slot leaks.
func TestAutoPipelineMaxQueuedCommandsReleasedOnClose(t *testing.T) {
	ctx := context.Background()
	client, gate, armed := maxQueuedTestClient(t)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{MaxQueuedCommands: 1000}, false)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	cmds := make([]*StatusCmd, 0, 50)
	for i := 0; i < 50; i++ {
		cmds = append(cmds, ap.Set(ctx, "mq:c", i, 0))
	}

	closed := make(chan error, 1)
	go func() { closed <- ap.Close() }()
	time.Sleep(20 * time.Millisecond)
	armed.Store(false)
	close(gate)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, cmd := range cmds {
		_ = cmd.Err() // waits for completion; ErrClosed for late ones is fine
	}
	waitQueuedZero(t, ap)
}

// TestAutoPipelineMaxQueuedCommandsReleasedOnPanic checks a dispatch panic
// still frees the batch's slots.
func TestAutoPipelineMaxQueuedCommandsReleasedOnPanic(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.AddHook(panicHook{})

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{MaxQueuedCommands: 100}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cmds := make([]*StatusCmd, 0, 20)
	for i := 0; i < 20; i++ {
		cmds = append(cmds, ap.Set(ctx, "mq:p", i, 0))
	}
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			t.Fatal("want the recovered panic error")
		}
	}
	waitQueuedZero(t, ap)
}

type panicHook struct{}

func (panicHook) DialHook(next DialHook) DialHook { return next }
func (panicHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error { panic("mq test panic") }
}

func (panicHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error { panic("mq test panic") }
}
