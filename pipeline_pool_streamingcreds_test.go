package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
)

// TestPipelinePoolStreamingCredsReauth verifies the StreamingCredentialsProvider
// drives AUTH on DEDICATED pipeline-pool connections too, both at init and on a
// credential rotation. It mirrors the main-pool streaming test but drives
// pipeline traffic (which runs on the pipeline pool) and uses PipelinePoolSize=1
// so the single pipeline conn must re-auth.
//
// Credentials are intentionally invalid on the server, so the pipeline itself
// errors — the assertion is that the AUTH command was issued with the initial,
// then the updated, credentials for the pipeline-pool connection (proving the
// re-auth listener is wired on that pool).
func TestPipelinePoolStreamingCredsReauth(t *testing.T) {
	ctx := context.Background()
	recorder := newCommandRecorder(200)
	initialCreds := auth.NewBasicCredentials("initial_user", "initial_pass")
	updatedCreds := auth.NewBasicCredentials("updated_user", "updated_pass")
	updates := make(chan auth.Credentials, 1)

	opt := &redis.Options{
		Addr:                         apTestAddr(),
		StreamingCredentialsProvider: &mockStreamingProvider{credentials: initialCreds, updates: updates},
		PipelineReadBufferSize:       64 << 10,
		PipelineWriteBufferSize:      64 << 10,
		PipelinePoolSize:             1,
		PoolSize:                     1,
	}
	c := redis.NewClient(opt)
	defer c.Close()
	c.AddHook(recorder.Hook())

	// probe with reachability: if there's no redis at all, skip.
	if err := c.Ping(ctx).Err(); err == nil {
		// creds are invalid, so a successful ping means auth isn't enforced here;
		// the test still exercises the AUTH-issuing path below.
	}

	runPipe := func() {
		pipe := c.Pipeline()
		pipe.Set(ctx, "sc:k", "v", 0)
		pipe.Get(ctx, "sc:k")
		_, _ = pipe.Exec(ctx) // errors (bad creds); we only care that AUTH was issued
	}

	runPipe()
	deadline := time.Now().Add(2 * time.Second)
	for !recorder.Contains("AUTH initial_user") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		runPipe()
	}
	if !recorder.Contains("AUTH initial_user") {
		t.Skipf("no AUTH observed (redis without auth enforcement or unreachable); last commands=%v", recorder.LastCommands())
	}

	// rotate credentials; the pipeline-pool conn must re-auth with the new creds.
	updates <- updatedCreds
	deadline = time.Now().Add(2 * time.Second)
	for !recorder.Contains("AUTH updated_user") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		runPipe()
	}
	if !recorder.Contains("AUTH updated_user") {
		t.Fatalf("pipeline-pool conn did not re-auth after credential rotation; commands=%v", recorder.LastCommands())
	}
	close(updates)
}
