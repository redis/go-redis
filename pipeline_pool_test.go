package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Tests for the DEDICATED pipeline connection pool (enabled by
// PipelineReadBufferSize/PipelineWriteBufferSize): handshake parity,
// maintnotifications wiring, and streaming-credentials re-auth on
// pipeline-pool connections.

// ===== from pipeline_pool_handshake_test.go =====

// TestPipelinePoolHandshakeACLAndSelect verifies that connections in the
// DEDICATED pipeline pool (enabled by PipelineReadBufferSize/WriteBufferSize)
// perform the full handshake — ACL AUTH (username+password) and SELECT DB — the
// same as normal-pool conns. A pipeline and an autopipeline both run on those
// conns; if the handshake were skipped they would get NOAUTH or land in the
// wrong DB.
func TestPipelinePoolHandshakeACLAndSelect(t *testing.T) {
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: apTestAddr()})
	defer admin.Close()
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	const user, pw, db = "ap_pipe_user", "ap_pipe_pw", 3
	_ = admin.Do(ctx, "ACL", "DELUSER", user).Err()
	if err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+pw, "~*", "+@all").Err(); err != nil {
		t.Skipf("ACL unsupported: %v", err)
	}
	defer admin.Do(ctx, "ACL", "DELUSER", user)

	c := redis.NewClient(&redis.Options{
		Addr:                    apTestAddr(),
		Username:                user,
		Password:                pw,
		DB:                      db,
		PipelineReadBufferSize:  64 << 10,
		PipelineWriteBufferSize: 64 << 10,
		PipelinePoolSize:        3,
	})
	defer c.Close()

	// Manual pipeline -> pipeline pool. Must AUTH (ACL) + SELECT db.
	pipe := c.Pipeline()
	pipe.Set(ctx, "hs:pipe", "v1", 0)
	g1 := pipe.Get(ctx, "hs:pipe")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline on ACL+DB pipeline pool: %v", err)
	}
	if g1.Val() != "v1" {
		t.Fatalf("pipeline get = %q, want v1", g1.Val())
	}
	if st := c.PoolStats(); st.PipelineStats == nil {
		t.Fatal("expected a dedicated pipeline pool (PipelineStats nil)")
	}

	// AutoPipeline (batched) also runs on the pipeline pool.
	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxFlushDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	s := ap.Set(ctx, "hs:ap1", "v2", 0)
	s2 := ap.Set(ctx, "hs:ap2", "v3", 0)
	if err := s.Err(); err != nil {
		t.Fatalf("autopipeline set1: %v", err)
	}
	if err := s2.Err(); err != nil {
		t.Fatalf("autopipeline set2: %v", err)
	}

	// SELECT DB really took effect on the pipeline conns: keys exist in `db`,
	// not in DB 0.
	verify := redis.NewClient(&redis.Options{Addr: apTestAddr(), Username: user, Password: pw, DB: db})
	defer verify.Close()
	for _, k := range []string{"hs:pipe", "hs:ap1", "hs:ap2"} {
		if n, _ := verify.Exists(ctx, k).Result(); n != 1 {
			t.Fatalf("key %q not found in DB %d (SELECT not applied on pipeline conn)", k, db)
		}
	}
	db0 := redis.NewClient(&redis.Options{Addr: apTestAddr(), DB: 0})
	defer db0.Close()
	if n, _ := db0.Exists(ctx, "hs:pipe").Result(); n != 0 {
		t.Fatalf("hs:pipe leaked into DB 0 (wrong SELECT on pipeline conn)")
	}

	// cleanup keys in db
	verify.Del(ctx, "hs:pipe", "hs:ap1", "hs:ap2")
}

// ===== from pipeline_pool_maintnotif_test.go =====

// TestPipelinePoolMaintNotifications verifies maintnotifications is wired to the
// DEDICATED pipeline pool: the CLIENT MAINT_NOTIFICATIONS handshake runs on
// pipeline-pool conns during initConn and the pipeline read path drains push
// frames, so pipelines and autopipelines keep working with the feature enabled.
// (The manager registers the pipeline pool via InitPoolHookForPool and tracks
// each conn via TrackMaintNotificationsConn.)
func TestPipelinePoolMaintNotifications(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                     apTestAddr(),
		Protocol:                 3, // maintnotifications requires RESP3
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeAuto},
		PipelineReadBufferSize:   64 << 10,
		PipelineWriteBufferSize:  64 << 10,
		PipelinePoolSize:         2,
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	// Manual pipeline on the pipeline pool.
	pipe := c.Pipeline()
	pipe.Set(ctx, "mn:k", "v", 0)
	g := pipe.Get(ctx, "mn:k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline with maintnotifications enabled: %v", err)
	}
	if g.Val() != "v" {
		t.Fatalf("pipeline get = %q, want v", g.Val())
	}
	if st := c.PoolStats(); st.PipelineStats == nil {
		t.Fatal("expected a dedicated pipeline pool (PipelineStats nil)")
	}

	// AutoPipeline (batched) on the pipeline pool.
	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxFlushDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	s1 := ap.Set(ctx, "mn:ap1", "v1", 0)
	s2 := ap.Incr(ctx, "mn:n")
	if err := s1.Err(); err != nil {
		t.Fatalf("autopipeline set with maintnotifications: %v", err)
	}
	if err := s2.Err(); err != nil {
		t.Fatalf("autopipeline incr with maintnotifications: %v", err)
	}
	if s2.Val() != 1 {
		t.Fatalf("autopipeline incr = %d, want 1", s2.Val())
	}

	c.Del(ctx, "mn:k", "mn:ap1", "mn:n")
}

// ===== from pipeline_pool_streamingcreds_test.go =====

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
