package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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
