package e2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// apWorkload is one traffic shape driven through a MultiDB client during a
// failover. step runs one unit of work and reports whether it succeeded;
// it returns a non-nil error from check for a reply that cannot be right
// (a value written for another key), which fails the test.
type apWorkload struct {
	name    string
	workers int
	// setup builds the workload's own state on its own MultiDB client.
	setup func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc
	// verify runs after traffic stops (nil = nothing extra to check).
	verify func(t *testing.T, mdb *redis.MultiDBClient)
}

type apStepFunc func(ctx context.Context, worker, seq int) (ok bool, bad error)

// keyValue writes values that embed their key, so a reply carrying another
// key's value — a reply delivered to the wrong command — is detectable even
// when the members are not read-your-writes consistent across a failover.
func keyValue(key string, seq int) string { return fmt.Sprintf("%s|%d", key, seq) }

func checkKeyValue(key, val string, err error) (bool, error) {
	switch {
	case errors.Is(err, redis.Nil):
		return true, nil
	case err != nil:
		return false, nil
	case !strings.HasPrefix(val, key+"|"):
		return false, fmt.Errorf("GET %s returned %q: reply for another command", key, val)
	}
	return true, nil
}

// apKey is the per-workload, per-worker key; a few keys per worker so batches
// carry distinct keys.
func apKey(workload string, worker, seq int) string {
	return fmt.Sprintf("e2e:ap:%s:%d:%d", workload, worker, seq%8)
}

// fullDuplex builds a full-duplex autopipeliner and fails the build when full
// duplex did not engage, so the FD rows cannot silently test half-duplex.
func fullDuplex(get func(*redis.MultiDBClient, *redis.AutoPipelineOptions) (*redis.AutoPipeliner, error)) func(*redis.MultiDBClient) (*redis.AutoPipeliner, error) {
	return func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) {
		ap, err := get(m, &redis.AutoPipelineOptions{FullDuplex: true})
		if err != nil {
			return nil, err
		}
		if !ap.Config().FullDuplex {
			return nil, errors.New("full duplex did not engage on the MultiDB client")
		}
		return ap, nil
	}
}

// blockingFace drives SET+GET on the blocking face, one command at a time.
func blockingFace(name string, get func(*redis.MultiDBClient) (*redis.AutoPipeliner, error)) apWorkload {
	return apWorkload{name: name, workers: 4, setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
		ap, err := get(mdb)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return func(ctx context.Context, w, seq int) (bool, error) {
			key := apKey(name, w, seq)
			if err := ap.Set(ctx, key, keyValue(key, seq), 0).Err(); err != nil {
				return false, nil
			}
			val, err := ap.Get(ctx, key).Result()
			return checkKeyValue(key, val, err)
		}
	}}
}

// asyncFace drives a window of SET+GET pairs on the async face and reads the
// results after submitting all of them.
func asyncFace(name string, get func(*redis.MultiDBClient) (*redis.AutoPipeliner, error)) apWorkload {
	const window = 16
	return apWorkload{name: name, workers: 2, setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
		ap, err := get(mdb)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return func(ctx context.Context, w, seq int) (bool, error) {
			keys := make([]string, window)
			sets := make([]*redis.StatusCmd, window)
			gets := make([]*redis.StringCmd, window)
			for i := range window {
				keys[i] = apKey(name, w, seq*window+i)
				sets[i] = ap.Set(ctx, keys[i], keyValue(keys[i], seq), 0)
				gets[i] = ap.Get(ctx, keys[i])
			}
			ok := true
			for i := range window {
				if sets[i].Err() != nil {
					ok = false
				}
				val, err := gets[i].Result()
				good, bad := checkKeyValue(keys[i], val, err)
				if bad != nil {
					return false, bad
				}
				ok = ok && good
			}
			return ok, nil
		}
	}}
}

// pipelinedFace drives SET+GET batches through ap.Pipelined / ap.TxPipelined.
func pipelinedFace(name string, tx bool) apWorkload {
	const batch = 8
	return apWorkload{name: name, workers: 2, setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
		ap, err := mdb.AutoPipeline()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		run := ap.Pipelined
		if tx {
			run = ap.TxPipelined
		}
		return func(ctx context.Context, w, seq int) (bool, error) {
			keys := make([]string, batch)
			gets := make([]*redis.StringCmd, batch)
			_, err := run(ctx, func(p redis.Pipeliner) error {
				for i := range batch {
					keys[i] = apKey(name, w, seq*batch+i)
					p.Set(ctx, keys[i], keyValue(keys[i], seq), 0)
					gets[i] = p.Get(ctx, keys[i])
				}
				return nil
			})
			if err != nil {
				return false, nil
			}
			for i := range batch {
				val, gerr := gets[i].Result()
				if _, bad := checkKeyValue(keys[i], val, gerr); bad != nil {
					return false, bad
				}
				// Inside one batch on one member the SET precedes the GET, so
				// the GET must see this batch's own value.
				if gerr == nil && val != keyValue(keys[i], seq) {
					return false, fmt.Errorf("GET %s after SET in the same batch returned %q", keys[i], val)
				}
			}
			return true, nil
		}
	}}
}

// atMostOnceCounter counts INCRs that a transaction reported as applied
// (succeeded) and those whose outcome is unknown (errored). The final counter
// must lie in [succeeded, succeeded+errored]: anything above means a
// transaction was applied twice.
type atMostOnceCounter struct {
	key string
	// lowerBound also checks that no acknowledged update was lost. Only valid
	// for updates that merge across members (CRDB counters): a last-write-wins
	// SET acknowledged on one member can be overwritten during the failover
	// window, which is Active-Active semantics, not a client bug.
	lowerBound bool
	succeeded  atomic.Int64
	errored    atomic.Int64
}

func (c *atMostOnceCounter) verify(t *testing.T, mdb *redis.MultiDBClient) {
	t.Helper()
	lo, hi := c.succeeded.Load(), c.succeeded.Load()+c.errored.Load()
	if !c.lowerBound {
		lo = 0
	}
	var got int64
	// The members converge asynchronously (CRDB counter merge), so wait for
	// the active member to report a value inside the bound.
	deadline := time.Now().Add(15 * time.Second)
	for {
		v, err := mdb.Get(context.Background(), c.key).Int64()
		if err == nil || errors.Is(err, redis.Nil) {
			got = v
			if got >= lo && got <= hi {
				return
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if got > hi {
		t.Errorf("%s = %d, above succeeded+errored = %d: a transaction was applied more than once", c.key, got, hi)
	} else {
		t.Errorf("%s = %d, below succeeded = %d: an acknowledged transaction was lost", c.key, got, lo)
	}
}

// txIncr runs MULTI/INCR/EXEC through the MultiDB client.
func txIncr(name string) apWorkload {
	c := &atMostOnceCounter{key: "e2e:ap:" + name + ":counter", lowerBound: true}
	return apWorkload{
		name: name, workers: 2,
		setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
			if err := mdb.Del(context.Background(), c.key).Err(); err != nil {
				t.Fatalf("%s: reset counter: %v", name, err)
			}
			return func(ctx context.Context, _, _ int) (bool, error) {
				_, err := mdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.Incr(ctx, c.key)
					return nil
				})
				if err != nil {
					c.errored.Add(1)
					return false, nil
				}
				c.succeeded.Add(1)
				return true, nil
			}
		},
		verify: c.verify,
	}
}

// watchIncr runs a WATCH/GET/MULTI/SET/EXEC check-and-set through the
// MultiDB client. A TxFailedErr (lost the optimistic race) applied nothing.
func watchIncr(name string) apWorkload {
	c := &atMostOnceCounter{key: "e2e:ap:" + name + ":counter"}
	return apWorkload{
		name: name, workers: 2,
		setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
			if err := mdb.Del(context.Background(), c.key).Err(); err != nil {
				t.Fatalf("%s: reset counter: %v", name, err)
			}
			return func(ctx context.Context, _, _ int) (bool, error) {
				err := mdb.Watch(ctx, func(tx *redis.Tx) error {
					n, err := tx.Get(ctx, c.key).Int64()
					if err != nil && !errors.Is(err, redis.Nil) {
						return err
					}
					_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
						p.Set(ctx, c.key, n+1, 0)
						return nil
					})
					return err
				}, c.key)
				switch {
				case err == nil:
					c.succeeded.Add(1)
					return true, nil
				case errors.Is(err, redis.TxFailedErr):
					return true, nil
				default:
					c.errored.Add(1)
					return false, nil
				}
			}
		},
		verify: c.verify,
	}
}

// TestAutoPipelineAndTransactionsAcrossFailover drives every autopipeline
// mode MultiDB supports, plus transactions, through one outage of member 0
// and the fallback after it recovers. Each workload has its own MultiDB
// client (an autopipeliner's config is fixed per client), so one fault covers
// all of them. Checks:
//   - every workload makes progress on member 1 during the outage, and on
//     member 0 again after the fallback;
//   - no reply is delivered to the wrong command;
//   - transactions are applied at most once.
func TestAutoPipelineAndTransactionsAcrossFailover(t *testing.T) {
	workloads := []apWorkload{
		blockingFace("blocking", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) { return m.AutoPipeline() }),
		asyncFace("async", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) { return m.AsyncAutoPipeline() }),
		asyncFace("async-unordered", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4})
		}),
		asyncFace("async-shards", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{Unordered: true, NumShards: 4})
		}),
		blockingFace("fd-blocking", fullDuplex(func(m *redis.MultiDBClient, o *redis.AutoPipelineOptions) (*redis.AutoPipeliner, error) {
			return m.AutoPipelineWithOptions(o)
		})),
		asyncFace("fd-async", fullDuplex(func(m *redis.MultiDBClient, o *redis.AutoPipelineOptions) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(o)
		})),
		pipelinedFace("pipelined", false),
		pipelinedFace("tx-pipelined", true),
		txIncr("tx-incr"),
		watchIncr("watch-cas"),
	}

	type running struct {
		w    apWorkload
		mdb  *redis.MultiDBClient
		step apStepFunc
		// progress[m] counts successful steps while member m was active.
		progress [2]atomic.Int64
	}
	runs := make([]*running, len(workloads))
	for i, w := range workloads {
		mdb := newE2EClient(t, fiMultiDBOptions())
		runs[i] = &running{w: w, mdb: mdb, step: w.setup(t, mdb)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var badMu sync.Mutex
	var bad []error
	// Stop the workers on every exit path: a t.Fatal inside eventually must
	// not leave them running while t.Cleanup closes the clients.
	defer func() {
		cancel()
		wg.Wait()
	}()
	for _, r := range runs {
		for w := range r.w.workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for seq := 0; ctx.Err() == nil; seq++ {
					sctx, scancel := context.WithTimeout(ctx, 2*time.Second)
					ok, b := r.step(sctx, w, seq)
					scancel()
					if b != nil {
						badMu.Lock()
						bad = append(bad, fmt.Errorf("%s: %w", r.w.name, b))
						badMu.Unlock()
					}
					if active := r.mdb.ActiveDatabaseID(); ok && (active == 0 || active == 1) {
						r.progress[active].Add(1)
					}
					if !ok {
						time.Sleep(20 * time.Millisecond)
					}
				}
			}()
		}
	}

	allProgressed := func(member int, atLeast int64) func() bool {
		base := make([]int64, len(runs))
		for i, r := range runs {
			base[i] = r.progress[member].Load()
		}
		return func() bool {
			for i, r := range runs {
				if r.progress[member].Load()-base[i] < atLeast {
					return false
				}
			}
			return true
		}
	}

	eventually(t, 15*time.Second, "every workload making progress on member 0", allProgressed(0, 5))

	const outage = 30 * time.Second
	id := triggerNetworkFailure(t, 0, outage)
	eventually(t, 20*time.Second, "every workload making progress on member 1 during the outage", allProgressed(1, 5))

	waitForActionDone(t, id, outage+30*time.Second)
	eventually(t, 30*time.Second, "every workload making progress on member 0 after the fallback", allProgressed(0, 5))

	cancel()
	wg.Wait()

	for _, b := range bad {
		t.Error(b)
	}
	for _, r := range runs {
		if r.w.verify != nil {
			r.w.verify(t, r.mdb)
		}
	}
	for _, r := range runs {
		t.Logf("%s: %d steps on member 0, %d on member 1", r.w.name, r.progress[0].Load(), r.progress[1].Load())
	}
}
