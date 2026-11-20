package e2e

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
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

// checkKeyValue reports ok only for a GET that returned this key's value. A
// nil reply is tolerated (not an error) but is not progress.
func checkKeyValue(key, val string, err error) (bool, error) {
	switch {
	case err != nil:
		return false, nil
	case !strings.HasPrefix(val, key+"|"):
		return false, fmt.Errorf("GET %s returned %q: reply for another command", key, val)
	}
	return true, nil
}

// apKeyTTL bounds how long keys outlive a run against a shared database.
const apKeyTTL = 10 * time.Minute

// apKey is the per-run, per-workload, per-worker key for slot. Commands in
// one window or batch use distinct slots, so each GET has its own key and a
// reply delivered to another GET in the same window is detectable. The hash
// tag keeps one worker's keys in one hash slot, so a MULTI/EXEC batch over
// them is not CROSSSLOT.
func apKey(run, workload string, worker, slot int) string {
	return fmt.Sprintf("e2e:ap:{%s:%s:%d}:%d", run, workload, worker, slot)
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
func blockingFace(run, name string, get func(*redis.MultiDBClient) (*redis.AutoPipeliner, error)) apWorkload {
	return apWorkload{name: name, workers: 4, setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
		ap, err := get(mdb)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return func(ctx context.Context, w, seq int) (bool, error) {
			key := apKey(run, name, w, seq%8)
			if err := ap.Set(ctx, key, keyValue(key, seq), apKeyTTL).Err(); err != nil {
				return false, nil
			}
			val, err := ap.Get(ctx, key).Result()
			return checkKeyValue(key, val, err)
		}
	}}
}

// asyncFace drives a window of SET+GET pairs on the async face and reads the
// results after submitting all of them.
func asyncFace(run, name string, get func(*redis.MultiDBClient) (*redis.AutoPipeliner, error)) apWorkload {
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
				keys[i] = apKey(run, name, w, i)
				sets[i] = ap.Set(ctx, keys[i], keyValue(keys[i], seq), apKeyTTL)
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
func pipelinedFace(run, name string, tx bool) apWorkload {
	const batch = 8
	return apWorkload{name: name, workers: 2, setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
		ap, err := mdb.AutoPipeline()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		exec := ap.Pipelined
		if tx {
			exec = ap.TxPipelined
		}
		return func(ctx context.Context, w, seq int) (bool, error) {
			keys := make([]string, batch)
			gets := make([]*redis.StringCmd, batch)
			_, err := exec(ctx, func(p redis.Pipeliner) error {
				for i := range batch {
					keys[i] = apKey(run, name, w, i)
					p.Set(ctx, keys[i], keyValue(keys[i], seq), apKeyTTL)
					gets[i] = p.Get(ctx, keys[i])
				}
				return nil
			})
			// Check the GETs that completed even when the batch failed: a
			// partial failure must not hide a reply for another command.
			ok := err == nil
			for i := range batch {
				val, gerr := gets[i].Result()
				good, bad := checkKeyValue(keys[i], val, gerr)
				if bad != nil {
					return false, bad
				}
				ok = ok && good
				// Inside one batch on one member the SET precedes the GET, so
				// the GET must see this batch's own value. Only checked when
				// the whole batch succeeded.
				if err == nil && gerr == nil && val != keyValue(keys[i], seq) {
					return false, fmt.Errorf("GET %s after SET in the same batch returned %q", keys[i], val)
				}
			}
			return ok, nil
		}
	}}
}

// atMostOnceCounter checks that transactions are applied at most once and
// that acknowledged ones are not lost. Each attempt runs, in one MULTI, an
// INCR of key and an HINCRBY of its own field (worker:seq) in attempts. A
// field above 1 is an attempt applied twice, a missing field for an
// acknowledged attempt is a lost one, and the counter must equal the sum of
// the fields. INCR and HINCRBY merge across Active-Active members (CRDB
// counters), so these checks hold after convergence.
type atMostOnceCounter struct {
	key      string
	attempts string

	mu        sync.Mutex
	succeeded []string
	errored   int
}

func newAtMostOnceCounter(run, name string) *atMostOnceCounter {
	tag := "e2e:ap:{" + run + ":" + name + "}:"
	return &atMostOnceCounter{key: tag + "counter", attempts: tag + "attempts"}
}

func attemptField(worker, seq int) string { return fmt.Sprintf("%d:%d", worker, seq) }

// queue adds the attempt's writes to a MULTI.
func (c *atMostOnceCounter) queue(ctx context.Context, p redis.Pipeliner, worker, seq int) {
	p.Incr(ctx, c.key)
	p.HIncrBy(ctx, c.attempts, attemptField(worker, seq), 1)
	p.Expire(ctx, c.key, apKeyTTL)
	p.Expire(ctx, c.attempts, apKeyTTL)
}

func (c *atMostOnceCounter) record(worker, seq int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.errored++
		return
	}
	c.succeeded = append(c.succeeded, attemptField(worker, seq))
}

func (c *atMostOnceCounter) cleanup(t *testing.T, mdb *redis.MultiDBClient) {
	t.Cleanup(func() { _ = mdb.Del(context.Background(), c.key, c.attempts).Err() })
}

// counterState is one member's view of a counter.
type counterState struct {
	n      int64
	fields map[string]string
	err    error
}

func (s counterState) equal(o counterState) bool {
	return s.err == nil && o.err == nil && s.n == o.n && maps.Equal(s.fields, o.fields)
}

// summary describes a state for error messages.
func (s counterState) summary() string {
	if s.err != nil {
		return s.err.Error()
	}
	var sum int64
	for _, v := range s.fields {
		n, _ := strconv.ParseInt(v, 10, 64)
		sum += n
	}
	return fmt.Sprintf("counter=%d fields=%d sum=%d", s.n, len(s.fields), sum)
}

func readCounter(m *redis.Client, c *atMostOnceCounter) counterState {
	ctx := context.Background()
	n, err := m.Get(ctx, c.key).Int64()
	if errors.Is(err, redis.Nil) {
		n, err = 0, nil
	}
	if err != nil {
		return counterState{err: err}
	}
	fields, err := m.HGetAll(ctx, c.attempts).Result()
	return counterState{n: n, fields: fields, err: err}
}

// verify waits until every member reports the same state for stablePolls
// consecutive polls, then checks it. It reads each member directly, not
// through MultiDB, so a member that has not merged yet cannot pass.
func (c *atMostOnceCounter) verify(t *testing.T, _ *redis.MultiDBClient) {
	t.Helper()
	const (
		stablePolls = 3
		interval    = 500 * time.Millisecond
		maxReported = 5
	)
	c.mu.Lock()
	succeeded, errored := c.succeeded, c.errored
	c.mu.Unlock()

	members := make([]*redis.Client, len(e2eTopology.Endpoints))
	for i, addr := range e2eTopology.Endpoints {
		members[i] = redis.NewClient(endpointOptions(addr))
		defer members[i].Close()
	}

	// Members agree only when the counter and the full field maps match;
	// summaries can match while the fields differ.
	var (
		st, last counterState
		stable   int
	)
	deadline := time.Now().Add(30 * time.Second)
	for {
		views := make([]string, len(members))
		agree := true
		for i, m := range members {
			s := readCounter(m, c)
			views[i] = s.summary()
			if i == 0 {
				st = s
			}
			if !s.equal(st) {
				agree = false
			}
		}
		switch {
		case !agree:
			stable = 0
		case stable > 0 && st.equal(last):
			stable++
		default:
			last, stable = st, 1
		}
		if stable >= stablePolls {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("%s did not converge across members: %v", c.key, views)
			return
		}
		time.Sleep(interval)
	}

	var sum int64
	reported := 0
	for f, v := range st.fields {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Errorf("%s field %s = %q: not an integer", c.attempts, f, v)
			continue
		}
		sum += n
		if n > 1 && reported < maxReported {
			reported++
			t.Errorf("%s: attempt %s applied %d times", c.key, f, n)
		}
	}
	if sum != st.n {
		t.Errorf("%s = %d, but its attempt fields sum to %d", c.key, st.n, sum)
	}
	lost := 0
	for _, f := range succeeded {
		if _, ok := st.fields[f]; !ok {
			if lost < maxReported {
				t.Errorf("%s: acknowledged attempt %s was lost", c.key, f)
			}
			lost++
		}
	}
	if hi := int64(len(succeeded) + errored); st.n > hi {
		t.Errorf("%s = %d, above succeeded+errored = %d: a transaction was applied more than once", c.key, st.n, hi)
	}
	if lost > maxReported {
		t.Errorf("%s: %d acknowledged attempts lost in total", c.key, lost)
	}
}

// txIncr runs MULTI/INCR/HINCRBY/EXEC through the MultiDB client.
func txIncr(run, name string) apWorkload {
	c := newAtMostOnceCounter(run, name)
	return apWorkload{
		name: name, workers: 2,
		setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
			c.cleanup(t, mdb)
			return func(ctx context.Context, w, seq int) (bool, error) {
				_, err := mdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
					c.queue(ctx, p, w, seq)
					return nil
				})
				c.record(w, seq, err)
				return err == nil, nil
			}
		},
		verify: c.verify,
	}
}

// watchIncr runs WATCH/GET/MULTI/INCR/HINCRBY/EXEC through the MultiDB
// client. The GET makes it a read-then-write under WATCH. A TxFailedErr (lost
// the optimistic race) applied nothing and is not progress.
func watchIncr(run, name string) apWorkload {
	c := newAtMostOnceCounter(run, name)
	return apWorkload{
		name: name, workers: 2,
		setup: func(t *testing.T, mdb *redis.MultiDBClient) apStepFunc {
			c.cleanup(t, mdb)
			return func(ctx context.Context, w, seq int) (bool, error) {
				err := mdb.Watch(ctx, func(tx *redis.Tx) error {
					if err := tx.Get(ctx, c.key).Err(); err != nil && !errors.Is(err, redis.Nil) {
						return err
					}
					_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
						c.queue(ctx, p, w, seq)
						return nil
					})
					return err
				}, c.key)
				if errors.Is(err, redis.TxFailedErr) {
					return false, nil
				}
				c.record(w, seq, err)
				return err == nil, nil
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
//   - transactions are applied at most once, and none acknowledged is lost.
func TestAutoPipelineAndTransactionsAcrossFailover(t *testing.T) {
	// run isolates this run's keys from earlier runs against the same
	// database.
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	workloads := []apWorkload{
		blockingFace(run, "blocking", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) { return m.AutoPipeline() }),
		asyncFace(run, "async", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) { return m.AsyncAutoPipeline() }),
		asyncFace(run, "async-unordered", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4})
		}),
		asyncFace(run, "async-shards", func(m *redis.MultiDBClient) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{Unordered: true, NumShards: 4})
		}),
		blockingFace(run, "fd-blocking", fullDuplex(func(m *redis.MultiDBClient, o *redis.AutoPipelineOptions) (*redis.AutoPipeliner, error) {
			return m.AutoPipelineWithOptions(o)
		})),
		asyncFace(run, "fd-async", fullDuplex(func(m *redis.MultiDBClient, o *redis.AutoPipelineOptions) (*redis.AutoPipeliner, error) {
			return m.AsyncAutoPipelineWithOptions(o)
		})),
		pipelinedFace(run, "pipelined", false),
		pipelinedFace(run, "tx-pipelined", true),
		txIncr(run, "tx-incr"),
		watchIncr(run, "watch-cas"),
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
	// bad keeps the first maxBad wrong replies; badCount counts all of them,
	// so a systematic misdelivery cannot grow the buffer without bound.
	const maxBad = 20
	var badMu sync.Mutex
	var bad []error
	var badCount atomic.Int64
	// Stop the workers on every exit path: a t.Fatal inside eventually must
	// not leave them running while t.Cleanup closes the clients. Report wrong
	// replies here too, so a progress timeout does not hide them.
	defer func() {
		cancel()
		wg.Wait()
		for _, b := range bad {
			t.Error(b)
		}
		if n := badCount.Load(); n > maxBad {
			t.Errorf("%d wrong replies in total, first %d shown", n, maxBad)
		}
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
					if b != nil && badCount.Add(1) <= maxBad {
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

	for _, r := range runs {
		if r.w.verify != nil {
			r.w.verify(t, r.mdb)
		}
	}
	for _, r := range runs {
		t.Logf("%s: %d steps on member 0, %d on member 1", r.w.name, r.progress[0].Load(), r.progress[1].Load())
	}
}
