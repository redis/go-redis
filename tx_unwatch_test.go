package redis_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// cmdRecorder captures the command stream a client sends so tests can assert
// the presence or absence of individual commands. UNWATCH from Tx.Close flows
// through ProcessHook; the MULTI/EXEC block flows through ProcessPipelineHook.
type cmdRecorder struct {
	mu   sync.Mutex
	sent []string
}

func (h *cmdRecorder) record(names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sent = append(h.sent, names...)
}

func (h *cmdRecorder) has(name string) bool {
	return h.count(name) > 0
}

func (h *cmdRecorder) count(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.sent {
		if c == name {
			n++
		}
	}
	return n
}

func (h *cmdRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cmdRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.record(strings.ToUpper(cmd.Name()))
		return next(ctx, cmd)
	}
}

func (h *cmdRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.record(strings.ToUpper(cmd.Name()))
		}
		return next(ctx, cmds)
	}
}

// commandCalls returns the per-command call counts reported by
// INFO commandstats, keyed by command name (e.g. "unwatch").
func commandCalls(client *redis.Client) map[string]int64 {
	info, err := client.Info(ctx, "commandstats").Result()
	Expect(err).NotTo(HaveOccurred())

	calls := make(map[string]int64)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		const prefix = "cmdstat_"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		name, rest, ok := strings.Cut(line[len(prefix):], ":")
		if !ok {
			continue
		}
		// rest looks like: calls=3,usec=10,usec_per_call=3.33,...
		fields := strings.Split(rest, ",")
		if len(fields) == 0 {
			continue
		}
		kv := strings.SplitN(fields[0], "=", 2)
		if len(kv) != 2 || kv[0] != "calls" {
			continue
		}
		n, err := strconv.ParseInt(kv[1], 10, 64)
		Expect(err).NotTo(HaveOccurred())
		calls[name] = n
	}
	return calls
}

// pipelineErrInjector returns a fixed error for the MULTI/EXEC batch without
// executing it, simulating a server error reported before EXEC runs (such as
// -LOADING on the MULTI reply). The watched keys stay armed, so Close must
// still send UNWATCH.
type pipelineErrInjector struct{ err error }

func (h *pipelineErrInjector) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *pipelineErrInjector) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *pipelineErrInjector) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if strings.EqualFold(cmd.Name(), "exec") {
				return h.err // do not call next: EXEC never runs
			}
		}
		return next(ctx, cmds)
	}
}

// cmdErrInjector fails a single named command without sending it, simulating a
// server error on that command (e.g. a WATCH that the server rejects).
type cmdErrInjector struct {
	name string
	err  error
}

func (h *cmdErrInjector) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cmdErrInjector) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), h.name) {
			return h.err // do not call next: the command never runs
		}
		return next(ctx, cmd)
	}
}

func (h *cmdErrInjector) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

var _ = Describe("Tx UNWATCH elision", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	// watchRecorded runs fn inside client.Watch (recording the wire commands)
	// and returns the recorder plus the error returned by Watch.
	watchRecorded := func(fn func(*redis.Tx) error, keys ...string) (*cmdRecorder, error) {
		rec := &cmdRecorder{}
		client.AddHook(rec)
		err := client.Watch(ctx, fn, keys...)
		return rec, err
	}

	// expectUnwatchSent asserts that fn, which must not run EXEC, leaves the
	// connection with a WATCH that Close releases via UNWATCH. The error
	// returned by fn is irrelevant here (a read may legitimately return
	// redis.Nil), so it is not asserted.
	expectUnwatchSent := func(fn func(*redis.Tx) error, keys ...string) {
		rec, _ := watchRecorded(fn, keys...)
		Expect(rec.has("WATCH")).To(BeTrue(), "WATCH must be sent")
		Expect(rec.has("EXEC")).To(BeFalse(), "no EXEC should have run")
		Expect(rec.has("UNWATCH")).To(BeTrue(), "UNWATCH must be sent when no EXEC cleared the watch")
	}

	// --- the watch is cleared by EXEC: UNWATCH must be elided ---

	It("does not send UNWATCH after a committed EXEC", func() {
		rec, err := watchRecorded(func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "value", 0)
				return nil
			})
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.has("WATCH")).To(BeTrue())
		Expect(rec.has("EXEC")).To(BeTrue())
		Expect(rec.has("UNWATCH")).To(BeFalse())
	})

	It("does not send UNWATCH when no keys are watched", func() {
		rec, err := watchRecorded(func(tx *redis.Tx) error {
			return tx.Ping(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.has("PING")).To(BeTrue(), "fn must have actually run")
		Expect(rec.has("WATCH")).To(BeFalse(), "no WATCH should be sent for a no-key Watch")
		Expect(rec.has("UNWATCH")).To(BeFalse())
	})

	It("does not send a second UNWATCH when fn already called Unwatch", func() {
		rec, err := watchRecorded(func(tx *redis.Tx) error {
			return tx.Unwatch(ctx).Err()
		}, "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.count("UNWATCH")).To(Equal(1), "Close must not re-issue UNWATCH after a manual Unwatch")
	})

	It("does not send UNWATCH when WATCH itself fails", func() {
		rec := &cmdRecorder{}
		client.AddHook(rec) // must be added before the injector so it records the short-circuited command
		client.AddHook(&cmdErrInjector{name: "watch", err: errors.New("watch rejected")})
		err := client.Watch(ctx, func(tx *redis.Tx) error {
			return nil
		}, "key")
		Expect(err).To(HaveOccurred())
		Expect(rec.has("WATCH")).To(BeTrue())
		Expect(rec.has("UNWATCH")).To(BeFalse(), "a failed WATCH armed nothing; Close must not UNWATCH")
	})

	It("records no UNWATCH server-side after a committed EXEC (commandstats)", Label("NonRedisEnterprise"), func() {
		Expect(client.Do(ctx, "config", "resetstat").Err()).NotTo(HaveOccurred())

		err := client.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "value", 0)
				return nil
			})
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		calls := commandCalls(client)
		Expect(calls).To(HaveKey("watch"))
		Expect(calls).To(HaveKey("exec"))
		Expect(calls).NotTo(HaveKey("unwatch"))
	})

	// --- the watch is NOT cleared (no EXEC): UNWATCH must still be sent ---

	It("sends UNWATCH when fn returns an error before EXEC", func() {
		expectUnwatchSent(func(tx *redis.Tx) error {
			return errors.New("bail")
		}, "key")
	})

	It("sends UNWATCH when fn reads and decides not to write", func() {
		expectUnwatchSent(func(tx *redis.Tx) error {
			_, err := tx.Get(ctx, "key").Result()
			if err != nil && err != redis.Nil {
				return err
			}
			return nil // no TxPipelined -> no EXEC
		}, "key")
	})

	It("sends UNWATCH when fn uses only a non-transactional pipeline", func() {
		expectUnwatchSent(func(tx *redis.Tx) error {
			_, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Get(ctx, "key")
				return nil
			})
			return err // Pipelined, not TxPipelined -> no MULTI/EXEC
		}, "key")
	})

	It("sends UNWATCH when TxPipelined queues no commands", func() {
		expectUnwatchSent(func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				return nil // nothing queued -> exec closure never runs -> no EXEC
			})
			return err
		}, "key")
	})

	It("sends UNWATCH when bailing with multiple keys watched", func() {
		expectUnwatchSent(func(tx *redis.Tx) error {
			return errors.New("bail")
		}, "{tag}.a", "{tag}.b", "{tag}.c")
	})

	It("sends UNWATCH when a WATCH is re-armed after a committed EXEC", func() {
		rec, err := watchRecorded(func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "value", 0)
				return nil
			}); err != nil {
				return err
			}
			// EXEC cleared the watch; re-arm it and bail without a second EXEC.
			return tx.Watch(ctx, "key").Err()
		}, "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.has("EXEC")).To(BeTrue())
		Expect(rec.has("UNWATCH")).To(BeTrue(), "re-armed WATCH must be released by Close")
	})

	It("sends UNWATCH when fn panics", func() {
		rec := &cmdRecorder{}
		client.AddHook(rec)
		Expect(func() {
			_ = client.Watch(ctx, func(tx *redis.Tx) error {
				panic("boom")
			}, "key")
		}).To(Panic())
		Expect(rec.has("WATCH")).To(BeTrue())
		Expect(rec.has("UNWATCH")).To(BeTrue(), "deferred Close must UNWATCH during panic unwinding")
	})

	It("sends UNWATCH when the EXEC pipeline fails before EXEC runs", func() {
		rec := &cmdRecorder{}
		client.AddHook(rec) // must be added before the injector so it records the short-circuited batch
		// Inject a non-TxFailedErr server error for the MULTI/EXEC batch so EXEC
		// never executes and the watch stays armed (mirrors -LOADING on MULTI).
		// redis.Nil is a RedisError other than TxFailedErr.
		client.AddHook(&pipelineErrInjector{err: redis.Nil})
		err := client.Watch(ctx, func(tx *redis.Tx) error {
			_, e := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "value", 0)
				return nil
			})
			return e
		}, "key")
		Expect(err).To(HaveOccurred())
		Expect(rec.has("WATCH")).To(BeTrue())
		Expect(rec.has("UNWATCH")).To(BeTrue(), "a pre-EXEC error must not elide UNWATCH; the watch is still armed")
	})

	It("records UNWATCH server-side when fn returns before EXEC (commandstats)", Label("NonRedisEnterprise"), func() {
		Expect(client.Do(ctx, "config", "resetstat").Err()).NotTo(HaveOccurred())

		err := client.Watch(ctx, func(tx *redis.Tx) error {
			return errors.New("bail")
		}, "key")
		Expect(err).To(MatchError("bail"))

		calls := commandCalls(client)
		Expect(calls).To(HaveKey("watch"))
		Expect(calls).To(HaveKey("unwatch"))
		Expect(calls).NotTo(HaveKey("exec"))
	})

	// --- optimistic abort: EXEC ran (returned TxFailedErr), watch is cleared ---

	It("does not send UNWATCH after an aborted EXEC (optimistic lock)", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "key", "0", 0).Err()).NotTo(HaveOccurred())

		rec, err := watchRecorded(func(tx *redis.Tx) error {
			// Dirty the watched key from another connection before EXEC.
			if e := other.Set(ctx, "key", "changed", 0).Err(); e != nil {
				return e
			}
			_, e := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "new", 0)
				return nil
			})
			return e
		}, "key")
		Expect(err).To(MatchError(redis.TxFailedErr))
		Expect(rec.has("EXEC")).To(BeTrue(), "EXEC must have run")
		Expect(rec.has("UNWATCH")).To(BeFalse(), "aborted EXEC already cleared the watch")
	})

	It("records no UNWATCH server-side after an aborted EXEC (commandstats)", Label("NonRedisEnterprise"), func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "key", "0", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Do(ctx, "config", "resetstat").Err()).NotTo(HaveOccurred())

		err := client.Watch(ctx, func(tx *redis.Tx) error {
			if e := other.Set(ctx, "key", "changed", 0).Err(); e != nil {
				return e
			}
			_, e := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "key", "new", 0)
				return nil
			})
			return e
		}, "key")
		Expect(err).To(MatchError(redis.TxFailedErr))

		calls := commandCalls(client)
		Expect(calls).To(HaveKey("watch"))
		Expect(calls).To(HaveKey("exec")) // EXEC ran and aborted
		Expect(calls).NotTo(HaveKey("unwatch"))
	})

	It("does not send UNWATCH after an EXECABORT", func() {
		rec, err := watchRecorded(func(tx *redis.Tx) error {
			_, e := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				// SET with a missing argument is rejected at queue time, so EXEC
				// returns EXECABORT and discards the transaction (and the watch).
				pipe.Do(ctx, "set", "key")
				return nil
			})
			return e
		}, "key")
		Expect(err).To(HaveOccurred())
		Expect(redis.IsExecAbortError(err)).To(BeTrue())
		Expect(rec.has("EXEC")).To(BeTrue(), "EXEC must have run")
		Expect(rec.has("UNWATCH")).To(BeFalse(), "EXECABORT already discarded the watch")
	})

	// --- pool safety: a bailed transaction must not leak a watch ---

	It("does not leak a watch onto a pooled connection", func() {
		opt := redisOptions()
		opt.PoolSize = 1
		opt.MinIdleConns = 0
		c := redis.NewClient(opt)
		defer c.Close()
		Expect(c.Set(ctx, "guard", "0", 0).Err()).NotTo(HaveOccurred())

		errBail := errors.New("bail")
		err := c.Watch(ctx, func(tx *redis.Tx) error {
			return errBail
		}, "guard")
		Expect(err).To(Equal(errBail))

		// Mutate the watched key on the (single) reused connection.
		Expect(c.Incr(ctx, "guard").Err()).NotTo(HaveOccurred())

		// An unrelated transaction on the reused connection must not be aborted
		// by a leftover WATCH on "guard"; a leak would surface as TxFailedErr.
		err = c.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "fresh", "ok", 0)
				return nil
			})
			return err
		})
		Expect(err).NotTo(HaveOccurred(), "a leaked WATCH would abort this transaction with TxFailedErr")
		Expect(c.Get(ctx, "fresh").Val()).To(Equal("ok"))
	})
})
