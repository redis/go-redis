package redis

// Multi-connection full duplex: N engines behind ONE autopipeliner.
//
// A single full-duplex engine streams on one held connection, and one
// connection has a throughput ceiling — measured at roughly 920k ops/s on a
// 14-core client against a local server, for both this client and rueidis. Past
// that the engine does not saturate, it degrades, because callers queue behind
// a wire that is already busy. Raising NumShards above 1 under FullDuplex runs
// several engines on the same client, each holding its own connection, which is
// the only way past that ceiling.
//
// Everything stays on ONE client: one connection pool, one hook chain, one
// invalidation stream. Only the engines multiply.
//
// ROUTING IS BY KEY, NOT ROUND-ROBIN, and that choice is load-bearing. Commands
// for the same key always take the same engine, so per-key order holds and no
// Unordered opt-in is needed — the same exemption the half-duplex path already
// grants cluster slot sharding (see the contentSharded note in
// newAutoPipeliner). Round-robin would put `SET k` and `GET k`, issued
// back-to-back as futures on the deferred face, on two engines with no ordering
// between them, which is a silent behaviour change on a released face.
//
// ORDER IS PER FIRST KEY, NOT PER COMMAND STREAM. A single engine keeps the
// whole submit order; N engines keep only the order of commands that share a
// first key. A command that touches other keys after its first one (COPY,
// RENAME, SMOVE, LMOVE, MSET, DEL/UNLINK/EXISTS with several keys, the
// *STORE family, EVAL with several keys) is routed by its first key only. So
// `Copy(src, dst)` followed by `Get(dst)` can run in either order: they may be
// on different engines. Cluster sharding never meets this case because Redis
// Cluster rejects multi-key commands across slots (CROSSSLOT); a standalone
// server accepts them. Callers that chain such commands should await the
// first future before submitting the dependent one, or keep NumShards at 1.
//
// For comparison, rueidis spreads keyed commands over its standalone wires at
// random (mux.go slotfn, 4 wires by default on 4+ cores) and orders nothing
// across them; its Do is synchronous, so there are no in-flight futures to
// reorder. Only a DoMulti batch shares one wire.
//
// This is an opt-in: before this change, NumShards>1 never ran more than one
// full-duplex engine, so every existing caller keeps the single ordered stream
// and only a caller that raises NumShards on a full-duplex autopipeliner
// accepts per-first-key order.
//
// The cost of that choice is measurable and worth stating: pinning a caller to
// one engine means an unlucky key waits behind that engine's queue while
// another sits idle. At eight wires and 4096 callers this shows up as a wider
// tail than a balancing client achieves (p99.9 77.6 ms against 59.9 ms), and
// the p99.9/p50 ratio rises from 1.2x on a single engine to 3.2x on eight. A
// single FIFO cannot starve anyone; any split gives that up. Ordering was
// judged the more valuable property, but a selectable policy is the obvious
// follow-up for callers who would rather have the tail.

import (
	"hash/fnv"
)

// fdShardCount reports how many full-duplex engines to run.
//
// 0 and 1 both mean a single engine, which is the default and leaves the
// single-engine path untouched. Sizing is deliberately NOT automatic yet: the
// optimum tracks caller count (roughly one wire per 256-512 callers), and a
// fixed count above 1 costs low-concurrency callers real throughput — eight
// engines measured ~30% behind one at 8 callers, because a handful of callers
// cannot fill eight queues and every batch fragments. Until the count can be
// derived from observed load, it stays the caller's explicit decision.
func fdShardCount(cfg *AutoPipelineOptions) int {
	if !cfg.FullDuplex || cfg.NumShards <= 1 {
		return 1
	}
	return cfg.NumShards
}

// fdFor picks the engine that will carry one command.
//
// The single-engine case returns before doing any work, so the default
// configuration pays one length check per command and nothing else.
func (ap *AutoPipeliner) fdFor(cmd Cmder) *fdEngine {
	if len(ap.fds) <= 1 {
		return ap.fd
	}
	// Keyed commands hash to a stable engine so per-key order holds. Keyless
	// ones have no order to preserve, so they round-robin and keep the engines
	// evenly loaded. Modulo on uint32: converting to int first goes negative
	// on 32-bit platforms and panics as a slice index.
	n := uint32(len(ap.fds))
	if k, ok := cmdFirstKeyFor(cmd); ok {
		h := fnv.New32a()
		_, _ = h.Write([]byte(k))
		return ap.fds[h.Sum32()%n]
	}
	return ap.fds[(ap.fdRR.Add(1)-1)%n]
}

// cmdFirstKeyFor returns the command's first key and whether it has one.
//
// Uses the shared key-position logic rather than assuming args[1]: that handles
// keyless commands, the static command table, and eval/evalsha variants whose
// key position depends on the runtime numkeys argument. Passing a nil
// CommandInfo keeps this synchronous and network-free, the same way the
// client-side-cache path calls it.
//
// The key is read with stringArg, as Ring does, so a []byte, *string or
// numeric key hashes like the same key given as a string. The key position,
// not the key text, decides whether the command is keyed, so an empty-string
// key still routes by hash.
func cmdFirstKeyFor(cmd Cmder) (string, bool) {
	pos := cmdFirstKeyPosWithInfo(cmd, nil)
	if pos <= 0 || pos >= len(cmd.Args()) {
		return "", false
	}
	return cmd.stringArg(pos), true
}
