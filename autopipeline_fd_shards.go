package redis

// SPIKE, NOT SHIPPABLE. Multi-connection full duplex inside the client, to
// answer one question: is the ~2x measured with N separate *redis.Client
// instances a property of N engines, or an artifact of N clients?
//
// The benchmark harness fakes multi-connection FD by building N clients, each
// with its own autopipeliner and therefore its own engine, and round-robining
// across them. That holds 6 sockets per client (one held wire plus five idle
// pool conns) to do I/O on one, so eight "wires" held 48 sockets. It also
// cannot test key routing at all, because the round-robin lives in the harness.
//
// Here the engines live on ONE client: one pool, one hook chain, one
// invalidation stream, N held wires. Routing is by key hash so same-key
// commands keep their order, which is the rule the half-duplex path already
// applies to shards (see the contentSharded exemption in newAutoPipeliner).
//
// What this spike deliberately does NOT do: resize the pipeline pool. Engines
// lease from it and DefaultPipelinePoolSize is 10, so more than ~10 engines
// would spill to the main pool and measure something else. A real feature must
// size that pool from the engine count.

import (
	"hash/fnv"
)

// fdShardCount reports how many engines to run. 0/1 mean the single engine, and
// anything higher only applies on the full-duplex path.
func fdShardCount(cfg *AutoPipelineOptions) int {
	if !cfg.FullDuplex || cfg.NumShards <= 1 {
		return 1
	}
	return cfg.NumShards
}

// fdFor picks the engine for one command.
//
// Key hash, not round-robin. Round-robin would put `SET k` and `GET k` issued
// back-to-back on the deferred face onto different engines with no ordering
// between them, which is a behavior change on a released face; hashing the key
// keeps same-key commands on one wire, so per-key order holds without asking
// callers to opt into Unordered. Commands with no key round-robin instead,
// because there is no per-key order to lose.
func (ap *AutoPipeliner) fdFor(cmd Cmder) *fdEngine {
	if len(ap.fds) <= 1 {
		return ap.fd
	}
	idx := -1
	if k := cmdFirstKey(cmd); k != "" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(k))
		idx = int(h.Sum32()) % len(ap.fds)
	} else {
		idx = int(ap.fdRR.Add(1)) % len(ap.fds)
	}
	if fdShardTrace != nil {
		fdShardTrace(cmd, idx)
	}
	return ap.fds[idx]
}

// fdShardTrace is spike instrumentation: set from a test to observe which
// engine each command was routed to.
var fdShardTrace func(cmd Cmder, idx int)

// cmdFirstKey returns the command's first key argument, or "" when it has none.
//
// Deliberately crude for a spike: args[1] is the key for the overwhelming
// majority of commands, which is enough to measure routing cost and balance. A
// real implementation must use the command's declared key positions (the
// cluster path already computes this).
func cmdFirstKey(cmd Cmder) string {
	args := cmd.Args()
	if len(args) < 2 {
		return ""
	}
	if s, ok := args[1].(string); ok {
		return s
	}
	return ""
}
