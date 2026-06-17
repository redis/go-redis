# Cluster routing for new commands

Read this only when the new command is keyless, fans out, targets multiple keys, or aggregates results across shards. **Most simple single-key commands need no cluster-side change** — slot routing is inferred from the key argument automatically.

This file (the skill reference) is a wiring checklist. For the full routing *design* — slot computation, MOVED/ASK, aggregator internals — read the separate architecture spec at the repo path `.claude/specs/cluster-routing.md` (different file, same name) before changing routing code.

## When a command needs wiring

| Command shape | What to wire |
|---------------|--------------|
| Keyless (e.g. `DBSIZE`, `FLUSHALL`, server-info commands) | Add the lowercase name to `keylessCommands` (`command.go:25`). Otherwise `ClusterClient` can't pick a node. |
| Unusual key position (key not the first arg) | Update the key-position / key-spec maps in `command.go`. |
| Fan-out across all shards, results aggregated | Add a request/response policy in `command_policy_resolver.go` and the matching aggregator in `internal/routing/aggregator.go` (policy declared in `internal/routing/policy.go`). |
| Multi-key, cross-slot | Confirm the cross-slot rules in `.claude/specs/cluster-routing.md`; commands spanning slots error unless the command supports it. |

## Source of truth = the command spec

The spec's `key_specs` and `command_flags` tell you the routing class:
- empty `key_specs` → keyless → `keylessCommands`.
- `command_flags` with no write flag → `READONLY` → eligible for replica routing.
- multiple key specs / `numkeys`-style args → multi-key; check cross-slot handling.

## Verify

After wiring, exercise against the cluster profile:

```sh
go test -run TestGinkgoSuite . -ginkgo.focus="cluster"
```
