# Reactive Markets fork of go-redis

|        |                                                                     |
| ------ | ------------------------------------------------------------------- |
| Date   | 2026-08-10                                                          |
| Owner  | Marat Seifullin                                                     |
| Status | v9 port of the v8.11.5-reactive4 changes, based on upstream v9.22.0 |
| Ticket | [SDB-2817](https://reactivemarkets.atlassian.net/browse/SDB-2817)   |

This repository is Reactive Markets' fork of [go-redis](https://github.com/redis/go-redis). It exists to cut GC pressure in hot services: stock go-redis materialises every reply as freshly allocated Go values (each bulk string becomes a new `string`, and typed commands allocate a result container on top — `HGetAll` a map, `ZRangeWithScores` a `[]Z`). For large periodic snapshot reads in `reactive-go` — reference-data IAM hashes, credit NOP limits, account/market mappings — the fork adds a zero-copy escape hatch: the caller supplies a function that parses the raw RESP reply directly into its own reusable buffers.

## What the fork adds on top of upstream

- `CustomCmd` (`command.go`) — a `Cmder` whose `readReply` delegates to a caller-supplied `func(*proto.Reader) error`, so the caller parses the raw reply itself. Modelled on upstream's `ZeroCopyStringCmd`: `NoRetry()` returns true (a retry would re-invoke the callback and could duplicate whatever a failed first attempt already produced), and `Clone()` returns a deliberately non-functional clone whose `readReply` drains the reply and errors.
- `Reader` and `NewReader` (`commands.go`) — a public alias re-exporting `internal/proto.Reader`, so callbacks can be typed and exercised in tests without a live connection (the `reactive-go` mocks depend on both).
- `HGetAllWithCustomReader` (`hash_commands.go`) and `ZRangeWithScoresWithCustomReader` / `ZRangeArgsWithScoresWithCustomReader` (`sortedset_commands.go`) — command variants that issue the normal wire command but hand the reply to the callback. Each is added to the corresponding `Cmdable` sub-interface so it works on clients and pipelines alike.
- A module rename in `go.mod` to `github.com/reactive-go/redis/v9`. See the wiring rules below.

The v8 fork also carried `Reader.ReadStringBuffered`; it was **not** ported because upstream v9 ships `Reader.ReadStringInto`, which does the same buffered read and additionally drains the payload when the buffer is too small, keeping the pooled connection aligned. Use `ReadStringInto` in callbacks. The unused extras from the v8.11.5-reactive5…7 tags (`HValsWithCustomReader`, `SMembersWithCustomReader`, plain-`ZRange` variants, `ReadStringToWriter`) were dropped — nothing consumes them, and upstream's buffer APIs cover the same ground. `CustomCmd.Result()` from reactive5 was kept.

## The callback contract

The reader function must consume the entire reply, exactly. The `proto.Reader` belongs to a pooled connection: bytes left unread (or read beyond the reply) desynchronise the connection for whatever command uses it next. Practical guidance:

- For `HGetAllWithCustomReader`, start with `rd.ReadMapLen()` — it accepts both the RESP3 map reply and the RESP2 flat array reply and returns the pair count either way.
- For the zset variants, start with `rd.ReadArrayLen()` and mind the protocol: RESP2 returns a flat array of member,score pairs; RESP3 an array of two-element member,score arrays (see upstream's `ZSliceCmd.readReply` for the canonical branch).
- Read each element with `rd.ReadStringInto(buf)` into a caller-owned buffer.

`custom_cmd_test.go` demonstrates the pattern under both protocols and pins the drain/no-retry behaviour.

## How the module wiring works — do not "fix" it

`go.mod` declares `module github.com/reactive-go/redis/v9`, while every import inside the fork still says `github.com/redis/go-redis/v9/...`. Both are required:

- The declared module path must match the fork's URL so Go can fetch a tag of this repository.
- Consumers keep importing the upstream path and map it with a replace directive, which also resolves the fork's own internal imports back into the fork: `replace github.com/redis/go-redis/v9 => github.com/reactive-go/redis/v9 <tag>`.

A consequence: the fork does not build standalone once the module is renamed (its internal imports only resolve through a consumer's replace directive). To develop or run its tests locally, temporarily set the module line back to `github.com/redis/go-redis/v9`, work, then restore the rename before tagging. Rewriting the internal imports instead was tried on the v8 fork (commit `23eb2762`) and had to be reverted.

## Versioning and upgrade procedure

Tag every published state `v<upstream>-reactiveN` (for example `v9.22.0-reactive1`) and never move an existing tag — the Go module proxy caches tags immutably. To rebase onto a newer upstream release:

```bash
git remote add upstream https://github.com/redis/go-redis.git
git fetch upstream --tags
git checkout -b feature/v9.X.Y-reactive1 v9.X.Y      # the new upstream tag
git cherry-pick <fork commits>                        # or: git rebase --onto v9.X.Y v9.OLD <fork branch>
# resolve conflicts; the go.mod module rename must survive
go test -run TestCustomCmd . && go test ./internal/proto/   # with go.mod temporarily un-renamed
git tag v9.X.Y-reactive1 && git push origin HEAD --tags
```

Then bump the replace directive in the consumer's `go.mod` and run its full build and tests. Before each upgrade, check whether upstream has grown an equivalent API — this port already retired half the v8 fork (`ReadStringBuffered` → upstream `ReadStringInto`), and upstream's `GetToBuffer`/`ZeroCopyStringCmd` show the zero-allocation pattern is landing upstream piece by piece. The fork should shrink over time, not grow.

## Consumers

`reactive-go` is the only consumer. The fork APIs are used by `pkg/db/redis` (generic snapshot loaders and mocks), `pkg/refdata`, `pkg/creditgw`, and `pkg/clmd`. Migrating it from the v8 fork additionally requires: switching imports to `github.com/redis/go-redis/v9`, rewriting callbacks from the removed `ReadArrayReply(func(rd, n))` to `ReadMapLen`/`ReadArrayLen` loops, replacing `ReadStringBuffered` with `ReadStringInto`, and choosing a protocol (`Options.Protocol: 2` keeps the v8 wire behaviour and makes the callback port mechanical).

## History

The original v8 fork ([v8.11.5-reactive4](https://github.com/reactive-go/redis/commits/v8.11.5-reactive4), June 2022, SDB-2817) introduced `CustomCmd`, the `Reader` re-export, `ReadStringBuffered`, and the `HGetAll`/`ZRangeWithScores` custom-reader variants on top of upstream v8.11.5 — the last v8 release before upstream development moved to v9. The v9 port (August 2026) re-implemented the surviving pieces against upstream v9.22.0 as described above.
