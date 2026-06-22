---
name: add-command
description: Use when adding a new Redis command (or RediSearch / TimeSeries / VectorSet / module subcommand) to go-redis — covers fetching the command spec, the Cmder type, Cmdable interface wiring, RESP parsing, tests, and the custom-vet rule that enforces SetVal.
---

# Adding a new Redis command

Router for adding a Redis command to the root `redis` package. Read the reference file for the area you're touching — don't load all of them.

## Step 0 — Get the command spec FIRST

Before writing any Go, know the exact command shape (arguments, optional flags, reply structure, since-version, key positions). Resolve the spec in this order:

1. **Spec file argument** — if the user passed a path to a spec/JSON file, read it.
2. **PR URL argument** — if the user passed a `github.com/redis/redis` PR (or other repo) URL, fetch the diff and read the command definition + `src/commands/<cmd>.json` it adds.
3. **No argument** — fetch the machine-readable spec from redis/redis:
   ```
   https://raw.githubusercontent.com/redis/redis/unstable/src/commands/<command>.json
   ```
   Container subcommands use `<container>-<sub>.json` (e.g. `client-info.json`). **A 404 means it's a module command** (RediSearch, TimeSeries, VectorSet, Bloom) — those specs live in the **module's own repo**, not redis/redis. Switch to the module repo (see `references/module-commands.md` §1) or ask the user for the spec/PR. Don't retry the redis/redis URL.

Map spec fields to the implementation:

| Spec field | Drives |
|------------|--------|
| `arguments` | method signature, args-slice build order, optional `FooArgs` struct |
| `reply_schema` | the `readReply` parser and Cmder result type |
| `since` | `SkipBeforeRedisVersion(...)` in the integration test, doc comment |
| `key_specs` | key-position maps in `command.go`; cluster routing |
| `command_flags` (e.g. `READONLY`, no key) | keyless / fan-out handling, cluster routing |

If you can't get a spec, STOP and ask the user — guessing the reply shape produces a broken `readReply`.

## Decide before you start

1. **Which `*_commands.go` file?** — pick the existing file matching the data type (`string_commands.go`, `hash_commands.go`, `search_commands.go`, …). Only create a new file for a genuinely new category. New files need a matching `XxxCmdable` interface embedded in `Cmdable` (`commands.go`).
2. **Reuse an existing Cmder, or define a new one?** — simple replies (`int`, `string`, `bool`, `[]string`, map) reuse `*IntCmd`, `*StringCmd`, `*BoolCmd`, `*StringSliceCmd`, `*MapStringStringCmd`, … Define a new Cmder only for a structured reply that no existing type fits.
3. **`Foo` vs `FooWithArgs`?** — for commands with optional flags, expose a positional `Foo(...)` for the common path plus `FooWithArgs(ctx, key, *FooArgs)` for the full surface.

## Reference files

| Read this | When |
|-----------|------|
| `references/core-command-pattern.md` | Always — the 7-step Go pattern (interface method, Cmder type, RESP parsing, tests, vet, fmt). Worked example: `LCS`. |
| `references/module-commands.md` | Adding a RediSearch / TimeSeries / VectorSet / Bloom subcommand — naming, RESP2-vs-RESP3 shape differences, where the Cmd type lives. |
| `references/cluster-routing-wiring.md` | Command is keyless, fans out, multi-key, or aggregates across shards. |

When several apply, read in order: core → module → cluster.

## Pitfalls (full list in core-command-pattern.md)

- Forgetting to embed the new `XxxCmdable` interface in `Cmdable` — compiles on `*Client`, silently unreachable via `UniversalClient`.
- Forgetting `Clone()` — pipelines reuse Cmders; shared `val` causes cross-execution bugs.
- Skipping `SetVal` because "nothing calls it" — hooks do, and the `setval` custom-vet check fails the build.
- Using `time.Duration` directly in args — server gets nanoseconds. Convert per spec.