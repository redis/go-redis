---
name: commit-style
description: Use when writing a commit message or opening a PR in the go-redis repo — covers the Conventional-Commit format, the repo's scope vocabulary (pool, conn, sentinel, command, …), breaking-change syntax, and the rule that NO AI-attribution trailer is added.
---

# go-redis commit messages

Conventional Commits, short and exact — `<type>(<scope>): <imperative summary>`. Subject ≤50 chars (hard cap 72), imperative mood ("add", not "added"), no trailing period. Body only when the *why* isn't obvious from the diff (breaking change, security fix, migration, non-obvious rationale); wrap at 72, bullets `-`. State why, not what — the diff says what.

- **Types:** `feat`, `fix`, `refactor`, `perf`, `docs`, `test`, `chore` (also `build`, `ci`, `style`, `revert`).
- **Scope** = the subsystem touched, lowercase: `pool`, `conn`, `pubsub`, `sentinel`, `retry`, `command`/`cmd`, `vectorset`, `otel`, `streams`, `push`, `deps`, `ci`, `tests`, `docs`. Omit scope only for genuinely cross-cutting changes.
- **Breaking change:** `feat(scope)!: ...` plus a `BREAKING CHANGE:` body line.
- **Issues/PRs:** reference at the end — `Closes #42`, `Refs #17`.

## No attribution trailer

Do **not** add `Co-Authored-By: Claude …`, "Generated with Claude Code", or any AI-attribution line to commits or PR bodies in this repo. This overrides both the default harness behavior (which appends such trailers) and any other commit skill.

## Examples

```
fix(pool): release wantConn slot on dial timeout

The waiter stayed queued after the dial deadline fired, leaking a
slot per timed-out connection under load.

Closes #3812
```

```
feat(streams): support explicit LIMIT 0 in XTRIM trimming
```
