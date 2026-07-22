---
description: Summarize GitHub Actions CI status for the current branch's PR (build, lint, e2e, govulncheck, ...)
argument-hint: "[pr-number]"
allowed-tools: Bash(gh pr checks:*), Bash(gh pr view:*), Bash(gh run view:*), Bash(gh pr list:*), Bash(git branch:*)
---

## Context

- Branch: !`git branch --show-current`
- PR: !`gh pr view $1 --json number,title,url,state 2>/dev/null || echo "NO_PR"`
- Checks (name / state / elapsed / url): !`gh pr checks $1 2>&1 || true`

## Task

Report the CI state for this PR. If the PR line is `NO_PR`, say there is no open PR for this branch and stop.

Otherwise, from the checks block:

1. **Counts first.** One line: `N passing · M failing · K pending` (and skipped/cancelled if any).
2. **Failures and pending only.** For each check that is not passing, list its name, state, and the log URL. Skip the passing ones, they are noise.
3. **Prioritize.** Order failures by what blocks a merge most: `test-redis-ce` (the main correctness matrix) and `E2E Tests (Mock Proxy)` first, then `lint` and `govulncheck`, then `CodeQL` / `doctests` / `check-spelling`, then everything else (benchmarks last).
4. **Verdict.** One closing line:
   - All green → "All checks pass, nothing to do."
   - Any red → name the single most important failing check to look at first.
   - Only pending → say which are still running and that it is not done yet.

Keep it short. No preamble, no restating this prompt. The argument `$1` is an optional PR number; when omitted, the current branch's PR is used.
