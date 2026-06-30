---
name: prepare-release
description: Use when preparing a go-redis release — cutting a new version, bumping version.go, updating the go-redis dependency version in submodule go.mod files, or writing the RELEASE-NOTES.md entry for a new vX.Y.Z. Covers choosing the next semver, finding the last release and gathering merged PRs since then, the release-notes format and what to exclude, running scripts/release.sh to bump versions, and verifying with the scripts/tag.sh dry-run. Does NOT create tags, push, or commit — publishing stays a manual maintainer step.
---

# Preparing a go-redis release

Stage everything for a release locally, then stop before anything is published.

## This skill never publishes

- **Never** run `scripts/tag.sh ... -t` — the `-t` flag creates and pushes git tags.
- **Never** `git push` anything (no commits, no tags).
- **Never** commit on the maintainer's behalf unless they explicitly ask.

The deliverable is a reviewable diff: bumped versions plus a new `RELEASE-NOTES.md`
entry. A human reviews it and runs the publish step.

## 1. Pick the next version

The current version is the source of truth in `version.go`:

```sh
grep 'return' version.go        # e.g. return "9.21.0"
```

Choose the next `vX.Y.Z` by semver, based on what shipped since the last release:

- **patch** (`Z`) — bug fixes only; drop-in upgrade.
- **minor** (`Y`) — new features, no breaking changes; drop-in upgrade.
- **major** (`X`) — breaking changes.

Confirm the level with the user if the changeset is ambiguous.

## 2. Find the last release and gather changes

`scripts/tag.sh` also tags every public submodule (`extra/redisotel/vX.Y.Z`, …),
so a naive `git describe` returns a submodule tag. Match the **root** tag only:

```sh
LAST=$(git describe --tags --abbrev=0 --match 'v[0-9]*')   # e.g. v9.21.0
git log "$LAST"..HEAD --oneline
gh pr list --state merged --limit 100 \
  --json number,title,author,mergedAt,url --search "merged:>=<last-release-date>"
```

Categorize the PRs: highlights, new features, bug fixes, performance,
testing/infrastructure. Exclude dependabot bumps, typo-only doc fixes, internal
refactors with no user-facing effect, and `dependabot[bot]` from the contributor
list.

## 3. Write the release notes

Prepend a new `# X.Y.Z (YYYY-MM-DD)` section to the **top** of `RELEASE-NOTES.md`
(newest first; leave older entries untouched). Follow
[`.github/RELEASE_NOTES_TEMPLATE.md`](../../../.github/RELEASE_NOTES_TEMPLATE.md)
exactly — section order, emoji headers, the `([#PR](url)) by [@user](url)` link
format, and the `**Full Changelog**` compare link `${LAST}...vX.Y.Z`. Open the
lead line with the release type and whether it is a drop-in upgrade, matching the
existing entries.

That template carries the full "what to exclude" and formatting rules — read it,
don't reinvent them. `release-drafter` separately auto-drafts a GitHub release
from PR labels (`.github/release-drafter-config.yml`); `RELEASE-NOTES.md` is the
curated, hand-written record and is the file you edit.

## 4. Bump versions

Run the repo's bump script. It rewrites the go-redis dependency version in every
submodule `go.mod`, runs `go mod tidy`, and bumps `version.go`. It does **not**
commit, push, or switch branches:

```sh
TAG=vX.Y.Z ./scripts/release.sh
```

## 5. Verify (dry run only)

Run the tag script's **dry run** — it checks that `version.go` and every `go.mod`
already match the tag, and prints the tags it *would* push. No `-t`:

```sh
./scripts/tag.sh vX.Y.Z         # DRY RUN — must pass cleanly; never add -t here
make build
git diff --stat                 # review version.go, submodule go.mod, RELEASE-NOTES.md
```

Fix anything the dry run flags before handing off.

## 6. Hand off

Report that the release is staged and list the publish steps for the maintainer
to run themselves (this skill does not do them):

1. Review `git diff`, then commit (`chore(release): vX.Y.Z` — see the
   `commit-style` skill; no AI-attribution trailer).
2. Open and merge the release PR.
3. After merge, tag and push: `./scripts/tag.sh vX.Y.Z -t`.
4. `release-drafter` publishes the GitHub release; reconcile it with the
   `RELEASE-NOTES.md` entry if needed.

Stop after step 5. Do not commit, tag, or push.
