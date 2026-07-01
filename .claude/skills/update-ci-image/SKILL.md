---
name: update-ci-image
description: Use when adding, updating, or removing a `redislabs/client-libs-test` image tag used by the test stack — covers every place a Redis version → image-tag mapping lives so local `make test` and GitHub CI stay in sync.
---

# Updating the CI test image

The `redislabs/client-libs-test` image is referenced in **six** places (five live config + one doc). Miss one and either local `make test` and CI diverge, or a single CI job uses the wrong image. Always edit all relevant locations in one change.

## The six locations

| # | File | What lives here |
|---|------|-----------------|
| 1 | `Makefile` | `CLIENT_LIBS_TEST_IMAGE ?= redislabs/client-libs-test:<tag>` — default for local `make test` / `make docker.start`. |
| 2 | `docker-compose.yml` | `x-default-image: &default-image ${CLIENT_LIBS_TEST_IMAGE:-redislabs/client-libs-test:<tag>}` — fallback when the env var is unset. Should match the Makefile default. |
| 3 | `.github/workflows/build.yml` | `redis_version_mapping` bash assoc array (`["8.X.x"]="<tag>"`). Drives the `benchmark` job and the `test-redis-ce` matrix via `run-tests`. |
| 4 | `.github/actions/run-tests/action.yml` | Identical `redis_version_mapping` array — the composite action that the matrix jobs invoke. Keep in lockstep with `build.yml`. |
| 5 | `.github/workflows/doctests.yaml` | Hardcoded `image: redislabs/client-libs-test:<tag>` on the `redis-stack` service — doctests do not consult the mapping. |
| 6 | `CONTRIBUTING.md` | Prose line "By default the docker image … is `redislabs/client-libs-test:<tag>`." Documentation only — no CI impact, but goes stale silently. Update it to match the Makefile default. |

## Tasks

### Update the tag for an existing Redis version (e.g. bump 8.8 → a new release)

1. `Makefile` — replace the tag after `redislabs/client-libs-test:` on the `CLIENT_LIBS_TEST_IMAGE` line.
2. `docker-compose.yml` — replace the tag in the `:-redislabs/client-libs-test:<tag>` default.
3. `.github/workflows/build.yml` — update the `["8.X.x"]="<tag>"` line in `redis_version_mapping`.
4. `.github/actions/run-tests/action.yml` — update the matching `["8.X.x"]="<tag>"` line.
5. `.github/workflows/doctests.yaml` — update the `image:` line **only if** doctests should run against this version. Doctests typically pin to the latest stable; check before changing.

If the version that doctests run against (`Makefile` default) and the mapping for that same minor version diverge, you have a bug — they should resolve to the same tag.

### Add a new Redis version to the CI matrix

1. `.github/workflows/build.yml`:
   - Add `- "<X.Y>.x"` to **both** `redis-version` matrices (the `benchmark` job and the `test-redis-ce` job — they have separate lists).
   - Add `["<X.Y>.x"]="<tag>"` to `redis_version_mapping`.
2. `.github/actions/run-tests/action.yml` — add the same `["<X.Y>.x"]="<tag>"` entry to its `redis_version_mapping`.
3. If this becomes the new default for local dev, also update `Makefile` (`REDIS_VERSION ?=` and `CLIENT_LIBS_TEST_IMAGE ?=`) and `docker-compose.yml`.

### Remove a Redis version

1. `.github/workflows/build.yml` — drop the `- "<X.Y>.x"` line from both `redis-version` matrices and the `["<X.Y>.x"]="..."` entry.
2. `.github/actions/run-tests/action.yml` — drop the same `["<X.Y>.x"]="..."` mapping entry.
3. If `Makefile` / `docker-compose.yml` / `doctests.yaml` were pinned to this version, pick a replacement (usually the next-newest version still in the matrix) and update them too.

### Switch to a custom build (e.g. `custom-26172898734-debian` for an unreleased server feature)

Same as "update the tag," but think about scope:
- Custom tags usually correspond to a single Redis minor — only touch the mapping entry for that minor, not all five rows.
- Custom tags often shouldn't be the local default if they're transient. Confirm with the user before touching `Makefile` / `docker-compose.yml`.
- Leave `REDIS_VERSION` (the numeric `8.8` / `8.6` etc.) alone — it drives `SkipBeforeRedisVersion` gating in `main_test.go`, not the image. The custom image still reports its base version.

## Verification

After editing, search for any stale tag references:

```sh
# Use the Grep tool, not bash grep.
# Pattern: literal old tag, e.g. "8.8-rc1" or "client-libs-test:8.8"
```

Expected matches: zero in `Makefile`, `docker-compose.yml`, `.github/**`. A reference in `CLAUDE.md` documentation (e.g. "e.g. `redislabs/client-libs-test:8.8-m03`") is illustrative — leave it unless the doc is misleading after the change.

## Gotchas

- **Two mappings, not one.** `build.yml` and `run-tests/action.yml` each declare their own `redis_version_mapping`. They are not deduped. Always edit both.
- **Two matrices in `build.yml`.** The `benchmark` job and the `test-redis-ce` job each have their own `redis-version` list. Adding/removing a version means editing both.
- **`doctests.yaml` is hardcoded.** It doesn't read the mapping. If the team intends "doctests follow the latest stable," verify the tag matches `Makefile`'s default after any change.
- **Don't change `REDIS_VERSION` when only swapping the image.** The number gates version-specific test skips (`SkipBeforeRedisVersion(8.8, ...)`). A custom image built off 8.8 should keep `REDIS_VERSION=8.8`.
- **Env var vs. default.** Local users can override with `CLIENT_LIBS_TEST_IMAGE=...`; `docker-compose.yml` uses that env var first and falls back to its baked-in default. So the docker-compose default only matters when the Makefile isn't driving things — but it's still load-bearing for direct `docker compose up` calls. Keep it in sync.
