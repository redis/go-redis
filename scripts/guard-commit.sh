#!/usr/bin/env bash
# Commit regenerated goldens to a PR branch as the guard bot and push.
# Runs no PR code: it only copies validated JSON files and calls git.
#
#   scripts/guard-commit.sh <pr-checkout> <goldens-dir> <expected-head-sha> <head-repo> <head-ref>
#
# Env: GUARD_BOT_LOGIN, GUARD_BOT_NAME, GUARD_BOT_EMAIL, GUARD_BOT_TOKEN,
# GUARD_BOT_SIGNING_KEY (GPG key id, already imported).
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <pr-checkout> <goldens-dir> <expected-head-sha> <head-repo> <head-ref>" >&2
  exit 2
fi
PR_DIR="$(cd "$1" && pwd)"
SRC="$(cd "$2" && pwd)"
EXPECTED="$3"
HEAD_REPO="$4"
HEAD_REF="$5"
: "${GUARD_BOT_LOGIN:?}" "${GUARD_BOT_NAME:?}" "${GUARD_BOT_EMAIL:?}" "${GUARD_BOT_TOKEN:?}" "${GUARD_BOT_SIGNING_KEY:?}"

GOLDENS="internal/guard/testdata"
DEST="$PR_DIR/$GOLDENS"

# Validate the artifact: only regular guard_*.json files, valid JSON, bounded size.
shopt -s nullglob
files=("$SRC"/*)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "guard-commit: no goldens in $SRC" >&2
  exit 1
fi
for f in "${files[@]}"; do
  name="$(basename -- "$f")"
  if [[ ! "$name" =~ ^guard_[0-9]+_[a-z0-9_]+\.json$ || -L "$f" || ! -f "$f" ]]; then
    echo "guard-commit: unexpected file in artifact: $name" >&2
    exit 1
  fi
  if (($(wc -c <"$f") > 10 * 1024 * 1024)); then
    echo "guard-commit: $name is larger than 10 MiB" >&2
    exit 1
  fi
  jq empty "$f"
done

mkdir -p -- "$DEST"
find "$DEST" -maxdepth 1 -type f -name 'guard_*.json' -delete
cp -- "${files[@]}" "$DEST/"

g() { git -C "$PR_DIR" -c core.hooksPath=/dev/null "$@"; }

if [[ "$(g rev-parse HEAD)" != "$EXPECTED" ]]; then
  echo "guard-commit: checkout is not at $EXPECTED" >&2
  exit 1
fi

g add -A -- "$GOLDENS"
if g diff --cached --quiet; then
  echo "guard-commit: goldens already up to date"
  exit 0
fi
staged="$(g diff --cached --name-only | grep -v "^$GOLDENS/" || true)"
if [[ -n "$staged" ]]; then
  echo "guard-commit: refusing to commit files outside $GOLDENS: $staged" >&2
  exit 1
fi

g -c user.name="$GUARD_BOT_NAME" -c user.email="$GUARD_BOT_EMAIL" \
  -c user.signingkey="$GUARD_BOT_SIGNING_KEY" -c commit.gpgsign=true \
  commit -q -S -m "test(guard): regenerate goldens" -m "Requested via the guard-update label on this PR."
g verify-commit HEAD

auth="$(printf '%s:%s' "$GUARD_BOT_LOGIN" "$GUARD_BOT_TOKEN" | base64 | tr -d '\n')"
url="https://github.com/$HEAD_REPO.git"
remote_head="$(g -c http.extraheader="AUTHORIZATION: basic $auth" ls-remote "$url" "refs/heads/$HEAD_REF" | cut -f1)"
if [[ "$remote_head" != "$EXPECTED" ]]; then
  echo "guard-commit: $HEAD_REPO:$HEAD_REF moved ($remote_head != $EXPECTED); re-run guard-update" >&2
  exit 1
fi
g -c http.extraheader="AUTHORIZATION: basic $auth" push "$url" "HEAD:refs/heads/$HEAD_REF"
echo "guard-commit: pushed $(g rev-parse --short HEAD) to $HEAD_REPO:$HEAD_REF"
