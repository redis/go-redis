#!/usr/bin/env bash
# Guard delta between two trees (internal/guard/doc.go).
#
#   scripts/guard-delta.sh <base-rev> [<head-rev>]
#
# Compares <head-rev> (default: the working tree) with the merge base of
# <base-rev> and <head-rev>, using the guard code of the checkout this
# script is in. Both trees are copied to a temp dir and only parsed; their
# code is never built or run. The working tree copy holds tracked and
# untracked files that are not ignored.
#
# Prints the report and the digest of the changes that need approval.
# Exit status: 0 nothing needs approval, 1 something does, 2 error.
#
# Environment:
#   GUARD_REPO  repository the revisions are read from (default: this checkout)
#   GUARD_BIN   prebuilt guard binary (default: build from this checkout)
# CI sets GOTOOLCHAIN=local so no toolchain is downloaded.
set -euo pipefail
trap 'echo "guard-delta: failed" >&2; exit 2' ERR

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <base-rev> [<head-rev>]" >&2
  exit 2
fi
BASE_REV="$1"
HEAD_REV="${2:-}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "${GUARD_REPO:-$HERE}" && pwd)"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/guard-delta.XXXXXX")"
cleanup() {
  case "$TMP" in
  */guard-delta.*) rm -rf -- "$TMP" ;;
  esac
}
trap cleanup EXIT

BIN="${GUARD_BIN:-}"
if [[ -z "$BIN" ]]; then
  BIN="$TMP/guard"
  (cd "$HERE" && go build -o "$BIN" ./internal/guard/cmd/guard)
fi

if [[ -n "$HEAD_REV" ]]; then
  HEAD_SHA="$(git -C "$REPO" rev-parse --verify --quiet "$HEAD_REV^{commit}")"
  HEAD_LABEL="$HEAD_REV (${HEAD_SHA:0:12})"
else
  HEAD_SHA="$(git -C "$REPO" rev-parse --verify --quiet "HEAD^{commit}")"
  HEAD_LABEL="working tree (HEAD ${HEAD_SHA:0:12})"
fi
git -C "$REPO" rev-parse --verify --quiet "$BASE_REV^{commit}" >/dev/null
MB="$(git -C "$REPO" merge-base "$BASE_REV" "$HEAD_SHA")"
BASE_LABEL="merge base of $BASE_REV (${MB:0:12})"

mkdir -- "$TMP/base" "$TMP/head"
git -C "$REPO" archive "$MB" | tar -x -C "$TMP/base"
if [[ -n "$HEAD_REV" ]]; then
  git -C "$REPO" archive "$HEAD_SHA" | tar -x -C "$TMP/head"
else
  # Files deleted in the working tree are still listed by -c; skip them.
  git -C "$REPO" ls-files -z -c -o --exclude-standard |
    while IFS= read -r -d '' f; do
      if [[ -f "$REPO/$f" && ! -L "$REPO/$f" ]]; then printf '%s\0' "$f"; fi
    done |
    (cd "$REPO" && tar -c --no-recursion --null -T - -f -) |
    tar -x -C "$TMP/head"
fi

"$BIN" dump "$TMP/base" "$TMP/base.dump"
"$BIN" dump "$TMP/head" "$TMP/head.dump"

rc=0
"$BIN" delta -base-label "$BASE_LABEL" -head-label "$HEAD_LABEL" "$TMP/base.dump" "$TMP/head.dump" || rc=$?
case "$rc" in
0 | 1) exit "$rc" ;;
*) exit 2 ;;
esac
