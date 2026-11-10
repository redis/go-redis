#!/usr/bin/env bash
# Run the guard for a PR in CI, without trusting the PR's guard code.
#
#   scripts/guard-run.sh check  <base-checkout> <pr-checkout> <base-rev>
#   scripts/guard-run.sh update <base-checkout> <pr-checkout> <base-rev> <out-dir>
#
# The guard code comes from <base-checkout> and scans <pr-checkout> as data
# (GUARD_ROOT). check compares against the PR's goldens; update writes fresh
# goldens to <out-dir>.
#
# If the PR changes the guard code itself (internal/guard/*.go):
#   check   runs nothing. The PR may change only internal/guard/, and the
#           goldens must be unchanged or last changed after the last
#           guard-code change (guard-provenance checks that commit is the
#           bot's). The guard code is reviewed via CODEOWNERS.
#   update  refuses, unless GUARD_TRUST_PR_CODE=1 (set only by a manual
#           workflow_dispatch after a maintainer reviewed the guard diff).
# Callers must run this without secrets. <base-checkout> is modified.
set -euo pipefail

MODE="${1:-}"
case "$MODE" in check | update) ;; *)
  echo "usage: $0 check|update <base-checkout> <pr-checkout> <base-rev> [out-dir]" >&2
  exit 2
  ;;
esac
BASE_DIR="$(cd "$2" && pwd)"
PR_DIR="$(cd "$3" && pwd)"
BASE_REV="$4"
OUT="${5:-}"
if [[ "$MODE" == update && -z "$OUT" ]]; then
  echo "update needs <out-dir>" >&2
  exit 2
fi

# Use the toolchain installed by the workflow; never one requested by a PR.
: "${GOTOOLCHAIN:=local}"
export GOTOOLCHAIN
GOLDENS="internal/guard/testdata"
MB="$(git -C "$PR_DIR" merge-base "$BASE_REV" HEAD)"

if git -C "$PR_DIR" diff --quiet "$MB" HEAD -- 'internal/guard/*.go'; then
  CODE="$BASE_DIR/internal/guard"
  export GUARD_ROOT="$PR_DIR"
  echo "guard-run: base guard code, PR sources from $PR_DIR"
  # Compare against the PR's goldens: replace the base copies.
  if [[ "$BASE_DIR" == "$PR_DIR" || ! -d "$CODE/testdata" ]]; then
    echo "guard-run: bad checkout layout" >&2
    exit 2
  fi
  find "$CODE/testdata" -maxdepth 1 -type f -name 'guard_*.json' -delete
  if [[ -d "$PR_DIR/$GOLDENS" ]]; then
    find "$PR_DIR/$GOLDENS" -maxdepth 1 -type f -name 'guard_*.json' -exec cp -- {} "$CODE/testdata/" \;
  fi
elif [[ "$MODE" == check ]]; then
  echo "guard-run: PR changes the guard code; not executing it (CODEOWNERS review required)"
  outside="$(git -C "$PR_DIR" diff --name-only "$MB" HEAD | grep -v '^internal/guard/' || true)"
  if [[ -n "$outside" ]]; then
    echo "guard-run: a PR that changes internal/guard/*.go may change only internal/guard/." >&2
    echo "Move these to a separate PR: $(echo "$outside" | tr '\n' ' ')" >&2
    exit 1
  fi
  last_code="$(git -C "$PR_DIR" log --no-merges --format=%H -n1 "$MB..HEAD" -- 'internal/guard/*.go')"
  if git -C "$PR_DIR" diff --quiet "$MB" HEAD -- "$GOLDENS"; then
    echo "guard-run: goldens unchanged"
    exit 0
  fi
  last_golden="$(git -C "$PR_DIR" log --no-merges --format=%H -n1 "$MB..HEAD" -- "$GOLDENS")"
  if [[ -z "$last_golden" ]] || ! git -C "$PR_DIR" merge-base --is-ancestor "$last_code" "$last_golden" ||
    [[ "$last_code" == "$last_golden" ]]; then
    echo "guard-run: goldens must be regenerated after the last guard-code change" >&2
    echo "(maintainer: run the Guard update workflow with trust_pr_guard_code)" >&2
    exit 1
  fi
  echo "guard-run: goldens regenerated after the last guard-code change"
  exit 0
else
  if [[ "${GUARD_TRUST_PR_CODE:-}" != 1 ]]; then
    echo "guard-run: PR changes the guard code. Review it, then run the Guard update" >&2
    echo "workflow manually with trust_pr_guard_code: true." >&2
    exit 1
  fi
  CODE="$PR_DIR/internal/guard"
  unset GUARD_ROOT
  echo "guard-run: using the PR's guard code (trusted by a maintainer)"
fi

cd "$CODE"
if [[ "$MODE" == check ]]; then
  go test -tags guard -count=1 .
  exit 0
fi

go test -tags guard -count=1 . -guard.update
mkdir -p -- "$OUT"
find "$CODE/testdata" -maxdepth 1 -type f -name 'guard_*.json' -exec cp -- {} "$OUT/" \;
echo "guard-run: goldens written to $OUT"
