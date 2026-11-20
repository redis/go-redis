#!/usr/bin/env bash
# Compare the exported API of the root module with a released version.
#
#   scripts/apidiff.sh [BASE]
#
# BASE defaults to the latest stable v9 tag reachable from HEAD. Prints the full apidiff report
# and exits 1 if there is an incompatible change that is not matched by
# scripts/apidiff-allow.txt. Run from the repository root.
set -euo pipefail

APIDIFF_VERSION="v0.0.0-20261109185158-a52e3aef5920" # golang.org/x/exp
MODULE="github.com/redis/go-redis/v9"
ALLOW="scripts/apidiff-allow.txt"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Latest stable v9 tag reachable from HEAD, so release branches compare
# against their own line.
BASE="${1:-$(git describe --tags --abbrev=0 --match 'v9.*' --exclude '*-*' HEAD 2>/dev/null || true)}"
if [[ -z "$BASE" ]] || ! git rev-parse --verify --quiet "$BASE^{commit}" >/dev/null; then
  echo "apidiff: base '$BASE' not found (fetch tags: git fetch --tags)" >&2
  exit 2
fi

TMP="$(mktemp -d)"
cleanup() {
  if [[ -n "$TMP" && -d "$TMP" && "$TMP" != "/" ]]; then rm -rf -- "$TMP"; fi
}
trap cleanup EXIT

APIDIFF=(go run "golang.org/x/exp/cmd/apidiff@$APIDIFF_VERSION")

mkdir -- "$TMP/base"
git archive "$BASE" | tar -x -C "$TMP/base"
(cd "$TMP/base" && "${APIDIFF[@]}" -m -w "$TMP/base.exp" "$MODULE" >/dev/null)
"${APIDIFF[@]}" -m -w "$TMP/head.exp" "$MODULE" >/dev/null

echo "apidiff: $MODULE $BASE..HEAD"
"${APIDIFF[@]}" -m "$TMP/base.exp" "$TMP/head.exp" 2>/dev/null | grep -v '^Ignoring internal package' || true

"${APIDIFF[@]}" -m -incompatible "$TMP/base.exp" "$TMP/head.exp" 2>/dev/null |
  grep '^- ' >"$TMP/incompatible.txt" || true
grep -Ev '^[[:space:]]*(#|$)' "$ALLOW" >"$TMP/allow.txt" || true

if [[ -s "$TMP/allow.txt" ]]; then
  grep -Evx -f "$TMP/allow.txt" "$TMP/incompatible.txt" >"$TMP/blocked.txt" || true
else
  cp -- "$TMP/incompatible.txt" "$TMP/blocked.txt"
fi

if [[ -s "$TMP/blocked.txt" ]]; then
  echo
  echo "apidiff: incompatible API changes since $BASE:"
  cat -- "$TMP/blocked.txt"
  echo
  echo "If intended, add a matching line to $ALLOW and get maintainer review."
  exit 1
fi
echo "apidiff: no unexpected incompatible changes since $BASE"
