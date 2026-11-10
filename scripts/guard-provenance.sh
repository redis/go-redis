#!/usr/bin/env bash
# Check that guard goldens in a PR were written by the guard bot.
#
#   scripts/guard-provenance.sh <pr-checkout> <base-rev> <head-rev>
#
# Env:
#   GUARD_BOT_LOGIN  GitHub login of the guard bot (required)
#   GITHUB_REPOSITORY, GH_TOKEN  used to read commit metadata via `gh api`
#   GUARD_PROVENANCE_META_FILE   test only: lines "<sha> <committer-login>
#                                <verified> <reason>" instead of the API
#
# Rule: if the goldens differ between merge-base(base, head) and head, the
# newest non-merge commit that touches them must
#   - have committer login GUARD_BOT_LOGIN, a verified signature with
#     reason "valid",
#   - touch only the golden directory,
# and the goldens must not change between that commit and head.
set -euo pipefail

GOLDENS="internal/guard/testdata"

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <pr-checkout> <base-rev> <head-rev>" >&2
  exit 2
fi
DIR="$1"
BASE_REV="$2"
HEAD_REV="$3"
: "${GUARD_BOT_LOGIN:?GUARD_BOT_LOGIN must be set}"

git_() { git -C "$DIR" "$@"; }

BASE="$(git_ merge-base "$BASE_REV" "$HEAD_REV")"
HEAD="$(git_ rev-parse "$HEAD_REV^{commit}")"

if git_ diff --quiet "$BASE" "$HEAD" -- "$GOLDENS"; then
  echo "guard-provenance: goldens unchanged"
  exit 0
fi

LAST="$(git_ log --no-merges --format=%H -n1 "$BASE..$HEAD" -- "$GOLDENS")"
if [[ -z "$LAST" ]]; then
  echo "guard-provenance: goldens changed only through merge commits; run guard-update" >&2
  exit 1
fi

meta() { # sha -> "login verified reason"
  if [[ -n "${GUARD_PROVENANCE_META_FILE:-}" ]]; then
    awk -v s="$1" '$1 == s { print $2, $3, $4; found = 1 } END { if (!found) print "- false missing" }' \
      "$GUARD_PROVENANCE_META_FILE"
    return
  fi
  : "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
  gh api "repos/$GITHUB_REPOSITORY/commits/$1" \
    --jq '[(.committer.login // "-"), .commit.verification.verified, .commit.verification.reason] | join(" ")'
}

fail() {
  echo "guard-provenance: $*" >&2
  echo "Golden files may only be changed by the guard bot. Ask a maintainer to add the" >&2
  echo "guard-update label; do not commit regenerated goldens yourself." >&2
  exit 1
}

read -r login verified reason <<<"$(meta "$LAST")"
short="$(git_ rev-parse --short "$LAST")"
[[ "$login" == "$GUARD_BOT_LOGIN" ]] || fail "$short changes goldens; committer is '$login', not '$GUARD_BOT_LOGIN'"
[[ "$verified" == "true" && "$reason" == "valid" ]] || fail "$short is not a verified signed commit (verified=$verified reason=$reason)"

other="$(git_ diff-tree --no-commit-id --name-only -r "$LAST" | grep -v "^$GOLDENS/" || true)"
[[ -z "$other" ]] || fail "bot commit $short also changes files outside $GOLDENS: $(echo "$other" | tr '\n' ' ')"

if ! git_ diff --quiet "$LAST" "$HEAD" -- "$GOLDENS"; then
  fail "goldens changed after bot commit $short (merge or later edit); run guard-update again"
fi

echo "guard-provenance: goldens from bot commit $short"
