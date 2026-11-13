#!/usr/bin/env bash
# Build the guard report comment for a PR (internal/guard/doc.go).
#
#   scripts/guard-pr-comment.sh <report-file> <out-file>
#
# <report-file> is the output of scripts/guard-delta.sh. Its lines contain
# text from the PR (string literals), so it is only ever placed inside a code
# fence that is longer than any backtick run in it, and the comment is capped
# below GitHub's 65536 character limit.
#
# Environment (optional, for the header): RUN_URL, BASE_REF.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <report-file> <out-file>" >&2
  exit 2
fi
REPORT="$1"
OUT="$2"

# Marker used to find and update this comment instead of adding a new one.
MARKER='<!-- guard-report -->'
LIMIT=60000

gated="$(sed -n 's/^needs approval: \([0-9][0-9]*\)$/\1/p' "$REPORT" | head -n 1)"
info="$(sed -n 's/^for information: \([0-9][0-9]*\)$/\1/p' "$REPORT" | head -n 1)"
digest="$(sed -n 's/^digest: \([0-9a-f]\{12\}\)$/\1/p' "$REPORT" | head -n 1)"
if [[ -z "$gated" || -z "$info" ]]; then
  echo "guard-pr-comment: report has no counts" >&2
  exit 2
fi

# Longest run of backticks in the report decides the fence length.
longest="$(awk '{ s = $0; while (match(s, /`+/)) { if (RLENGTH > m) m = RLENGTH; s = substr(s, RSTART + RLENGTH) } } END { print m + 0 }' "$REPORT")"
n=$((longest < 3 ? 3 : longest + 1))
fence="$(printf '%*s' "$n" '' | tr ' ' '`')"

{
  echo "$MARKER"
  echo "### Guard report"
  echo
  if [[ "$gated" -eq 0 ]]; then
    echo "**Nothing needs approval.** For information: $info."
  else
    echo "**Needs approval: $gated** (digest \`$digest\`). For information: $info."
  fi
  echo
  if [[ -n "${BASE_REF:-}" ]]; then
    echo "Compared with the merge base of \`${BASE_REF//\`/}\`."
  fi
  echo "Report only: this check does not block merging yet."
  if [[ -n "${RUN_URL:-}" ]]; then
    echo "Full output: [workflow run](${RUN_URL})."
  fi
  echo
  echo "${fence}text"
} >"$OUT"

used=$(($(wc -c <"$OUT")))
room=$((LIMIT - used - 200))
size=$(($(wc -c <"$REPORT")))
if ((size <= room)); then
  cat -- "$REPORT" >>"$OUT"
else
  # Cut at a line boundary.
  head -c "$room" -- "$REPORT" | sed '$d' >>"$OUT"
  echo "... report cut at $room of $size bytes; see the workflow run" >>"$OUT"
fi
printf '%s\n' "$fence" >>"$OUT"
