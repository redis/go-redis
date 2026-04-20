#!/usr/bin/env bash
# Driver script for the bench matrix. Invoked from bench-results/.
# Each configuration runs once and writes a log file alongside this script.
# Uses persistent docker volumes for Go caches so compilation happens only once.

set -u
cd "$(dirname "$0")/.."

REDIS_NAME="${REDIS_NAME:-fastreadwrite-redis}"
REDIS_PORT="${REDIS_PORT:-16379}"
NUM_KEYS="${NUM_KEYS:-200}"
CHUNK_MB="${CHUNK_MB:-4.0}"
NUM_WORKERS="${NUM_WORKERS:-8}"
RUNS="${RUNS:-5}"
MODE="${MODE:-both}"

run_in_docker() {
  local label="$1"; shift
  local out="bench-results/${label}.log"
  echo "=== $label ==="
  {
    echo "# label: $label"
    echo "# args: $*"
    echo "# date: $(date -u +%FT%TZ)"
    echo
  } > "$out"
  docker run --rm \
    --network "container:${REDIS_NAME}" \
    -v "$(pwd)/../..:/src" \
    -w /src/example/fastreadwrite \
    -v fastreadwrite-gocache:/root/.cache/go-build \
    -v fastreadwrite-gomodcache:/go/pkg/mod \
    -e GOFLAGS=-mod=mod \
    golang:1.24 \
    go run . --host 127.0.0.1 --port 6379 \
      --chunk-mb "$CHUNK_MB" --num-keys "$NUM_KEYS" \
      --num-workers "$NUM_WORKERS" --mode "$MODE" \
      --runs "$RUNS" \
      "$@" 2>&1 | tee -a "$out"
  echo
}

# 1) Baseline: every optimization off (matches default go-redis minus our changes)
run_in_docker baseline \
  --read-buf-kb 32 --write-buf-kb 32 \
  --sock-rcvbuf-kb 0 --sock-sndbuf-kb 0 \
  --huge-pages=false --min-idle 0

# 2) Just bufio tuning (1 MiB r/w)
run_in_docker bufio-only \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 0 --sock-sndbuf-kb 0 \
  --huge-pages=false --min-idle 0

# 3) Bufio + SO_*BUF
run_in_docker bufio-sockbuf \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 4096 --sock-sndbuf-kb 4096 \
  --huge-pages=false --min-idle 0

# 4) Full tuning (our defaults: QUICKACK implicit in tune_linux, MinIdleConns auto, hugepages on)
run_in_docker full-tuned \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 4096 --sock-sndbuf-kb 4096 \
  --huge-pages=true

# 5) Full + GC off during timed region
run_in_docker full-tuned-nogc \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 4096 --sock-sndbuf-kb 4096 \
  --huge-pages=true --no-gc

# 6) Full + no deadlines (skip SetReadDeadline/SetWriteDeadline syscalls)
run_in_docker full-tuned-nogc-nodead \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 4096 --sock-sndbuf-kb 4096 \
  --huge-pages=true --no-gc --no-deadlines

# 7) Regular (non-zero-copy) comparison on same tuned path
run_in_docker regular-tuned \
  --read-buf-kb 1024 --write-buf-kb 1024 \
  --sock-rcvbuf-kb 4096 --sock-sndbuf-kb 4096 \
  --huge-pages=true --regular --mode pipeline

echo "All runs complete."
