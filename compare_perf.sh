#!/bin/bash

# Performance Comparison Script for OTel Observability Overhead
# This script automates the 3-way comparison:
# 1. Current branch with OTel Enabled
# 2. Current branch with OTel Disabled
# 3. Upstream master (baseline without OTel code)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration (can be overridden via environment variables)
# Optimized defaults: 3 iterations × 3s = statistically valid in ~5-8 minutes total
BENCHMARK_COUNT=${BENCHMARK_COUNT:-3}        # Number of times to run each benchmark (3 is minimum for benchstat)
BENCHMARK_TIME=${BENCHMARK_TIME:-3s}         # How long to run each benchmark (3s gives stable results)
BENCHMARK_FILTER=${BENCHMARK_FILTER:-"BenchmarkOTelOverhead"}  # Benchmark name filter
UPSTREAM_REMOTE=${UPSTREAM_REMOTE:-"upstream"}
UPSTREAM_BRANCH=${UPSTREAM_BRANCH:-"master"}

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Redis Go Client - OTel Observability Performance Comparison  ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo -e "${BLUE}⏱️  Estimated time: ~5-8 minutes (count=${BENCHMARK_COUNT}, time=${BENCHMARK_TIME})${NC}"
echo -e "${YELLOW}💡 Customize: BENCHMARK_COUNT=5 BENCHMARK_TIME=5s ./compare_perf.sh${NC}"
echo ""

# Check if benchstat is installed
if ! command -v benchstat &> /dev/null; then
    echo -e "${YELLOW}⚠️  benchstat not found. Installing...${NC}"
    go install golang.org/x/perf/cmd/benchstat@latest
    if ! command -v benchstat &> /dev/null; then
        echo -e "${RED}❌ Failed to install benchstat. Please install manually:${NC}"
        echo -e "${RED}   go install golang.org/x/perf/cmd/benchstat@latest${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ benchstat installed${NC}"
fi

# Check if Redis is running
echo -e "${BLUE}🔍 Checking Redis connection...${NC}"

# Try redis-cli first (works on both Linux and macOS)
if command -v redis-cli &> /dev/null; then
    if redis-cli -h localhost -p 6379 ping &> /dev/null; then
        echo -e "${GREEN}✅ Redis is running${NC}"
    else
        echo -e "${RED}❌ Redis is not running on localhost:6379${NC}"
        echo -e "${YELLOW}💡 Start Redis with: docker run -d -p 6379:6379 redis:latest${NC}"
        exit 1
    fi
# Fallback to nc (netcat) - works on both Linux and macOS
elif command -v nc &> /dev/null; then
    if nc -z localhost 6379 2>/dev/null; then
        echo -e "${GREEN}✅ Redis is running (detected via port check)${NC}"
    else
        echo -e "${RED}❌ Redis is not running on localhost:6379${NC}"
        echo -e "${YELLOW}💡 Start Redis with: docker run -d -p 6379:6379 redis:latest${NC}"
        exit 1
    fi
# Fallback to /dev/tcp (works on Linux with bash, not macOS)
elif timeout 2 bash -c "echo > /dev/tcp/localhost/6379" 2>/dev/null; then
    echo -e "${GREEN}✅ Redis is running (detected via /dev/tcp)${NC}"
else
    echo -e "${RED}❌ Redis is not running on localhost:6379${NC}"
    echo -e "${YELLOW}💡 Start Redis with: docker run -d -p 6379:6379 redis:latest${NC}"
    exit 1
fi
echo ""

# Save current state
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo -e "${BLUE}📍 Current branch: ${GREEN}${CURRENT_BRANCH}${NC}"

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}⚠️  You have uncommitted changes. Stashing...${NC}"
    git stash push -m "benchmark_comparison_$(date +%s)"
    STASHED=true
else
    STASHED=false
fi

# Create results directory
RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo -e "${BLUE}📁 Results directory: ${GREEN}${RESULTS_DIR}${NC}"
echo ""

# Function to run benchmarks
run_benchmark() {
    local output_file=$1
    local description=$2
    local test_dir=$3

    echo -e "${BLUE}🏃 Running benchmarks: ${YELLOW}${description}${NC}"
    echo -e "${BLUE}   Directory: ${test_dir}${NC}"
    echo -e "${BLUE}   Benchmark count: ${BENCHMARK_COUNT}${NC}"
    echo -e "${BLUE}   Benchmark time: ${BENCHMARK_TIME}${NC}"
    echo -e "${BLUE}   Filter: ${BENCHMARK_FILTER}${NC}"

    # Run benchmark multiple times for statistical significance
    (cd "$test_dir" && go test -bench="${BENCHMARK_FILTER}" \
        -benchmem \
        -benchtime="${BENCHMARK_TIME}" \
        -count="${BENCHMARK_COUNT}" \
        -timeout=30m \
        -run=^$ \
        .) > "$output_file" 2>&1

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Benchmarks completed${NC}"
    else
        echo -e "${RED}❌ Benchmarks failed. Check ${output_file} for details${NC}"
        return 1
    fi
}

# Cleanup function
cleanup() {
    echo ""
    echo -e "${BLUE}🧹 Cleaning up...${NC}"
    
    # Return to original branch
    if [ "$(git rev-parse --abbrev-ref HEAD)" != "$CURRENT_BRANCH" ]; then
        echo -e "${BLUE}   Returning to branch: ${CURRENT_BRANCH}${NC}"
        git checkout "$CURRENT_BRANCH" 2>/dev/null || true
    fi
    
    # Restore stashed changes
    if [ "$STASHED" = true ]; then
        echo -e "${BLUE}   Restoring stashed changes...${NC}"
        git stash pop 2>/dev/null || true
    fi
    
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# ============================================================================
# STEP 1: Run benchmarks on current branch (OTel Enabled + Disabled)
# ============================================================================
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  STEP 1: Benchmarking Current Branch (with OTel code)         ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

run_benchmark "$RESULTS_DIR/current_branch.txt" "Current Branch (OTel Enabled + Disabled)" "extra/redisotel-native"

# ============================================================================
# STEP 2: Run benchmarks on upstream/master (baseline)
# ============================================================================
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  STEP 2: Benchmarking Upstream Master (baseline)              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Fetch upstream
echo -e "${BLUE}📥 Fetching ${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}...${NC}"
git fetch "$UPSTREAM_REMOTE" "$UPSTREAM_BRANCH"

# Checkout upstream/master
echo -e "${BLUE}🔀 Checking out ${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}...${NC}"
git checkout "${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}"

# Check if benchmark file exists on master
if [ ! -f "benchmark_overhead_test.go" ]; then
    echo -e "${YELLOW}⚠️  benchmark_overhead_test.go not found on ${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}${NC}"
    echo -e "${YELLOW}   Running baseline benchmarks using BenchmarkOTelOverhead_Baseline${NC}"

    # Run baseline benchmarks
    go test -bench="BenchmarkOTelOverhead_Baseline" \
        -benchmem \
        -benchtime="${BENCHMARK_TIME}" \
        -count="${BENCHMARK_COUNT}" \
        -timeout=30m \
        -run=^$ \
        . > "$RESULTS_DIR/upstream_master.txt" 2>&1
else
    # Run the same benchmark on master
    run_benchmark "$RESULTS_DIR/upstream_master.txt" "Upstream Master (No OTel code)" "."
fi

# Return to original branch
echo -e "${BLUE}🔀 Returning to ${CURRENT_BRANCH}...${NC}"
git checkout "$CURRENT_BRANCH"

# ============================================================================
# STEP 3: Analyze results with benchstat
# ============================================================================
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  STEP 3: Analyzing Results                                    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Extract OTel_Enabled and OTel_Disabled results from current branch
echo -e "${BLUE}📊 Extracting results...${NC}"
grep "OTel_Enabled" "$RESULTS_DIR/current_branch.txt" > "$RESULTS_DIR/otel_enabled.txt" || true
grep "OTel_Disabled" "$RESULTS_DIR/current_branch.txt" > "$RESULTS_DIR/otel_disabled.txt" || true

# Generate comparison reports
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  COMPARISON 1: Upstream Master vs OTel Disabled               ║${NC}"
echo -e "${GREEN}║  (Measures overhead of dormant OTel code)                     ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ -s "$RESULTS_DIR/upstream_master.txt" ] && [ -s "$RESULTS_DIR/otel_disabled.txt" ]; then
    benchstat "$RESULTS_DIR/upstream_master.txt" "$RESULTS_DIR/otel_disabled.txt" | tee "$RESULTS_DIR/comparison_master_vs_disabled.txt"
else
    echo -e "${YELLOW}⚠️  Insufficient data for comparison${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  COMPARISON 2: OTel Disabled vs OTel Enabled                  ║${NC}"
echo -e "${GREEN}║  (Measures overhead when metrics are enabled)                 ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ -s "$RESULTS_DIR/otel_disabled.txt" ] && [ -s "$RESULTS_DIR/otel_enabled.txt" ]; then
    benchstat "$RESULTS_DIR/otel_disabled.txt" "$RESULTS_DIR/otel_enabled.txt" | tee "$RESULTS_DIR/comparison_disabled_vs_enabled.txt"
else
    echo -e "${YELLOW}⚠️  Insufficient data for comparison${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  COMPARISON 3: Upstream Master vs OTel Enabled                ║${NC}"
echo -e "${GREEN}║  (Measures total overhead with metrics enabled)               ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ -s "$RESULTS_DIR/upstream_master.txt" ] && [ -s "$RESULTS_DIR/otel_enabled.txt" ]; then
    benchstat "$RESULTS_DIR/upstream_master.txt" "$RESULTS_DIR/otel_enabled.txt" | tee "$RESULTS_DIR/comparison_master_vs_enabled.txt"
else
    echo -e "${YELLOW}⚠️  Insufficient data for comparison${NC}"
fi

# ============================================================================
# STEP 4: Generate summary
# ============================================================================
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  SUMMARY                                                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

cat > "$RESULTS_DIR/README.md" << EOF
# OTel Observability Performance Comparison

**Date:** $(date)
**Branch:** ${CURRENT_BRANCH}
**Benchmark Count:** ${BENCHMARK_COUNT}
**Benchmark Time:** ${BENCHMARK_TIME}

## Files

- \`current_branch.txt\` - Raw benchmark results from current branch (both enabled and disabled)
- \`upstream_master.txt\` - Raw benchmark results from upstream/master
- \`otel_enabled.txt\` - Extracted OTel enabled results
- \`otel_disabled.txt\` - Extracted OTel disabled results
- \`comparison_master_vs_disabled.txt\` - Comparison showing dormant code overhead
- \`comparison_disabled_vs_enabled.txt\` - Comparison showing metrics collection overhead
- \`comparison_master_vs_enabled.txt\` - Comparison showing total overhead

## How to Read Results

### Comparison 1: Master vs Disabled
This shows the overhead of having the OTel code present but disabled.
**Goal:** Should be ~0% overhead (proves no-op pattern works)

### Comparison 2: Disabled vs Enabled
This shows the overhead when metrics are actively collected.
**Goal:** Should be acceptable for production use (<5-10% for most operations)

### Comparison 3: Master vs Enabled
This shows the total overhead with metrics enabled.
**Goal:** Combined overhead should still be acceptable

## Interpreting benchstat Output

- **~** means no significant difference (good for dormant code!)
- **+X%** means slower (overhead)
- **-X%** means faster (unlikely but possible due to variance)
- **p-value < 0.05** means the difference is statistically significant

## Running Benchmarks Manually

\`\`\`bash
# Run all benchmarks
go test -bench=BenchmarkOTelOverhead -benchmem -benchtime=10s -count=5

# Run specific benchmark
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping -benchmem -benchtime=10s -count=5

# Compare two runs
benchstat old.txt new.txt
\`\`\`
EOF

echo -e "${GREEN}✅ Results saved to: ${BLUE}${RESULTS_DIR}${NC}"
echo -e "${GREEN}✅ Summary saved to: ${BLUE}${RESULTS_DIR}/README.md${NC}"
echo ""
echo -e "${BLUE}📖 To view detailed results:${NC}"
echo -e "${BLUE}   cat ${RESULTS_DIR}/comparison_*.txt${NC}"
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅ Performance comparison complete!                           ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"

