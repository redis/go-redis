#!/bin/bash

# Maintenance Notifications E2E Tests Runner
# This script sets up the environment and runs the maintnotifications upgrade E2E tests

set -euo pipefail

# Script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
E2E_DIR="${REPO_ROOT}/maintnotifications/e2e"

# Configuration
FAULT_INJECTOR_URL="http://127.0.0.1:20324"
CONFIG_PATH="${REPO_ROOT}/maintnotifications/e2e/infra/cae-client-testing/config/endpoints.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Help function
show_help() {
    cat << EOF
Maintenance Notifications E2E Tests Runner

Usage: $0 [OPTIONS]

OPTIONS:
    -h, --help              Show this help message
    -v, --verbose           Enable verbose test output (human-readable)
    -d, --debug             Enable debug logging (shows detailed fault injector responses)
    -t, --timeout DURATION  Test timeout (default: 30m)
    -r, --run PATTERN       Run only tests matching pattern
    --json                  Enable JSON output (default)
    --no-json               Disable JSON output, use verbose human-readable format
    --cluster               Run only cluster tests (tests with 'Cluster' in name)
    --single                Run only non-cluster tests (exclude tests with 'Cluster' in name)
    --dry-run               Show what would be executed without running
    --list                  List available tests
    --config PATH           Override config path (default: infra/cae-client-testing/endpoints.json)
    --fault-injector URL    Override fault injector URL (default: http://127.0.0.1:20324)

EXAMPLES:
    $0                                    # Run all E2E tests with JSON output
    $0 --no-json                         # Run all tests with verbose human-readable output
    $0 --debug                           # Run with debug logging enabled
    $0 --cluster                         # Run only cluster tests (OSS Cluster API)
    $0 --single                          # Run only non-cluster tests
    $0 -r TestPushNotifications          # Run only tests matching pattern
    $0 -t 45m                            # Run with 45 minute timeout
    $0 --dry-run                         # Show what would be executed
    $0 --list                            # List available tests

ENVIRONMENT:
    The script automatically sets up the required environment variables:
    - REDIS_ENDPOINTS_CONFIG_PATH: Path to Redis endpoints configuration
    - FAULT_INJECTION_API_URL: URL of the fault injector server
    - E2E_SCENARIO_TESTS: Enables scenario tests
    - E2E_DEBUG: Enables debug logging (set by --debug flag)

EOF
}

# Parse command line arguments
TIMEOUT="30m"
RUN_PATTERN=""
DRY_RUN=false
LIST_TESTS=false
JSON_OUTPUT=true
CLUSTER_ONLY=false
SINGLE_ONLY=false
DEBUG_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            # Verbose is now the same as --no-json for backward compatibility
            JSON_OUTPUT=false
            shift
            ;;
        -d|--debug)
            DEBUG_MODE=true
            shift
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -r|--run)
            RUN_PATTERN="$2"
            shift 2
            ;;
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --no-json)
            JSON_OUTPUT=false
            shift
            ;;
        --cluster)
            CLUSTER_ONLY=true
            shift
            ;;
        --single)
            SINGLE_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --list)
            LIST_TESTS=true
            shift
            ;;
        --config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        --fault-injector)
            FAULT_INJECTOR_URL="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate mutually exclusive options
if [[ "$CLUSTER_ONLY" == true && "$SINGLE_ONLY" == true ]]; then
    log_error "Cannot use --cluster and --single together"
    exit 1
fi

# Set run pattern based on cluster/single options
if [[ "$CLUSTER_ONLY" == true ]]; then
    if [[ -n "$RUN_PATTERN" ]]; then
        log_warning "Overriding -r pattern with --cluster"
    fi
    RUN_PATTERN="Cluster"
elif [[ "$SINGLE_ONLY" == true ]]; then
    if [[ -n "$RUN_PATTERN" ]]; then
        log_warning "Overriding -r pattern with --single"
    fi
    # Match tests that don't have "Cluster" in the name
    # Using negative lookahead alternative: match Test followed by anything not containing Cluster
    RUN_PATTERN="^Test[^C]|^Test.*[^r]$"
fi

# Validate configuration file exists
if [[ ! -f "$CONFIG_PATH" ]]; then
    log_error "Configuration file not found: $CONFIG_PATH"
    log_info "Please ensure the endpoints.json file exists at the specified path"
    exit 1
fi

# Set up environment variables
export REDIS_ENDPOINTS_CONFIG_PATH="$CONFIG_PATH"
export FAULT_INJECTION_API_URL="$FAULT_INJECTOR_URL"
export E2E_SCENARIO_TESTS="true"
if [[ "$DEBUG_MODE" == true ]]; then
    export E2E_DEBUG="true"
fi

# Build test command
TEST_CMD="go test -tags=e2e"

# Add JSON or verbose output
if [[ "$JSON_OUTPUT" == true ]]; then
    TEST_CMD="$TEST_CMD -json"
else
    TEST_CMD="$TEST_CMD -v"
fi

if [[ -n "$TIMEOUT" ]]; then
    TEST_CMD="$TEST_CMD -timeout=$TIMEOUT"
fi

if [[ -n "$RUN_PATTERN" ]]; then
    TEST_CMD="$TEST_CMD -run '$RUN_PATTERN'"
fi

TEST_CMD="$TEST_CMD ./maintnotifications/e2e/"

# List tests if requested
if [[ "$LIST_TESTS" == true ]]; then
    log_info "Available E2E tests:"
    cd "$REPO_ROOT"
    go test -tags=e2e ./maintnotifications/e2e/ -list=. | grep -E "^Test" | sort
    exit 0
fi

# Show configuration
log_info "Maintenance notifications E2E Tests Configuration:"
echo "  Repository Root: $REPO_ROOT" >&2
echo "  E2E Directory: $E2E_DIR" >&2
echo "  Config Path: $CONFIG_PATH" >&2
echo "  Fault Injector URL: $FAULT_INJECTOR_URL" >&2
echo "  Test Timeout: $TIMEOUT" >&2
echo "  JSON Output: $JSON_OUTPUT" >&2
echo "  Debug Mode: $DEBUG_MODE" >&2
if [[ -n "$RUN_PATTERN" ]]; then
    echo "  Test Pattern: $RUN_PATTERN" >&2
fi
echo "" >&2

# Validate fault injector connectivity
log_info "Checking fault injector connectivity..."
if command -v curl >/dev/null 2>&1; then
    if curl -s --connect-timeout 5 "$FAULT_INJECTOR_URL/health" >/dev/null 2>&1; then
        log_success "Fault injector is accessible at $FAULT_INJECTOR_URL"
    else
        log_warning "Cannot connect to fault injector at $FAULT_INJECTOR_URL"
        log_warning "Tests may fail if fault injection is required"
    fi
else
    log_warning "curl not available, skipping fault injector connectivity check"
fi

# Show what would be executed in dry-run mode
if [[ "$DRY_RUN" == true ]]; then
    log_info "Dry run mode - would execute:"
    echo "  cd $REPO_ROOT" >&2
    echo "  export REDIS_ENDPOINTS_CONFIG_PATH=\"$CONFIG_PATH\"" >&2
    echo "  export FAULT_INJECTION_API_URL=\"$FAULT_INJECTOR_URL\"" >&2
    echo "  export E2E_SCENARIO_TESTS=\"true\"" >&2
    if [[ "$DEBUG_MODE" == true ]]; then
        echo "  export E2E_DEBUG=\"true\"" >&2
    fi
    echo "  $TEST_CMD" >&2
    exit 0
fi

# Change to repository root
cd "$REPO_ROOT"

# Run the tests
log_info "Starting E2E tests..."
log_info "Command: $TEST_CMD"
echo "" >&2

if eval "$TEST_CMD"; then
    echo "" >&2
    log_success "All E2E tests completed successfully!"
    exit 0
else
    echo "" >&2
    log_error "E2E tests failed!"
    log_info "Check the test output above for details"
    exit 1
fi
