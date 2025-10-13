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
CONFIG_PATH="${REPO_ROOT}/maintnotifications/e2e/infra/cae-client-testing/endpoints.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Maintenance Notifications E2E Tests Runner

Usage: $0 [OPTIONS]

OPTIONS:
    -h, --help              Show this help message
    -v, --verbose           Enable verbose test output
    -t, --timeout DURATION Test timeout (default: 30m)
    -r, --run PATTERN       Run only tests matching pattern
    --dry-run              Show what would be executed without running
    --list                 List available tests
    --config PATH          Override config path (default: infra/cae-client-testing/endpoints.json)
    --fault-injector URL   Override fault injector URL (default: http://127.0.0.1:20324)

EXAMPLES:
    $0                                    # Run all E2E tests
    $0 -v                                # Run with verbose output
    $0 -r TestPushNotifications         # Run only push notification tests
    $0 -t 45m                           # Run with 45 minute timeout
    $0 --dry-run                        # Show what would be executed
    $0 --list                           # List available tests

ENVIRONMENT:
    The script automatically sets up the required environment variables:
    - REDIS_ENDPOINTS_CONFIG_PATH: Path to Redis endpoints configuration
    - FAULT_INJECTION_API_URL: URL of the fault injector server
    - E2E_SCENARIO_TESTS: Enables scenario tests

EOF
}

# Parse command line arguments
VERBOSE=""
TIMEOUT="30m"
RUN_PATTERN=""
DRY_RUN=false
LIST_TESTS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE="-v"
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

# Build test command
TEST_CMD="go test -json -tags=e2e -v"

if [[ -n "$TIMEOUT" ]]; then
    TEST_CMD="$TEST_CMD -timeout=$TIMEOUT"
fi

if [[ -n "$VERBOSE" ]]; then
    TEST_CMD="$TEST_CMD $VERBOSE"
fi

if [[ -n "$RUN_PATTERN" ]]; then
    TEST_CMD="$TEST_CMD -run $RUN_PATTERN"
fi

TEST_CMD="$TEST_CMD ./maintnotifications/e2e/ "

# List tests if requested
if [[ "$LIST_TESTS" == true ]]; then
    log_info "Available E2E tests:"
    cd "$REPO_ROOT"
    go test -tags=e2e ./maintnotifications/e2e/ -list=. | grep -E "^Test" | sort
    exit 0
fi

# Show configuration
log_info "Maintenance notifications E2E Tests Configuration:"
echo "  Repository Root: $REPO_ROOT"
echo "  E2E Directory: $E2E_DIR"
echo "  Config Path: $CONFIG_PATH"
echo "  Fault Injector URL: $FAULT_INJECTOR_URL"
echo "  Test Timeout: $TIMEOUT"
if [[ -n "$RUN_PATTERN" ]]; then
    echo "  Test Pattern: $RUN_PATTERN"
fi
echo ""

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
    echo "  cd $REPO_ROOT"
    echo "  export REDIS_ENDPOINTS_CONFIG_PATH=\"$CONFIG_PATH\""
    echo "  export FAULT_INJECTION_API_URL=\"$FAULT_INJECTOR_URL\""
    echo "  export E2E_SCENARIO_TESTS=\"true\""
    echo "  $TEST_CMD"
    exit 0
fi

# Change to repository root
cd "$REPO_ROOT"

# Run the tests
log_info "Starting E2E tests..."
log_info "Command: $TEST_CMD"
echo ""

if eval "$TEST_CMD"; then
    echo ""
    log_success "All E2E tests completed successfully!"
    exit 0
else
    echo ""
    log_error "E2E tests failed!"
    log_info "Check the test output above for details"
    exit 1
fi
