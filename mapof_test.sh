#!/bin/bash

# =============================================
# CONFIGURATION SECTION
# =============================================

# Number of parallel test processes to run
NUM_PROCESSES=1

# Name of the Go test function to execute
TEST_NAME="TestMapOf"

# Directory to store log files
LOG_DIR="$(dirname "$0")/logs"

# File to record test failures
ERROR_LOG="$LOG_DIR/error.log"

# File to record all test output
OUTPUT_LOG="$LOG_DIR/output.log"

# Current script name (for process management)
SCRIPT_NAME=$(basename "$0")

# Flag file to signal all processes to stop
STOP_FLAG="$LOG_DIR/stop.flag"

# =============================================
# FUNCTION DEFINITIONS
# =============================================

# Initialize the test environment
initialize() {
    # Create log directory if it doesn't exist
    mkdir -p "$LOG_DIR"
    
    # Remove any existing stop flag
    rm -f "$STOP_FLAG"
}

# Kill all related test processes
kill_related_processes() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stopping all test processes..."
    
    # Kill all running test binaries
    pkill -f "pb.test" 2>/dev/null
    
    # Kill all instances of this script (except current one)
    pkill -f "$SCRIPT_NAME" 2>/dev/null
    
    # Clean up stop flag
    rm -f "$STOP_FLAG"
}

# Build the test binary
build_target() {
    echo "Building test binary..."
    
    # Compile Go test files into executable
    if ! go test mapof_test.go mapof.go mapof_deprecated.go mapof_experimental.go mapof_opt_cachelinesize.go mapof_opt_*_off.go atomic_utils.go -c -o pb.test; then
        echo "Build failed!" | tee -a "$ERROR_LOG"
        exit 1
    fi
}

# Run a single test process
run_test() {
    local process_num=$1
    
    # Continue running tests until stop flag is detected
    while [ ! -f "$STOP_FLAG" ]; do
        echo "[Process $process_num] Running test..." | tee -a "$OUTPUT_LOG"
        
        # Execute test and capture output
        if ! ./pb.test -test.run "$TEST_NAME" >> "$OUTPUT_LOG" 2>&1; then
            # On test failure:
            echo "[Process $process_num] Test failed!" | tee -a "$ERROR_LOG"
            
            # Create stop flag to terminate all processes
            touch "$STOP_FLAG"
            exit 1
        fi
        
        echo "[Process $process_num] Test passed." | tee -a "$OUTPUT_LOG"
    done
}

# Start the test swarm
start_tests() {
    # Clean up any existing processes
    kill_related_processes
    
    # Build the test binary
    build_target
    
    # Log test start information
    echo "===== TEST STARTED =====" | tee -a "$OUTPUT_LOG"
    echo "Start time: $(date)" | tee -a "$OUTPUT_LOG"
    
    # Launch test processes in parallel
    for i in $(seq 1 $NUM_PROCESSES); do
        run_test $i &
    done
    
    # Set up signal handlers for clean shutdown
    trap 'kill_related_processes; exit' INT TERM
    
    # Wait for all child processes to complete
    wait
    
    # Log test completion
    echo "===== TEST FINISHED =====" | tee -a "$OUTPUT_LOG"
}

# =============================================
# MAIN SCRIPT EXECUTION
# =============================================

# Initialize test environment
initialize

# Process command line arguments
case "$1" in
    start)
        # Start the test suite
        start_tests
        ;;
    stop)
        # Stop all running tests
        kill_related_processes
        ;;
    *)
        # Display usage information
        echo "Usage: $0 {start|stop}"
        echo "  start - Launch stress testing"
        echo "  stop  - Terminate all test processes"
        exit 1
        ;;
esac
