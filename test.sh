#!/bin/bash

# Configuration parameters
NUM_PROCESSES=1                   # Number of parallel processes
TEST_NAME="TestMapOf"             # Name of the test to run
ERROR_LOG="error.log"             # Error log file
SCRIPT_NAME=$(basename "$0")      # Get the script name

# Kill previous processes of the same script
kill_previous_processes() {
    echo "Killing previous $SCRIPT_NAME processes..."
    pids=$(pgrep -f "$SCRIPT_NAME" | grep -v $$)
    [ -n "$pids" ] && kill -9 $pids
    pkill -f pb.test
}

# Build the target file
build_target() {
    go test mapof_test.go mapof.go mapof_cachelinesize_64.go mapof_enablepadding_off.go mapof_atomic_loads_off.go mapof_atomic_stores_off.go -c -o pb.test
    echo "Build completed: pb.test"
}

# Run a single test process
run_test() {
    local process_num=$1
    while true; do
        echo "[Process $process_num] Running test..."
        
        # Output in real-time to the console while capturing the output
        exec 3>&1  # Create a new file descriptor 3 pointing to stdout
        output=$(./pb.test -test.run "$TEST_NAME" 2>&1 | tee /dev/fd/3)
        status=$?
        exec 3>&-  # Close file descriptor 3
        
        if [ $status -ne 0 ]; then
            echo -e "\n[Process $process_num] \033[31mERROR!\033[0m Exit code: $status" >&2
            echo "$output" > "$ERROR_LOG"
            exit 1
        fi
        echo "[Process $process_num] Test passed. Looping..."
    done
}

# Main function
main() {
    kill_previous_processes
    build_target
    
    echo "Starting $NUM_PROCESSES parallel test processes..."
    pids=()
    for i in $(seq 1 $NUM_PROCESSES); do
        run_test $i &
        pids+=($!)
    done
    
    # Capture Ctrl+C signal
    trap 'kill ${pids[@]} 2>/dev/null; exit' INT TERM
    
    wait ${pids[0]}  # Only need to wait for the first process, as errors will propagate
}

main

