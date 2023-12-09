#!/bin/bash

LOG_DIR=~/airflow/logs
SCRIPT_LOG=$LOG_DIR/check_airflow_processes.log
NOT_FOUND_MESSAGE=""

# Check if log directory exists, create if not
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Function to log a message
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$SCRIPT_LOG"
}

# Function to check a process and log the result
check_process() {
    process_desc=$1
    log_message "Checking for process: $process_desc"
    if pgrep -f "$process_desc" > /dev/null; then
        log_message "Process found: $process_desc"
    else
        log_message "Process NOT found: $process_desc"
        NOT_FOUND_MESSAGE="$NOT_FOUND_MESSAGE $process_desc;"
    fi
}

# Check for Airflow executor - LocalExecutor
check_process "airflow executor -- LocalExecutor"

# Check for gunicorn master for Airflow webserver
check_process "gunicorn: master [airflow-webserver]"

# Final check
if [ -n "$NOT_FOUND_MESSAGE" ]; then
    log_message "The following process(es) were not found:$NOT_FOUND_MESSAGE Check failed."
    echo "Failed - The following process(es) were not found:$NOT_FOUND_MESSAGE"
    echo Starting airflow
    bash ~/develop/mycrypto/src/shell_scripts/start_airflow.sh

else
    log_message "All processes were found. Check successful."
fi

log_message "Process check completed."

