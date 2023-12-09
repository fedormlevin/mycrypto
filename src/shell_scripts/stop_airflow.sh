#!/bin/bash

LOG_DIR=~/airflow/logs
SCRIPT_LOG=$LOG_DIR/kill_airflow_script.log

# Check if log directory exists, create if not
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Function to log a message
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$SCRIPT_LOG"
}

# Function to kill a process and log the action
kill_process() {
    process_desc=$1
    log_message "Attempting to kill process: $process_desc"
    pkill -f "$process_desc"
    sleep 3
    log_message "Process killed: $process_desc"
}

# Kill Airflow executor - LocalExecutor
kill_process "airflow executor -- LocalExecutor"

# Kill gunicorn master for Airflow webserver
kill_process "gunicorn: master [airflow-webserver]"

# Kill Airflow webserver
kill_process "airflow webserver"

log_message "All specified Airflow processes have been killed."

