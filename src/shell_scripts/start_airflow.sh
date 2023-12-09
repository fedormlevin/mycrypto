#!/bin/bash

LOG_DIR=~/airflow/logs
WEB_LOG=$LOG_DIR/webserver.log
SCHED_LOG=$LOG_DIR/scheduler.log
SCRIPT_LOG=$LOG_DIR/startup_script.log

# Check if log directory exists, create if not
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Function to log a message
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$SCRIPT_LOG"
}

log_message "Starting Airflow webserver..."
nohup airflow webserver > "$WEB_LOG" 2>&1 &
log_message "Airflow webserver started."

log_message "Starting Airflow scheduler..."
nohup airflow scheduler > "$SCHED_LOG" 2>&1 &
log_message "Airflow scheduler started."

log_message "Airflow services are being started. Check individual logs for details."

