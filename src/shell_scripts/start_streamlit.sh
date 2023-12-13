#!/bin/bash

LOG_DIR=$HOME/logs
WEB_LOG=$LOG_DIR/streamlit.log

# Check if log directory exists, create if not
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Function to log a message
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$SCRIPT_LOG"
}

log_message "Starting streamlit..."
nohup streamlit run $HOME/develop/mycrypto/src/env/streamlit/home.py > "$WEB_LOG" 2>&1 &
log_message "Airflow webserver started."

log_message "Streamlit services are being started."

