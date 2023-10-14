#!/bin/bash

# Set the default log directory
LOG_DIR="/Users/fedorlevin/workspace/LOG"

# Get the current date and time with seconds precision
CURRENT_DATETIME=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/run_scripts_$CURRENT_DATETIME.log"

echo "Running gen_config.sh..." >> $LOG_FILE
bash gen_config.sh

echo "Script finished at $(date +"%Y%m%d_%H%M%S")." >> $LOG_FILE