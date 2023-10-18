#!/bin/bash

# doesn't work on Mac

# Directory containing the logs
LOG_DIR="~/workspace/LOG"

# Calculate the date from 5 days ago
DATE_5_DAYS_AGO=$(date -d "5 days ago" +%Y%m%d)

# Iterate over all .log files in the directory
for file in "$LOG_DIR"/*.log; do
    # Extract the date from the file name
    FILE_DATE=$(basename "$file" .log)
    
    # Check if the file date is older than 5 days ago
    if [[ "$FILE_DATE" -lt "$DATE_5_DAYS_AGO" ]]; then
        echo "Removing $file"
        rm -f "$file"
    fi
done
