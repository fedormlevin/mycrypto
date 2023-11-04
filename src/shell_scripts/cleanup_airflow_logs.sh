#!/bin/bash

# The current date in YYYY-MM-DD format
current_date="2023-11-04"

# The directory where you want to search for folders (replace with your actual path)
search_dir="/Users/fedorlevin/workspace/airflow/logs"

# Find directories with date names and traverse recursively
# Find directories with date names and traverse recursively
find "$search_dir" -type d | while read -r dir; do
    # Extract the date part from the directory name
    dirdate=$(echo "$dir" | grep -o '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}')

    # Check if the directory name contains a date
    if [[ -n "$dirdate" ]]; then
        
        # Compare the extracted date to the current date
        if [[ "$dirdate" < "$current_date" ]]; then
            echo "Directory date ($dirdate) is before the current date ($current_date)."
            echo "Would remove directory: $dir"
            rm -rf "$dir"  # Uncomment this line to actually remove directories

        fi

    fi
done
