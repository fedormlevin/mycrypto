#!/bin/bash

# Assuming the input CSV file is named 'data.csv'
input_file="/Users/fedorlevin/workspace/mycrypto/refdata_config.csv"
output_file="$HOME/.my_config"

# Empty or create the output file
> "$output_file"

# Read the CSV and generate the config lines
while IFS=, read -r name value; do
    # Skip the header
    if [ "$name" != "Name" ]; then
        echo "export $name=$value" >> "$output_file"
    fi
done < "$input_file"

echo ".my_config file has been generated in your home directory!"

