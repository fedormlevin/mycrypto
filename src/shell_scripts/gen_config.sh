#!/bin/bash

input_file="/Users/fedorlevin/workspace/mycrypto/refdata_config.csv"
output_file="$HOME/.my_config"

# Empty or create the output file
> "$output_file"

# Using awk to process the CSV
awk -F, 'NR > 1 { gsub(/\r/, "", $2); print "export " $1 "=" $2 }' "$input_file" >> "$output_file"

echo ".config file has been generated!"