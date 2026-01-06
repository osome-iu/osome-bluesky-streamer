#!/bin/bash

# Set variables
current_date=$(date -u +"%Y-%m-%d")
filename="${current_date}.json"

# Email variables
recipient=""  # Replace with your email address
subject="File Update Alert: ${filename}"

# Check if the file exists
if [ ! -f "$filename" ]; then
  # Find the most recently modified JSON file in the backup directory
  latest_json=$(find "$backup_root_folder" -type f -name "*.json" -printf "%T@ %p\n" 2>/dev/null | sort -nr | head -n1 | awk '{print $2}')
  message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - File $filename does not exist."
  if [ -n "$latest_json" ]; then
    message+="\nLatest updated JSON file: $latest_json"
  else
    message+="\nNo recent JSON files found"
  fi
  echo -e "$message" | mailx -s "$subject" "$recipient"
  pkill python  # Could be supervisorctl restart, or a command that force restarts firehose_streamer.py
  exit 1
fi

# Check if the file has been modified in the last 20 minutes
last_modified_time=$(stat -c %y "$filename" 2>/dev/null | cut -d'.' -f1)  # Get last modification time
if ! find "$filename" -mmin -20 | grep -q "$filename"; then
  message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - File $filename has not been modified in the last 20 minutes.
Last modified: $last_modified_time"
  
  echo "$message" | mailx -s "$subject" "$recipient"

  pkill python  # Could be supervisorctl restart, or cmd that forces restart of firehose_streamer.py
  exit 1
fi
