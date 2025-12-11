#!/bin/bash
#
# ---------------------------------------------------------------------------
# Script Name: check_streamer_lag.sh
# Description: Monitors and reports the time lag between 'collected_at_str'
#              and 'commit_time_str' in Bluesky streamer data.
# Author     : Pasan Kamburugamuwa
# ---------------------------------------------------------------------------

# Set current UTC date
current_date=$(date -u +"%Y-%m-%d")
filename="${current_date}.json"
recipients=""
subject="Streamer Lag Alert"

# Check if today's file exists
if [ ! -f "$filename" ]; then
    message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - Error: File $filename not found"
    echo "$message" | mail -s "$subject" "$recipients"
    exit 1
fi


# Get the last complete line from the file
last_line=$(tail -n 1 "$filename")

# Use the extracting method.
timestamps=$(python3 -c "
import json, sys
try:
    data = json.loads(sys.argv[1])
    collected = data.get('collected_at_str', '')
    commit = data.get('commit_time_str', '')
    print(f'{collected}|{commit}')
except:
    print('ERROR')
" "$last_line")

# Check if Python extraction worked
if [ "$timestamps" = "ERROR" ] || [ -z "$timestamps" ]; then
    message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - Error: Failed to parse JSON line: $last_line"
    echo "$message" | mail -s "$subject" "$recipients"
    exit 1
fi

# et the timestamp
collected_at=$(echo "$timestamps" | cut -d'|' -f1)
commit_time=$(echo "$timestamps" | cut -d'|' -f2)

# Convert timestamps to epoch seconds
collected_epoch=$(date -u -d "$collected_at" +%s 2>/dev/null)
commit_epoch=$(date -u -d "$commit_time" +%s 2>/dev/null)


if [ -z "$collected_epoch" ] || [ -z "$commit_epoch" ]; then
    message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - Error: Failed to convert timestamps: '$collected_at' '$commit_time'"
    echo "$message" | mail -s "$subject" "$recipients"
    exit 1
fi

# Calculating the time difference
time_diff=$((collected_epoch - commit_epoch))
hours_diff=$((time_diff / 3600))

# Email message body
message="Reading Last line of $(date -u +"%Y-%m-%d %H:%M:%S UTC") - Streamer Status
Collected at: $collected_at
Commit time: $commit_time
Time difference: $hours_diff hours ($time_diff seconds)"

# Send email if lag exceeds 1 hour
if [ $hours_diff -gt 1 ]; then
    echo "$message" | mail -s "ALERT: $subject - $hours_diff hours lag" "$recipients"
    echo "Alert sent: $hours_diff hours lag"
else
    echo "OK: $hours_diff hours lag"
fi
