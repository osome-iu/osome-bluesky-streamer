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
recipients="recipient_emails"
subject="Streamer Lag Alert"
tail_lines=10
alert_threshold_seconds=3600
skipped_alert_threshold=1
error_log=$(mktemp)
skipped_log=$(mktemp)

trap 'rm -f "$error_log" "$skipped_log"' EXIT

# Check if today's file exists
if [ ! -f "$filename" ]; then
    message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - Error: File $filename not found"
    echo "$message" | mail -s "$subject" "$recipients"
    exit 1
fi

# Inspect a recent window and alert only if even the smallest lag in that window is large.
export SKIPPED_LOG="$skipped_log"
window_stats=$(tail -n "$tail_lines" "$filename" | python3 -c '
import json
import os
import sys
from datetime import datetime, timezone

INPUT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SKIPPED_LINE_PREVIEW_LENGTH = 300

def parse_timestamp(value):
    try:
        return datetime.strptime(value, INPUT_DATE_FORMAT).replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()

records = []
skipped_lines = 0
skipped_details = []

for line_number, raw_line in enumerate(sys.stdin, start=1):
    line = raw_line.strip()
    if not line:
        continue

    try:
        data = json.loads(line)
        collected = data.get("collected_at_str", "")
        commit = data.get("commit_time_str", "")

        if not collected or not commit:
            raise ValueError("Missing timestamp fields")

        gap_seconds = int(parse_timestamp(collected) - parse_timestamp(commit))
        records.append((gap_seconds, collected, commit))
    except Exception:
        skipped_lines += 1
        preview = line[:SKIPPED_LINE_PREVIEW_LENGTH]
        if len(line) > SKIPPED_LINE_PREVIEW_LENGTH:
            preview += "..."
        skipped_details.append(f"tail line {line_number}: {preview}")

if not records:
    raise ValueError("No valid JSON lines found in the tail window")

with open(os.environ["SKIPPED_LOG"], "w", encoding="utf-8") as skipped_file:
    if skipped_details:
        skipped_file.write("\n".join(skipped_details))

min_gap = min(records, key=lambda item: item[0])
max_gap = max(records, key=lambda item: item[0])

print(
    f"{len(records)}|{skipped_lines}|{min_gap[0]}|{min_gap[1]}|{min_gap[2]}|"
    f"{max_gap[0]}|{max_gap[1]}|{max_gap[2]}"
)
' 2>"$error_log")
parse_status=$?

# Check if Python extraction worked
if [ $parse_status -ne 0 ] || [ -z "$window_stats" ]; then
    error_details=$(cat "$error_log" 2>/dev/null)
    message="$(date -u +"%Y-%m-%d %H:%M:%S UTC") - Error: Failed to parse recent JSON lines from $filename. $error_details"
    echo "$message" | mail -s "$subject" "$recipients"
    exit 1
fi

# Get lag summary for the tail window
sample_count=$(echo "$window_stats" | cut -d'|' -f1)
skipped_count=$(echo "$window_stats" | cut -d'|' -f2)
min_gap_seconds=$(echo "$window_stats" | cut -d'|' -f3)
min_gap_collected_at=$(echo "$window_stats" | cut -d'|' -f4)
min_gap_commit_time=$(echo "$window_stats" | cut -d'|' -f5)
max_gap_seconds=$(echo "$window_stats" | cut -d'|' -f6)
max_gap_collected_at=$(echo "$window_stats" | cut -d'|' -f7)
max_gap_commit_time=$(echo "$window_stats" | cut -d'|' -f8)

min_gap_hours=$((min_gap_seconds / 3600))
max_gap_hours=$((max_gap_seconds / 3600))
should_alert=0
alert_reason=""
skipped_details_section=""

if [ "$min_gap_seconds" -gt "$alert_threshold_seconds" ]; then
    should_alert=1
    alert_reason="minimum lag ${min_gap_hours} hours"
fi

if [ "$skipped_count" -gt "$skipped_alert_threshold" ]; then
    should_alert=1
    if [ -n "$alert_reason" ]; then
        alert_reason="${alert_reason}; skipped lines ${skipped_count}"
    else
        alert_reason="skipped lines ${skipped_count}"
    fi

    skipped_details=$(cat "$skipped_log" 2>/dev/null)
    if [ -n "$skipped_details" ]; then
        skipped_details_section="
Skipped tail line details:
$skipped_details"
    fi
fi

# Email message body
message="Reading tail ${tail_lines} lines of $filename - Streamer Status
Valid tail lines: $sample_count
Ignored tail lines: $skipped_count
Minimum lag in window: $min_gap_hours hours ($min_gap_seconds seconds)
Minimum lag collected at: $min_gap_collected_at
Minimum lag commit time: $min_gap_commit_time
Maximum lag in window: $max_gap_hours hours ($max_gap_seconds seconds)
Maximum lag collected at: $max_gap_collected_at
Maximum lag commit time: $max_gap_commit_time${skipped_details_section}"

# Send email if the smallest lag in the window exceeds 1 hour or too many lines were skipped.
if [ "$should_alert" -eq 1 ]; then
    echo "$message" | mail -s "ALERT: $subject - $alert_reason" "$recipients"
    echo "Alert sent: $alert_reason across last $tail_lines lines ($sample_count valid)"
else
    echo "OK: minimum lag $min_gap_hours hours ($min_gap_seconds seconds) across last $tail_lines lines ($sample_count valid)"
fi
