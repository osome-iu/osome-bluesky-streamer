#!/bin/bash

# Example usages:
# ./daily_workflow.sh -> processes yesterday's data
# ./daily_workflow.sh --date 2025-10-22 -> processes a specific date
# ./daily_workflow.sh --date 2025-10-22 --dry-run -> simulates the workflow for a specific date

# This is for daily archiving bsky firehose data
# set the python path here
python="/path/to/osome-bluesky-streamer/venv/bin/python"
backup_root_folders=("/path/to/backup/folder" "/another/backup/folder")

# Ensure python is set and valid
if [[ -z "$python" ]]; then
    echo "Error: Python path is not set."
    exit 1
elif ! command -v "$python" &> /dev/null; then
    echo "Error: The specified Python path '$python' is not valid or not found."
    exit 1
fi
echo "Using Python at: $python"

# Check if inside a git repository
if ! git rev-parse --is-inside-work-tree &> /dev/null; then
    echo "Error: Not inside a git repository."
    exit 1
fi

# Check if running inside the firehose directory
if [[ "$(basename "$(pwd)")" != "firehose" ]]; then
    echo "Error: Script must be run from the firehose/ directory."
    exit 1
fi

# Parse arguments
DRY_RUN=0
input_date=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry|--dry-run)
            DRY_RUN=1
            shift
            ;;
        --date)
            input_date="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if [ -z "$input_date" ]; then
    # Portable yesterday calculation
    if date -u -d yesterday +"%Y-%m-%d" &>/dev/null; then
        input_date=$(date -u -d "yesterday" +"%Y-%m-%d")
    else
        input_date=$(date -u -v -1d +"%Y-%m-%d")
    fi
fi

yesterday="$input_date"
# Extract YYYY-MM from YYYY-MM-DD
yesterdays_yyyy_mm="${yesterday:0:7}"
filename="$yesterday.json"

local_file="$filename"
counts_file="${yesterday}_counts.csv"

# Check if local file exists
if [ ! -f "$local_file" ]; then
  echo "$(date "+%Y-%m-%d %r") Local file $local_file does not exist. Exiting."
  exit 1
fi

echo "$(date "+%Y-%m-%d %r") Starting the workflow for $yesterday..."
# get counts
if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY RUN] Would run: $python count_types.py $local_file"
else
    $python count_types.py $local_file
fi

echo "$(date "+%Y-%m-%d %r") Generated count file"

# Compress the JSON file if not already compressed
if [ ! -f "$local_file.gz" ]; then
    echo "Compressing $local_file..."
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "[DRY RUN] Would run: gzip -c $local_file > $local_file.gz && touch gzip_status_success.txt"
    else
        gzip -c "$local_file" > "$local_file.gz" && touch gzip_status_success.txt
        if [ -f gzip_status_success.txt ]; then
            rm "$local_file"
            rm gzip_status_success.txt
            echo "Compression successful, original $local_file removed."
        else
            echo "Compression failed, original $local_file kept."
            exit 1
        fi
    fi
fi

# Check backup_root_folders
if [[ ${#backup_root_folders[@]} -eq 0 ]]; then
    echo "Error: No backup root folders specified. Exiting."
    exit 1
else
    echo "Backup root folders:"
    for folder in "${backup_root_folders[@]}"; do
        echo "  $folder"
    done
fi

# Loop through all backup folders and copy files
for backup_root_folder in "${backup_root_folders[@]}"; do
    target_backup_folder="$backup_root_folder/$yesterdays_yyyy_mm"
    target_backup_file="$target_backup_folder/$filename.gz"
    target_counts_file="$target_backup_folder/${yesterday}_counts.csv"

    # Skip if the backup root folder does not exist
    if [[ ! -d "$backup_root_folder" ]]; then
        echo "Skipping $backup_root_folder (does not exist)"
        continue
    fi

    # Ensure the target backup folder exists
    if [[ ! -d "$target_backup_folder" ]]; then
        if [[ $DRY_RUN -eq 1 ]]; then
            echo "[DRY RUN] Would run: mkdir -p $target_backup_folder"
        else
            echo "Creating target backup folder: $target_backup_folder"
            mkdir -p "$target_backup_folder"
        fi
    fi

    # Copy the counts CSV
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "[DRY RUN] Would run: cp $counts_file $target_counts_file"
    else
        cp "$counts_file" "$target_counts_file"
        echo "Copied $counts_file to $target_counts_file"
    fi

    # Copy the compressed JSON file
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "[DRY RUN] Would run: cp $local_file.gz $target_backup_file"
    else
        cp "$local_file.gz" "$target_backup_file"
        echo "Copied $local_file.gz to $target_backup_file"
    fi

done

# remove the local compressed file and counts file
if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY RUN] Would run: rm $local_file.gz $counts_file"
else
    rm "$local_file.gz" "$counts_file"
    echo "Removed local files: $local_file.gz and $counts_file"
fi
