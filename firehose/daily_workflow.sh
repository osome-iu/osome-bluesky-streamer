#!/bin/bash

# This is for daily archiving bsky firehose data
# set the python path here
python="/Users/nick/Desktop/iuni_stuff/osome-bluesky-streamer/venv/bin/python"
backup_root_folder=""

# Ensure python is set and valid
if [[ -z "$python" ]]; then
    echo "Error: Python path is not set."
    exit 1
elif ! command -v "$python" &> /dev/null; then
    echo "Error: The specified Python path '$python' is not valid or not found."
    exit 1
fi
echo "Using Python at: $python"

# default to running dir if not set
if [[ -z "$backup_root_folder" ]]; then
    backup_root_folder="$(pwd)"
fi
# Ensure the folder exists
if [[ ! -d "$backup_root_folder" ]]; then
    echo "Error: Backup root folder '$backup_root_folder' does not exist."
    exit 1
fi
echo "Backup root folder: $backup_root_folder"

current_date=$(date +"%Y-%m-%d")
current_yyyy_mm=$(date +"%Y-%m")
filename="$current_date".json

local_file="$filename"
target_backup_folder="$backup_root_folder/$current_yyyy_mm"
target_backup_file="$backup_root_folder/$current_yyyy_mm/$filename"

# Check if local file exists
if [ ! -f "$local_file" ]; then
  echo "$(date "+%Y-%m-%d %r") Local file $local_file does not exist. Exiting."
  exit 1
fi

echo "$(date "+%Y-%m-%d %r") Starting the workflow for $current_date..."
# get counts
$python count_types.py $local_file
echo "$(date "+%Y-%m-%d %r") Generated count file"
# Ensure the target backup folder exists
if [[ ! -d "$target_backup_folder" ]]; then
    echo "Creating target backup folder: $target_backup_folder"
    mkdir -p "$target_backup_folder"
fi
cp "${current_date}_counts.csv" "$target_backup_folder/" && rm "${current_date}_counts.csv"
# Check if the remote file exists and backup as needed
if "[ -e '$target_backup_file.gz' ]"; then
    echo "$(date "+%Y-%m-%d %r") $target_backup_file already exists."
else
    echo "$(date "+%Y-%m-%d %r") Compressing, archiving, and removing $local_file..."
    command="gzip "$local_file" && cp $local_file.gz $target_backup_file.gz && rm $local_file.gz"
    echo $command
    eval $command
    echo "$(date "+%Y-%m-%d %r") Done compressing, archiving, and removing the local file"
fi
