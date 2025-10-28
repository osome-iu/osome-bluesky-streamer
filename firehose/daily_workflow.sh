#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# =============================================================================
#  Bluesky Firehose Daily Workflow Script
#
#  This script automates the daily processing, compression, and backup of bluesky firehose data files.
#  It counts data types, compresses files safely, and backs them up to specified locations.
#
#  All dates and times are handled in UTC to ensure consistency.
#  Please set cronjob to run at a time that aligns with your data availability (e.g., early morning UTC).
#
#  Example usages:
#    ./daily_workflow.sh
#    ./daily_workflow.sh --date 2025-10-22
#    ./daily_workflow.sh --date 2025-10-22 --dry-run
#    ./daily_workflow.sh --help
# =============================================================================

# --- CONFIGURATION ---
# Default hardcoded fallback Python path
FALLBACK_PYTHON="/path/to/osome-bluesky-streamer/venv/bin/python"
backup_root_folders=(
  # "/path/to/backup/folder"
  # "/another/backup/folder"
) # default empty; will use "./current_directory/backup" if none specified

# --- FUNCTIONS ---
show_help() {
cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --date YYYY-MM-DD   Process data for a specific date (default: yesterday)
  --dry, --dry-run    Simulate without making changes
  --help              Show this help message and exit

Examples:
  $0
  $0 --date 2025-10-22
  $0 --dry-run
EOF
}

log() {
  echo "[$(date -u +'%Y-%m-%d %H:%M:%S')] $*"
}

# --- LOGGING SETUP ---
log_dir="log"
mkdir -p "$log_dir"
today="$(date -u +"%Y-%m-%d")"
logfile="$log_dir/daily_workflow_${today}.log"
exec > >(tee -a "$logfile") 2>&1
log "======================== Starting daily workflow ========================"

get_file_size() {
  local file="$1"
  if [[ "$(uname)" == "Darwin" ]]; then
    stat -f%z "$file" 2>/dev/null || echo 0
  else
    stat -c%s "$file" 2>/dev/null || echo 0
  fi
}

get_yesterday() {
  if [[ "$(uname)" == "Darwin" ]]; then
    date -u -v -1d +"%Y-%m-%d"
  else
    date -u -d "yesterday" +"%Y-%m-%d"
  fi
}

# --- ARGUMENT PARSING ---
DRY_RUN=0
input_date=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry|--dry-run)
      DRY_RUN=1; shift ;;
    --date)
      input_date="$2"; shift 2 ;;
    --help)
      show_help; exit 0 ;;
    *)
      echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# --- PYTHON DETECTION ---
if [[ -n "${VIRTUAL_ENV:-}" && -x "$VIRTUAL_ENV/bin/python" ]]; then
  python="$VIRTUAL_ENV/bin/python"
elif command -v python3 &>/dev/null; then
  python="$(command -v python3)"
elif [[ -x "$FALLBACK_PYTHON" ]]; then
  python="$FALLBACK_PYTHON"
else
  echo "Error: Could not find a valid Python interpreter."
  exit 1
fi
log "Using Python: $python"

# --- DEFAULT BACKUP PATH FALLBACK ---
if [[ ${#backup_root_folders[@]} -eq 0 ]]; then
  backup_root_folders=("$PWD/backup")
  log "No backup folders specified; defaulting to current directory: ${backup_root_folders[0]}"
  if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY RUN] Would create backup folder: ${backup_root_folders[0]}"
  else
    mkdir -p "${backup_root_folders[0]}"
  fi
fi

# --- DATE HANDLING ---
if [[ -z "$input_date" ]]; then
  input_date="$(get_yesterday)"
else
  if [[ ! "$input_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log "Error: Invalid date format. Use YYYY-MM-DD. You provided: $input_date"
    exit 1
  fi
fi

yesterday="$input_date"
yesterdays_yyyy_mm="${yesterday:0:7}"
filename="$yesterday.json"
local_file="$filename"
counts_file="${yesterday}_counts.csv"

# --- ENVIRONMENT CHECKS ---
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  log "Error: Not inside a Git repository."; exit 1
fi

current_dir_name="$(basename "$(pwd)")"
if [[ "$current_dir_name" != "firehose" ]]; then
  log "Error: Must be run from the 'firehose' directory (currently in '$current_dir_name')."
  exit 1
fi

if [[ ! -f "$local_file" ]]; then
  log "Error: Local file $local_file not found."; exit 1
fi

# --- COUNT TYPES ---
if [[ $DRY_RUN -eq 1 ]]; then
  log "[DRY RUN] Would run: $python count_types.py $local_file"
else
  if ! $python count_types.py "$local_file"; then
    log "Error: count_types.py failed."; exit 1
  fi
fi
log "Count file generated: $counts_file"

# --- GZIP WITH SAFE TEMP FLAG ---
if [[ ! -f "$local_file.gz" ]]; then
  log "Compressing $local_file..."
  if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY RUN] Would gzip $local_file"
  else
    tmp_gz="${local_file}.gz.tmp"
    if gzip -c "$local_file" > "$tmp_gz"; then
      mv "$tmp_gz" "$local_file.gz"
      rm "$local_file"
      log "Compression successful. Original removed."
    else
      rm -f "$tmp_gz"
      log "Compression failed, keeping original file."
      exit 1
    fi
  fi
else
  log "File already compressed: $local_file.gz"
fi

# --- BACKUP ---
if [[ ${#backup_root_folders[@]} -eq 0 ]]; then
  log "Error: No backup folders specified."; exit 1
fi

all_backups_success=1

for backup_root_folder in "${backup_root_folders[@]}"; do
  target_backup_folder="$backup_root_folder/$yesterdays_yyyy_mm"
  target_backup_file="$target_backup_folder/$filename.gz"
  target_counts_file="$target_backup_folder/${yesterday}_counts.csv"

  if [[ ! -d "$backup_root_folder" ]]; then
    log "Skipping $backup_root_folder (not found)"
    all_backups_success=0
    continue
  fi

  if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY RUN] Would create folder: $target_backup_folder"
    log "[DRY RUN] Would copy $counts_file → $target_counts_file"
    log "[DRY RUN] Would copy $local_file.gz → $target_backup_file"
  else
    mkdir -p "$target_backup_folder"
    if cp "$counts_file" "$target_counts_file" && cp "$local_file.gz" "$target_backup_file"; then
      # --- INTEGRITY CHECK ---
      src_size=$(get_file_size "$local_file.gz")
      dst_size=$(get_file_size "$target_backup_file")
      if [[ "$src_size" -eq "$dst_size" && "$src_size" -gt 0 ]]; then
        log "Backup verified OK at $target_backup_folder"
      else
        log "Warning: Size mismatch at $target_backup_folder"
        all_backups_success=0
      fi
    else
      log "Backup failed for $backup_root_folder"
      all_backups_success=0
    fi
  fi
done

# --- CLEANUP ---
if [[ $DRY_RUN -eq 1 ]]; then
  log "[DRY RUN] Would remove local files only if all backups succeeded."
else
  if [[ $all_backups_success -eq 1 ]]; then
    rm -f "$local_file.gz" "$counts_file"
    log "All backups succeeded. Removed local files."
  else
    log "Backup verification failed — keeping local copies."
  fi
fi

log "========================    Workflow completed   ========================"
