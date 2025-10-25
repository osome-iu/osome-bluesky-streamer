#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# =============================================================================
#  Bluesky Firehose Daily Workflow Script
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
  "/path/to/backup/folder"
  "/another/backup/folder"
)

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
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
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

# --- DATE HANDLING ---
if [[ -z "$input_date" ]]; then
  if date -u -d yesterday +%Y-%m-%d &>/dev/null; then
    input_date=$(date -u -d "yesterday" +"%Y-%m-%d")
  else
    input_date=$(date -u -v -1d +"%Y-%m-%d")
  fi
fi

yesterday="$input_date"
yesterdays_yyyy_mm="${yesterday:0:7}"
filename="$yesterday.json"
local_file="$filename"
counts_file="${yesterday}_counts.csv"

# --- LOGGING SETUP ---
log_dir="log"
mkdir -p "$log_dir"
logfile="$log_dir/daily_workflow_${yesterday}.log"
exec > >(tee -a "$logfile") 2>&1
log "=== Starting daily workflow for $yesterday ==="

# --- ENVIRONMENT CHECKS ---
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  log "Error: Not inside a git repository."; exit 1
fi

if [[ "$(basename "$(pwd)")" != "firehose" ]]; then
  log "Error: Must be run from firehose/ directory."; exit 1
fi

if [ ! -f "$local_file" ]; then
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

# --- GZIP WITH SAFER TEMP FLAG ---
if [ ! -f "$local_file.gz" ]; then
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
      src_size=$(stat -c%s "$local_file.gz")
      dst_size=$(stat -c%s "$target_backup_file")
      if [[ $src_size -eq $dst_size ]]; then
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

log "=== Workflow completed for $yesterday ==="
