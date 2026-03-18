#!/bin/bash
# Improved Supervisor eventlistener: logs + emails on all PROCESS_STATE changes
# Drop this into supervisor_email_notify.sh and make executable (chmod +x).

email_recipients="email recipients"
LOGFILE="/path/to/logfile"

# Optional mailx SMTP (uncomment and configure if needed)
# export MAILRC=/dev/null
# export MAILX_OPTS="-S smtp-use-starttls -S smtp-auth=login \
#   -S smtp=smtp.gmail.com:587 \
#   -S smtp-auth-user=your@gmail.com \
#   -S smtp-auth-password='your-app-password' \
#   -S from='Supervisor <your@gmail.com>'"

# Maximum allowed payload length (bytes) to avoid runaway reads on corrupt headers
MAX_LEN=1048576   # 1 MiB, tune if necessary

mkdir -p "$(dirname "$LOGFILE")"
touch "$LOGFILE"
chmod 644 "$LOGFILE"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Email listener started." >> "$LOGFILE"

while true; do
  # Tell supervisor we're ready for the next event. Supervisor expects a newline.
  echo "READY"

  # Read header line (raw). Use -r to avoid backslash escapes.
  if ! IFS= read -r header; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] read header failed — exiting." >> "$LOGFILE"
    break
  fi

  # Trim trailing CR if present (some senders include \r\n)
  header=${header%$'\r'}

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Received header: $header" >> "$LOGFILE"

  # Extract length (len:<number>) robustly
  if [[ "$header" =~ len:([0-9]+) ]]; then
    len=${BASH_REMATCH[1]}
  else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: No length found in header: $header" >> "$LOGFILE"
    # Acknowledge with empty result and continue
    echo -ne "RESULT 2\nOK"
    continue
  fi

  # Validate len is numeric and within sane bounds
  if ! [[ "$len" =~ ^[0-9]+$ ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: Non-numeric length: $len" >> "$LOGFILE"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  if (( len <= 0 )); then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: zero or negative length: $len" >> "$LOGFILE"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  if (( len > MAX_LEN )); then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: length $len exceeds MAX_LEN ($MAX_LEN). Skipping." >> "$LOGFILE"
    # consume and discard the payload so we remain in sync
    dd if=/dev/stdin bs=1 count=$len of=/dev/null 2>/dev/null || {
      # fallback: attempt to read into /dev/null with read -N repeatedly
      remaining=$len
      while (( remaining > 0 )); do
        chunk=$(( remaining > 4096 ? 4096 : remaining ))
        IFS= read -r -N "$chunk" _ || break
        remaining=$(( remaining - chunk ))
      done
    }
    echo -ne "RESULT 2\nOK"
    continue
  fi

  # Read payload of exactly $len bytes
  if ! IFS= read -r -N "$len" payload; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: failed to read payload of length $len" >> "$LOGFILE"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  # Trim possible trailing CR from payload (helps when payload lines end with \r\n)
  payload=${payload%$'\r'}

  timestamp=$(date '+%Y-%m-%d %H:%M:%S')

  # Parse event name from header by looking for eventname:<value>
  if [[ "$header" =~ eventname:([^[:space:]]+) ]]; then
    event_name=${BASH_REMATCH[1]}
  else
    # As a fallback, try first token (legacy), but prefer eventname: token
    event_name=$(echo "$header" | awk '{print $1}')
  fi

  # Only process PROCESS_STATE events
  if [[ "$event_name" == PROCESS_STATE_* ]]; then
    # Defaults
    process="unknown"
    from_state="unknown"

    # Extract processname from payload (payload is like "processname:foo groupname:bar ...")
    if [[ "$payload" =~ processname:([^[:space:],]+) ]]; then
      process=${BASH_REMATCH[1]}
    fi

    # Extract from_state from payload
    if [[ "$payload" =~ from_state:([^[:space:],]+) ]]; then
      from_state=${BASH_REMATCH[1]}
    fi

    # Determine to_state by stripping prefix
    to_state=${event_name#PROCESS_STATE_}

    log_line="[$timestamp] $process changed: $from_state → $to_state"
    echo "$log_line" >> "$LOGFILE"

    subject="Supervisor: $process → $to_state"
    body=$(cat <<EOF
      Supervisor detected a process state change.

      Process: $process
      From:    $from_state
      To:      $to_state
      Host:    $(hostname)
      Time:    $timestamp

      Raw Event:
      $payload
      EOF
    )

    if ! echo "$body" | mailx -s "$subject" $email_recipients; then
      echo "[$timestamp] ❌ Failed to send email for $process ($to_state)" >> "$LOGFILE"
    else
      echo "[$timestamp] ✅ Email sent for $process ($to_state)" >> "$LOGFILE"
    fi
  else
    # Log non-PROCESS_STATE events but don't email
    echo "[$timestamp] Skipping non-PROCESS_STATE event: $event_name" >> "$LOGFILE"
  fi

  # Tell supervisor we're done for this event
  echo -ne "RESULT 2\nOK"
done
