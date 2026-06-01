#!/bin/bash
# Supervisor eventlistener: smart email alerting with flap detection,
# stuck-state monitoring, and per-process cooldowns.
#
# Alert conditions:
#   1. Restart loop  — process hits BACKOFF/EXITED/FATAL ≥ FLAP_THRESHOLD
#                      times within FLAP_WINDOW seconds
#   2. Stuck STARTING — process stays in STARTING for > STARTING_DELAY seconds
#   3. Stuck bad state — process stays in FATAL/BACKOFF/EXITED/STOPPED for
#                        > BAD_STATE_DELAY seconds
#   4. FATAL (immediate) — process reaches FATAL state (always alert once
#                           cooldown allows, no delay needed)

# ── Configuration ────────────────────────────────────────────────────────────

email_recipients="email recipients"
LOGFILE="/path/to/logfile"

STATE_DIR="/tmp/supervisor_monitor"

STARTING_DELAY=300    # seconds before alerting on a stuck STARTING state
BAD_STATE_DELAY=300   # seconds before alerting on a stuck FATAL/BACKOFF/EXITED/STOPPED
FLAP_WINDOW=300       # rolling window (seconds) used to count restarts
FLAP_THRESHOLD=5      # restart count within FLAP_WINDOW that triggers a flap alert
COOLDOWN=600          # minimum seconds between emails for the same process

MAX_LEN=1048576       # max payload bytes (1 MiB)

# Optional mailx SMTP (uncomment and configure if needed)
# export MAILRC=/dev/null
# export MAILX_OPTS="-S smtp-use-starttls -S smtp-auth=login \
#   -S smtp=smtp.gmail.com:587 \
#   -S smtp-auth-user=your@gmail.com \
#   -S smtp-auth-password='your-app-password' \
#   -S from='Supervisor <your@gmail.com>'"

# ── Helpers ───────────────────────────────────────────────────────────────────

mkdir -p "$STATE_DIR"
mkdir -p "$(dirname "$LOGFILE")"
touch "$LOGFILE"
chmod 644 "$LOGFILE"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOGFILE"
}

send_email() {
  local subject="$1"
  local body="$2"
  local process="$3"   # used only for log messages

  if ! echo "$body" | mailx -s "$subject" $email_recipients; then
    log "❌ Failed to send email for $process — subject: $subject"
  else
    log "✅ Email sent for $process — subject: $subject"
  fi
}

# Returns 0 (true) if we are outside the cooldown window for $process and
# updates the cooldown timestamp.  Returns 1 (false) if still in cooldown.
should_send_alert() {
  local process="$1"
  local now
  now=$(date +%s)
  local cooldown_file="$STATE_DIR/${process}.cooldown"

  if [[ -f "$cooldown_file" ]]; then
    local last
    last=$(cat "$cooldown_file")
    if (( now - last < COOLDOWN )); then
      log "  Cooldown active for $process — skipping alert ($(( COOLDOWN - (now - last) ))s remaining)"
      return 1
    fi
  fi

  echo "$now" > "$cooldown_file"
  return 0
}

# ── State-change handlers ─────────────────────────────────────────────────────

handle_starting() {
  local process="$1"
  local now
  now=$(date +%s)
  local start_file="$STATE_DIR/${process}.starting"

  echo "$now" > "$start_file"
  log "  Recorded STARTING timestamp for $process — will alert if not RUNNING within ${STARTING_DELAY}s"

  # Subshell: wake up after the delay and alert if still stuck
  (
    sleep "$STARTING_DELAY"
    if [[ -f "$start_file" ]]; then
      local saved
      saved=$(cat "$start_file")
      if [[ "$saved" == "$now" ]]; then
        if should_send_alert "$process"; then
          local subject="Supervisor: $process stuck in STARTING"
          local body
          body="Process $process has been in STARTING for over $(( STARTING_DELAY / 60 )) minutes.

Host:    $(hostname)
Time:    $(date)
Process: $process"
          send_email "$subject" "$body" "$process"
          log "🚨 STARTING stuck alert sent for $process"
        fi
      fi
    fi
  ) &
}

handle_running() {
  local process="$1"
  # Clear stuck-state sentinels when the process recovers
  rm -f "$STATE_DIR/${process}.starting"
  rm -f "$STATE_DIR/${process}.bad"
  log "  $process is RUNNING — cleared stuck-state sentinels"
}

handle_flap_state() {
  # Called for BACKOFF / EXITED / FATAL — records the event and checks for a
  # restart loop within FLAP_WINDOW.
  local process="$1"
  local to_state="$2"
  local now
  now=$(date +%s)
  local flap_file="$STATE_DIR/${process}.flap"

  # Append current timestamp
  echo "$now" >> "$flap_file"

  # Prune entries older than FLAP_WINDOW
  local tmp_file="${flap_file}.tmp"
  awk -v now="$now" -v window="$FLAP_WINDOW" \
    '$1 >= now - window' "$flap_file" > "$tmp_file" && mv "$tmp_file" "$flap_file"

  local count
  count=$(wc -l < "$flap_file")
  log "  Flap count for $process: $count / $FLAP_THRESHOLD in last ${FLAP_WINDOW}s"

  if (( count >= FLAP_THRESHOLD )); then
    if should_send_alert "$process"; then
      local subject="Supervisor: $process restart loop detected"
      local body
      body="Process $process has restarted $count times within the last $(( FLAP_WINDOW / 60 )) minutes.

Host:    $(hostname)
Time:    $(date)
Process: $process
State:   $to_state

Recent event timestamps:
$(cat "$flap_file")"
      send_email "$subject" "$body" "$process"
      log "🚨 Restart loop alert sent for $process ($count events in ${FLAP_WINDOW}s)"
    fi
  fi
}

handle_bad_state() {
  # Called for FATAL / BACKOFF / EXITED / STOPPED — sets a sentinel and
  # schedules a delayed alert if the process remains in this state.
  local process="$1"
  local to_state="$2"
  local now
  now=$(date +%s)
  local bad_file="$STATE_DIR/${process}.bad"

  echo "$now" > "$bad_file"
  log "  Recorded bad-state ($to_state) timestamp for $process — will alert if not recovered within ${BAD_STATE_DELAY}s"

  (
    sleep "$BAD_STATE_DELAY"
    if [[ -f "$bad_file" ]]; then
      local saved
      saved=$(cat "$bad_file")
      if [[ "$saved" == "$now" ]]; then
        if should_send_alert "$process"; then
          local subject="Supervisor: $process stuck in $to_state"
          local body
          body="Process $process has remained in $to_state for over $(( BAD_STATE_DELAY / 60 )) minutes with no recovery.

Host:    $(hostname)
Time:    $(date)
Process: $process
State:   $to_state"
          send_email "$subject" "$body" "$process"
          log "🚨 Stuck bad-state ($to_state) alert sent for $process"
        fi
      fi
    fi
  ) &
}

# ── Main event loop ───────────────────────────────────────────────────────────

log "Email listener started."

while true; do
  echo "READY"

  # ── Read header ──
  if ! IFS= read -r header; then
    log "read header failed — exiting."
    break
  fi
  header=${header%$'\r'}
  log "Received header: $header"

  # ── Extract payload length ──
  if [[ "$header" =~ len:([0-9]+) ]]; then
    len=${BASH_REMATCH[1]}
  else
    log "Warning: No length found in header: $header"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  if ! [[ "$len" =~ ^[0-9]+$ ]] || (( len <= 0 )); then
    log "Warning: Invalid length value: $len"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  if (( len > MAX_LEN )); then
    log "Warning: length $len exceeds MAX_LEN ($MAX_LEN) — discarding payload."
    dd if=/dev/stdin bs=1 count="$len" of=/dev/null 2>/dev/null
    echo -ne "RESULT 2\nOK"
    continue
  fi

  # ── Read payload ──
  if ! IFS= read -r -N "$len" payload; then
    log "Warning: failed to read payload of length $len"
    echo -ne "RESULT 2\nOK"
    continue
  fi
  payload=${payload%$'\r'}

  timestamp=$(date '+%Y-%m-%d %H:%M:%S')

  # ── Parse event name ──
  if [[ "$header" =~ eventname:([^[:space:]]+) ]]; then
    event_name=${BASH_REMATCH[1]}
  else
    event_name=$(echo "$header" | awk '{print $1}')
  fi

  # ── Only handle PROCESS_STATE_* events ──
  if [[ "$event_name" != PROCESS_STATE_* ]]; then
    log "Skipping non-PROCESS_STATE event: $event_name"
    echo -ne "RESULT 2\nOK"
    continue
  fi

  # ── Parse process name and states ──
  process="unknown"
  from_state="unknown"

  [[ "$payload" =~ processname:([^[:space:],]+) ]] && process=${BASH_REMATCH[1]}
  [[ "$payload" =~ from_state:([^[:space:],]+) ]]  && from_state=${BASH_REMATCH[1]}

  to_state=${event_name#PROCESS_STATE_}

  log "$process changed: $from_state → $to_state"

  # ── Route to the appropriate handler ──
  case "$to_state" in

    STARTING)
      handle_starting "$process"
      ;;

    RUNNING)
      handle_running "$process"
      # Send a recovery email when email_on_state_change itself comes back up
      if [[ "$process" == "email_on_state_change" ]]; then
        subject="Supervisor: email_on_state_change is back up"
        body="The supervisor event listener has restarted and is running.

    Host:    $(hostname)
    Time:    $(date)
    Process: $process
    From:    $from_state → RUNNING

    Email alerts have resumed."
        send_email "$subject" "$body" "$process"
        log "📧 RUNNING email sent for $process"
      fi
      ;;

    BACKOFF|EXITED)
      # Flap tracking only — bad-state delayed alert not needed here because
      # supervisor will attempt a restart automatically; we care about loops.
      handle_flap_state "$process" "$to_state"
      handle_bad_state  "$process" "$to_state"
      ;;

    FATAL)
      # FATAL means supervisor has given up — track flap history AND set a
      # bad-state sentinel for the stuck-state delayed alert.
      handle_flap_state "$process" "$to_state"
      handle_bad_state  "$process" "$to_state"
      ;;

    STOPPING)
        # Bypass cooldown — always alert on STOPPING so manual restarts are never missed
        subject="Supervisor: $process is stopping"
        body="Process $process is being stopped.

    Host:    $(hostname)
    Time:    $(date)
    Process: $process
    From:    $from_state → STOPPING"
        send_email "$subject" "$body" "$process"
        log "📧 STOPPING alert sent for $process"
    ;;

    STOPPED)
      # Intentional stops are common (deploys, maintenance).  Only alert if
      # the process stays stopped for longer than BAD_STATE_DELAY.
      handle_bad_state "$process" "$to_state"
      ;;

    UNKNOWN|EXITED)
      log "  Unhandled to_state '$to_state' for $process — no action taken."
      ;;

  esac

  echo -ne "RESULT 2\nOK"
done