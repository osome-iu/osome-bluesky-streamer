"""
Firehose Streamer for Bluesky Network

Author: Nick Liu

Purpose:
This script subscribes to the Bluesky network's firehose and processes commit messages.
It extracts relevant information from each commit and stores it in a JSON file, which is named
based on the current date. The script also handles restarts by resuming from the last processed sequence.

Inputs:
- Bluesky network firehose messages
- Existing JSON files or a sequence file for resuming

Outputs:
- JSON files containing processed commit information

Logging:
- Logs are saved to 'streamer_stdout.log' for standard output
- Errors are saved to 'streamer_stderr.log'

Usage:
Run the script to start processing messages from the firehose.
"""

from atproto_client.models.utils import get_or_create, get_model_as_json
from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message
import json
from datetime import datetime, timezone
import os
import sys
import logging
import base64
import signal


# Batch buffer
event_buffer = []
current_output_filename = None
N_EVENTS_PER_CHECKPOINT = 200 # No of event per batch/checkpoint

def flush_buffer():
    """Write all buffered events to disk at once"""
    global event_buffer, current_output_filename, events_since_checkpoint
    if not event_buffer or not current_output_filename:
        return

    try:
        with open(current_output_filename, "a", encoding='utf-8', errors='replace') as json_file:
            for commit_info in event_buffer:
                json_data = json.dumps(commit_info, ensure_ascii=True) + '\n'
                json_file.write(json_data)

            json_file.flush()
            os.fsync(json_file.fileno())  # Force write to disk

        logger.debug(f"Flushed {len(event_buffer)} events to {current_output_filename}")
        event_buffer.clear()
        events_since_checkpoint = 0  # Reset counter after flush

    except Exception as e:
        logger.critical(f"Failed to write batch to file: {current_output_filename} because of exception {e}")
        raise

def add_to_buffer(commit_info, output_filename, commit_seq):
    """Add event to buffer and flush if checkpoint reached"""
    global event_buffer, current_output_filename, events_since_checkpoint

    # If filename changed, flush previous buffer
    if current_output_filename and current_output_filename != output_filename:
        flush_buffer()

    current_output_filename = output_filename
    event_buffer.append(commit_info)

    # Flush when we reach the checkpoint size using sequence number
    if commit_seq % N_EVENTS_PER_CHECKPOINT == 0:
        flush_buffer()


def convert_to_json_serializable(obj):
    """Convert model objects to JSON serializable format."""
    if isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')  # or .hex()
    if hasattr(obj, '__dict__'):
        return convert_to_json_serializable(obj.__dict__)
    return obj


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:
    """
    Process commit operations and save them to a JSON file.

    Args:
        commit (models.ComAtprotoSyncSubscribeRepos.Commit): The commit message to process.
    """
    car = CAR.from_bytes(commit.blocks)
    
    input_date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    output_date_format = "%Y-%m-%d"
    collected_at_datetime = datetime.now(timezone.utc)
    collected_at = collected_at_datetime.timestamp()
    collected_at_str = collected_at_datetime.strftime(input_date_format)
    commit_datetime = datetime.strptime(commit.time, input_date_format)
    commit_timestamp = commit_datetime.timestamp()
    collect_date = collected_at_datetime.strftime(output_date_format)
    output_filename = f"{collect_date}.json"

    # Process each operation in the commit
    for op in commit.ops:
        try:
            uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')
            commit_info = {
                'seq': commit.seq,
                'collected_at': collected_at,
                'collected_at_str': collected_at_str,
                'commit_time': commit_timestamp,
                'commit_time_str': commit.time,
                'action': op.action,
                'type': uri.collection,
                'uri': str(uri),
                'author': commit.repo,
                'cid': str(op.cid)
            }

            try:
                record_json = None
                record_raw_data = car.blocks.get(op.cid)
                record = get_or_create(record_raw_data, strict=False)
                if record:
                    record_json = convert_to_json_serializable(record)
                    commit_info.update(record_json)
            except Exception as e:
                logger.error(f"Failed to update with info from blocks\nError: {e}\nRecord content not parsed: {record}\nCommit Info: {commit_info}")

            # Use batched writing instead of immediate file write                
            add_to_buffer(commit_info, output_filename, commit.seq)
        except Exception as e:
            logger.error(f"Failed to get basic info from: {op.cid}, {uri} because of exception: {e}")


if __name__ == '__main__':
    # Configure the logger
    log_folder = "log"
    # Ensure log folder exists
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(os.path.join(log_folder, 'streamer_stdout.log')),
        ]
    )
    logger = logging.getLogger(os.path.join(log_folder, 'firehose_stream_logger'))
    sys.stderr = open(os.path.join(log_folder, 'streamer_stderr.log'), 'a')

    logger.info("Starting Firehose Streamer...")

    shutdown_requested = False
    client = None  # Initialize client variable
    last_seq_file = "last_seq"
    last_seq = None

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        global shutdown_requested, client
        shutdown_requested = True
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")

        # Flush any remaining events before shutdown
        flush_buffer()

        if client is not None:
            try:
                client.stop()
                logger.info("Client stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping client: {e}")
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    def checkpoint_seq(seq: int, force: bool = False) -> None:
        """Persist cursor and notify client so we can skip bad events."""
        global last_seq
        if last_seq is None:
            last_seq = seq
        else:
            last_seq = max(seq, last_seq)

        if force or (seq % N_EVENTS_PER_CHECKPOINT == 0):
            if force:
                logger.info(f"Forcing checkpoint at seq: {last_seq}")
            try:
                client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
            except Exception as e:
                logger.error(f"Failed to update cursor to {last_seq}: {e}")

            try:
                with open(last_seq_file, 'w+', encoding='utf-8', errors='replace') as file:
                    file.write(str(last_seq))
            except Exception as e:
                logger.error(f"Failed to persist last_seq {last_seq}: {e}")

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        """
        Handle incoming messages from the firehose.

        Args:
            message (firehose_models.MessageFrame): The incoming message frame.
        """
        if shutdown_requested:
            return
            
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        if not commit.blocks:
            return

        success = False
        try:
            _get_ops_by_type(commit)
            success = True
        except Exception as e:
            logger.error(f"Failed processing commit seq {commit.seq}: {e}")
        except BaseException as e:  # Catches pyo3 PanicException
            logger.error(f"Low-level panic at seq {commit.seq}, skipping: {e}")
            success = False
        finally:
            # Always checkpoint; on failure, advance the cursor to skip the bad seq
            next_seq = commit.seq if success else commit.seq + 1
            checkpoint_seq(next_seq, force=not success)

    def on_callback_error_handler(error: BaseException):
        """
        Handle errors from the callback.

        Args:
            error (BaseException): The error encountered.
        """
        logger.error(f'Got error! {error}')

    client = FirehoseSubscribeReposClient(base_uri='wss://bsky.network/xrpc')

    try:
        json_files = [file for file in os.listdir() if file.endswith('.json')]
        if last_seq:  # DEBUG only
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
            logger.info(f"Streamer Started with DEBUG seq: {last_seq}")
        else:
            # Primary resume source: last_seq file
            if os.path.exists(last_seq_file):
                try:
                    with open(last_seq_file, 'r', encoding='utf-8', errors='replace') as file:
                        last_seq = int(file.readline().strip())
                        client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
                        logger.info(f"Streamer Started with file {last_seq_file} from seq: {last_seq}")
                except Exception as e:
                    logger.warning(f"Failed to read {last_seq_file}: {e}")

            # Fallback: scan most recent JSON to find last valid line if no saved cursor
            if last_seq is None and len(json_files) > 0:
                latest_json_file = max(json_files, key=lambda x: os.path.getmtime(x))
                last_seq = None
                lines_to_read = N_EVENTS_PER_CHECKPOINT * 2
                
                with open(latest_json_file, 'r', encoding='utf-8', errors='replace') as file:
                    file.seek(0, os.SEEK_END)
                    file_size = file.tell()
                    chunk_size = 8192  # 8KB chunks
                    buffer = ''
                    position = file_size
                    lines_found = 0
                    
                    while position > 0 and lines_found < lines_to_read:
                        position = max(0, position - chunk_size)
                        file.seek(position)
                        chunk = file.read(chunk_size if position > 0 else file_size)
                        buffer = chunk + buffer
                        lines_found = buffer.count('\n')
                    
                    lines = buffer.split('\n')
                    if lines[-1] == '':
                        lines = lines[:-1]
                    lines_to_check = lines[-lines_to_read:] if len(lines) > lines_to_read else lines
                    
                    for line in reversed(lines_to_check):
                        if line.strip():
                            try:
                                last_line_json = json.loads(line)
                                last_seq = int(last_line_json['seq']) + N_EVENTS_PER_CHECKPOINT
                                break
                            except (json.JSONDecodeError, KeyError, ValueError) as e:
                                logger.warning(f"Skipping corrupted JSON line: {line[:100]}... Error: {e}")
                                continue

                if last_seq:
                    client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
                    logger.info(f"Streamer Started with existing JSON from seq: {last_seq}")
                else:
                    logger.warning(f"No valid JSON found in last {lines_to_read} lines of {latest_json_file}")
                    logger.info("Streamer Started fresh")
            elif last_seq is None:
                logger.info("Streamer Started fresh")
        client.start(on_message_handler, on_callback_error_handler)
    except Exception as e:
        logger.critical(f"Streamer crashed because of error: {e}")
        flush_buffer()  # Try to save any buffered events
        sys.exit(1)