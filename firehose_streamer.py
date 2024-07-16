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


def convert_to_json_serializable(obj):
    """Convert model objects to JSON serializable format."""
    if isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    return get_model_as_json(obj)


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
                    commit_info.update(json.loads(record_json))
            except Exception as e:
                logger.error(f"Failed to update with info from blocks\nError: {e}\nRecord content not parsed: {record}\nCommit Info: {commit_info}")
            finally:
                try:
                    with open(output_filename, "at+", encoding='utf-8') as json_file:
                        json_file.write(f'{json.dumps(commit_info)}\n')
                except Exception as e:
                    logger.critical(f"Failed to write to file: {output_filename} because of exception {e}")
                    sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to get basic info from: {op.cid}, {uri} because of exception: {e}")


if __name__ == '__main__':
    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('streamer_stdout.log'),
        ]
    )
    logger = logging.getLogger('firehose_stream_logger')
    sys.stderr = open('streamer_stderr.log', 'a')

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        """
        Handle incoming messages from the firehose.

        Args:
            message (firehose_models.MessageFrame): The incoming message frame.
        """
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        if not commit.blocks:
            return
        # Update stored state every ~20 events
        if commit.seq % 20 == 0:
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))
            global last_seq
            if not last_seq:
                last_seq = commit.seq
            else:
                last_seq = max(commit.seq, last_seq)
                with open(last_seq_file, 'w+') as file:
                    file.write(str(last_seq))

        _get_ops_by_type(commit)

    def on_callback_error_handler(error: BaseException):
        """
        Handle errors from the callback.

        Args:
            error (BaseException): The error encountered.
        """
        logger.error('Got error!', error)

    client = FirehoseSubscribeReposClient(base_uri='wss://bsky.network/xrpc')
    last_seq_file = "last_seq"
    last_seq = None

    try:
        json_files = [file for file in os.listdir() if file.endswith('.json')]
        if last_seq:  # DEBUG only
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
            logger.info(f"Streamer Started with DEBUG seq: {last_seq}")
        elif len(json_files) > 0:  # if restarting the streamer
            # Sort the files by modification time and get the most recently modified file
            latest_json_file = max(json_files, key=lambda x: os.path.getmtime(x))
            # Get the last line of the latest modified file
            with open(latest_json_file, 'r') as file:
                # Seek to the end of the file
                file.seek(0, os.SEEK_END)
                file.seek(file.tell() - 2, os.SEEK_SET)
                while file.read(1) != '\n':
                    file.seek(file.tell() - 2, os.SEEK_SET)
                last_line = file.readline()
                last_line_json = json.loads(last_line)

            last_seq = int(last_line_json['seq'])
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
            logger.info(f"Streamer Started with existing JSON from seq: {last_seq}")
        elif os.path.exists(last_seq_file):
            with open(last_seq_file, 'r') as file:
                last_seq = int(file.readline())
                client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=last_seq))
                logger.info(f"Streamer Started with file {last_seq_file} from seq: {last_seq}")
        else:
            logger.info("Streamer Started fresh")
        client.start(on_message_handler, on_callback_error_handler)
    except Exception as e:
        logger.critical(f"Streamer crashed because of error: {e}")
