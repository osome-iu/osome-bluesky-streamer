###########################################
# Stream label events from labelers' endpoints
# Docs :
#   - https://atproto.blue/en/latest/atproto_firehose/index.html#atproto_firehose.AsyncFirehoseSubscribeLabelsClient
#   - https://docs.bsky.app/docs/advanced-guides/moderation#labels
#
# input : "labelers_service_endpoints.csv" list of labeler endpoints
# output : label events 1 file per endpoint to "{STREAMS_DIR}/{safe_endpoint_name}_labels.jsonl"
###########################################
import asyncio
import json
import math
import base64
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import csv
import signal
import time
from urllib.parse import urlparse
from atproto import AsyncFirehoseSubscribeLabelsClient, firehose_models, models, parse_subscribe_labels_message

# Directories for storing logs and label streams
LOG_DIR = "log"
STREAMS_DIR = "label_streams"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STREAMS_DIR, exist_ok=True)

# Log file paths
LOG_FILE = os.path.join(LOG_DIR, "label_streamer_stdout.log")  # Ensure logs are in the correct directory

# CSV file containing service endpoints
CSV_FILE = "labelers_service_endpoints.csv"

# Configure Rotating File Handler (Ensures logs do not grow indefinitely)
stdout_handler = RotatingFileHandler(
    LOG_FILE,                # Store logs in the correct log directory
    maxBytes=20 * 1024 * 1024,  # 20MB per log file
    backupCount=10,          # Keep last 10 logs
    encoding="utf-8"
)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Log level
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[stdout_handler],  # Log only to file, not console
)

# Create Logger
logger = logging.getLogger("label_stream_logger")

# Redirect sys.stderr to logger
class StderrLogger:
    def write(self, message):
        if message.strip():  # Avoid logging empty messages
            logger.error(message.strip())

    def flush(self):
        pass  # Required for compatibility

# Redirect stderr to log file (for capturing errors)
sys.stderr = StderrLogger()

# Global task list and file handle cache
running_tasks = []
file_handles = {}  # Store file handles for writing
semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent streamers

# Track file update timestamps
file_update_times = {}
shutdown_event = asyncio.Event()
MAX_BATCH_DURATION = 30



def load_endpoints_from_csv(csv_file):
    """Load and extract service endpoint without the protocol part."""
    endpoints = set()
    if not os.path.exists(csv_file):
        logger.error(f"CSV file '{csv_file}' not found!")
        return endpoints

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            service_endpoint = row.get("service_endpoint", "").strip().rstrip("/")
            if service_endpoint:
                if "://" not in service_endpoint:
                    service_endpoint = "https://" + service_endpoint
                
                parsed_url = urlparse(service_endpoint)
                endpoint_without_protocol = f"{parsed_url.netloc}{parsed_url.path}"
                if endpoint_without_protocol in endpoints:
                    logger.warning(f"endpoint appeared more than once: {row}")
                if not endpoint_without_protocol or endpoint_without_protocol.startswith("localhost"):
                    logger.warning(f"skipping {row}")
                    continue
                endpoints.add(endpoint_without_protocol)
    logger.info(f"Total # of endpoints: {len(endpoints)}")
    return sorted(list(endpoints))  # sorted list for batch processing

def convert_to_json_serializable(obj):
    """Convert objects to JSON serializable format."""
    if isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    elif hasattr(obj, "dict"):
        return convert_to_json_serializable(obj.model_dump())
    else:
        return obj

def get_last_sequence_from_json(endpoint):
    """Retrieve the last sequence number from the most recent JSON log file for an endpoint."""
    safe_endpoint_name = endpoint.replace("/", "_").replace(":", "_")
    output_filename = os.path.join(STREAMS_DIR, f"{safe_endpoint_name}_labels.jsonl")
    
    if not os.path.exists(output_filename):
        return 0  # Default last seq to 0 if no label stream file exists
    
    try:
        with open(output_filename, "rb") as f:
            f.seek(0, os.SEEK_END)  # Move to end of file
            pos = f.tell()
            buffer = b""
            chunk_size = 4096  # Start with 4KB chunks
            while pos > 0:
                pos = max(0, pos - chunk_size)
                f.seek(pos)
                chunk = f.read(chunk_size) + buffer  # Prepend previous buffer
                lines = chunk.split(b"\n")
                
                for line in reversed(lines):
                    try:
                        last_json = json.loads(line.decode("utf-8"))
                        return int(last_json.get("seq", 0))
                    except json.JSONDecodeError:
                        buffer = line  # Store buffer in case JSON is split
                        continue  # Keep scanning upwards if decoding fails
                    except Exception as e:
                        logger.error(f"Could not decode last line from {output_filename}: {e}, line: {line}")
                        continue
    except Exception as e:
        logger.error(f"Could not retrieve last sequence from {output_filename}: {e}")
    
    return 0  # Default to 0 in case of error

async def on_message_handler(endpoint, message: firehose_models.MessageFrame):
    """Handle incoming messages from Firehose."""
    try:
        labels_message = parse_subscribe_labels_message(message)
        if not isinstance(labels_message, models.ComAtprotoLabelSubscribeLabels.Labels):
            return

        safe_endpoint_name = endpoint.replace("/", "_").replace(":", "_")
        output_filename = os.path.join(STREAMS_DIR, f"{safe_endpoint_name}_labels.jsonl")

        labels_message_json = convert_to_json_serializable(labels_message)

        if safe_endpoint_name not in file_handles:
            file_handles[safe_endpoint_name] = open(output_filename, "a", encoding="utf-8")

        file_handles[safe_endpoint_name].write(json.dumps(labels_message_json) + "\n")
        file_update_times[safe_endpoint_name] = time.time()
    except Exception as e:
        logger.error(f"Error processing message from {endpoint}: {e}")

async def stream_from_endpoint(endpoint):
    """Start streaming from a given Firehose endpoint with controlled concurrency."""
    async with semaphore:
        client = AsyncFirehoseSubscribeLabelsClient(base_uri=f"wss://{endpoint}/xrpc")
        last_seq = get_last_sequence_from_json(endpoint)
        client.update_params(models.ComAtprotoLabelSubscribeLabels.Params(cursor=last_seq))
        logger.info(f"Starting stream for {endpoint} from seq: {last_seq}")

        try:
            await client.start(
                lambda msg: on_message_handler(endpoint, msg),
                lambda err: logger.error(f"Error in {endpoint}: {err}")
            )
        except asyncio.CancelledError:
            logger.info(f"Streaming for {endpoint} stopped.")
        except Exception as e:
            logger.error(f"Streamer crashed on {endpoint}: {e}")

async def shutdown():
    """Handle graceful shutdown on exit signals."""
    logger.info("Shutdown signal received. Cancelling tasks and closing files...")
    
    shutdown_event.set()  # Signal `main()` to stop processing
    
    # Cancel all running tasks
    for task in running_tasks:
        task.cancel()

    # Ensure tasks complete cancellation
    await asyncio.gather(*running_tasks, return_exceptions=True)

    # Close file handles
    for handle in file_handles.values():
        handle.close()
    file_handles.clear()

    logger.info("Shutdown complete.")

async def main():
    """Start multiple Firehose clients asynchronously with controlled concurrency in rounds."""
    endpoints = load_endpoints_from_csv(CSV_FILE)

    if not endpoints:
        logger.error("No valid endpoints found! Exiting.")
        return

    batch_size = 10
    total_num_of_batches = math.ceil(len(endpoints) / batch_size)
    num_digits = len(str(total_num_of_batches))
    while not shutdown_event.is_set():
        for i in range(0, len(endpoints), batch_size):
            if shutdown_event.is_set():  # Break early if shutdown is requested
                logger.info("Shutdown requested, stopping batch processing.")
                break

            batch = endpoints[i:i + batch_size]
            logger.info(f"[{(i // batch_size + 1):0{num_digits}d}/{total_num_of_batches}] Starting batch : {batch}")
            tasks = [asyncio.create_task(stream_from_endpoint(endpoint)) for endpoint in batch]
            running_tasks.extend(tasks)

            batch_start_time = time.time()
            while time.time() - batch_start_time < MAX_BATCH_DURATION:
                if shutdown_event.is_set():  # Stop waiting early if shutdown is requested
                    logger.info("Shutdown requested, stopping early.")
                    break
                await asyncio.sleep(max(MAX_BATCH_DURATION//10,2))  # Check 10 times / 2 secs for each batch

                # Check if any endpoint in the batch has received updates in the last second
                updates_per_sec = sum(
                    1 for ep in batch
                    if file_update_times.get(ep.replace("/", "_").replace(":", "_"), 0) > time.time() - 1
                )

                if updates_per_sec == 0:  # If no updates, switch batches early
                    logger.info("No updates detected, switching batches early.")
                    break

            if shutdown_event.is_set():
                break  # Stop all processing

            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        if shutdown_event.is_set():
            logger.info("Exiting main loop due to shutdown.")
            break



if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Register signal handlers to trigger shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    try:
        loop.run_until_complete(main())  # Runs main until shutdown_event is set
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Shutdown signal received. Exiting gracefully.")
    finally:
        loop.run_until_complete(shutdown())  # Ensure shutdown tasks complete
        loop.close()
