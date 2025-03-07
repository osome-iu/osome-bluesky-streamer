###########################################
# Stream label events from labelers' endpoints
# Docs :
#   - https://atproto.blue/en/latest/atproto_firehose/index.html#atproto_firehose.AsyncFirehoseSubscribeLabelsClient
#   - https://docs.bsky.app/docs/advanced-guides/moderation#labels
#
# input : "labelers_service_endpoints.csv" list of labeler endpoints
# output : label events 1 file per endpoint to "{STREAMS_DIR}/{safe_endpoint_name}_labels.json"
###########################################

import asyncio
import json
import base64
import logging
import os
import csv
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone
from atproto import AsyncFirehoseSubscribeLabelsClient, firehose_models, models, parse_subscribe_labels_message

# Directories for storing logs and label streams
LOG_DIR = "log"
STREAMS_DIR = "label_streams"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STREAMS_DIR, exist_ok=True)

# CSV file containing service endpoints
CSV_FILE = "labelers_service_endpoints.csv"

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "label_streamer_stdout.log")),
        # logging.StreamHandler()
    ],
)
logger = logging.getLogger("label_stream_logger")


def load_endpoints_from_csv(csv_file):
    """Load and extract service endpoint without the protocol part."""
    endpoints = set()
    if not os.path.exists(csv_file):
        logger.error(f"CSV file '{csv_file}' not found!")
        return endpoints

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            service_endpoint = row.get("service_endpoint", "").strip()
            if service_endpoint:
                # Ensure the URL has a scheme; if missing, prepend '//' for correct parsing
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
    logger.info(f"total # of endpoints: {len(endpoints)}")
    return endpoints



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
    json_files = [f for f in os.listdir(STREAMS_DIR) if f.endswith(f"labels_{safe_endpoint_name}.json")]

    if not json_files:
        return 0  # Default last seq to 0 if no label stream file exists

    # Find the most recent JSON file
    latest_file = max(json_files, key=lambda x: os.path.getmtime(os.path.join(STREAMS_DIR, x)))

    # Read the last line to extract sequence number
    try:
        with open(os.path.join(STREAMS_DIR, latest_file), "r", encoding="utf-8") as f:
            lines = f.readlines()
            if lines:
                last_line = json.loads(lines[-1])  # Load last JSON entry
                return int(last_line.get("seq", 0))
    except Exception as e:
        logger.warning(f"Could not retrieve last sequence from {latest_file}: {e}")
    
    return 0  # Default to 0 in case of error


async def on_message_handler(endpoint, message: firehose_models.MessageFrame):
    """Handle incoming messages from Firehose."""
    try:
        labels_message = parse_subscribe_labels_message(message)
        if not isinstance(labels_message, models.ComAtprotoLabelSubscribeLabels.Labels):
            return

        # Save labels message as JSON
        safe_endpoint_name = endpoint.replace("/", "_").replace(":", "_")
        output_filename = os.path.join(STREAMS_DIR, f"{safe_endpoint_name}_labels.json")

        labels_message_json = convert_to_json_serializable(labels_message)

        with open(output_filename, "a", encoding="utf-8") as json_file:
            json_file.write(json.dumps(labels_message_json) + "\n")

    except Exception as e:
        logger.error(f"Error processing message from {endpoint}: {e}")


async def stream_from_endpoint(endpoint):
    """Start streaming from a given Firehose endpoint."""
    client = AsyncFirehoseSubscribeLabelsClient(base_uri=f"wss://{endpoint}/xrpc")

    # Resume from last sequence if available, otherwise default to 0
    last_seq = get_last_sequence_from_json(endpoint)
    client.update_params(models.ComAtprotoLabelSubscribeLabels.Params(cursor=last_seq))
    logger.info(f"Starting stream for {endpoint} from seq: {last_seq}")

    try:
        await client.start(
            lambda msg: on_message_handler(endpoint, msg),
            lambda err: logger.error(f"Error in {endpoint}: {err}")
        )
    except Exception as e:
        logger.critical(f"Streamer crashed on {endpoint}: {e}")


async def main():
    """Start multiple Firehose clients asynchronously."""
    endpoints = load_endpoints_from_csv(CSV_FILE)
    
    if not endpoints:
        logger.error("No valid endpoints found! Exiting.")
        return

    tasks = [stream_from_endpoint(endpoint) for endpoint in endpoints]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
