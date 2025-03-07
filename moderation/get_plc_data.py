###########################################
# Collect all relevant DID operations since day 1
# see docs on https://web.plc.directory/api/redoc#operation/Export
# input : None
# output: all plc operations saved to data_file, each line is a json object
###########################################
import requests
import json
import logging
import os
from datetime import datetime, timedelta
import time

# Setup logging
log_dir = "log"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "plc_data.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

url = "https://plc.directory/export"
params = {"count": 1000}
data_file = "plc_data.jsonl"

def save_last_timestamp(last_timestamp):
    """Save the last timestamp to a file."""
    with open("last_timestamp.txt", "w") as file:
        file.write(last_timestamp)


def read_last_timestamp():
    """Read the last timestamp from a file, or infer from data file if missing."""
    try:
        with open("last_timestamp.txt", "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        try:
            with open(data_file, "r") as json_file:
                lines = json_file.readlines()
                if lines:
                    last_object = json.loads(lines[-1])
                    return last_object.get("createdAt", "")
        except FileNotFoundError:
            pass
    return ""

# Get the last timestamp
last_timestamp = read_last_timestamp()
n = 0

while True:
    if last_timestamp:
        params["after"] = last_timestamp
        logging.info(f"{n}-th request, with last timestamp: {last_timestamp}")

    response = requests.get(url, params=params)

    if response.status_code == 200:
        n += 1
        data = response.text.splitlines()
        if data:
            with open(data_file, "a+") as json_file:
                for line in data:
                    if "label" in line:
                        json_file.write(line + "\n")
            
            last_timestamp = json.loads(data[-1])["createdAt"]
            save_last_timestamp(last_timestamp)
            
            last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            current_time = datetime.utcnow()
            if current_time - last_timestamp_dt <= timedelta(minutes=1):
                logging.info("Last timestamp is within 1 minute of current time. Exiting loop.")
                break
        else:
            logging.info("No data received.")
            break
    elif response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 30))
        logging.warning(f"Rate limited. Retrying after {retry_after} seconds...")
        time.sleep(retry_after)
    else:
        logging.error(f"Error: {response.status_code}")
        break
