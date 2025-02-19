###########################################
# Collect ALL DID operations since day 1
# see docs on https://web.plc.directory/api/redoc#operation/Export
# input : None
# output: all plc operations saved to data_file, each line is a json object
###########################################
import requests
import json
import os
import time
import logging

# Define directory for logs
log_dir = "log/"
os.makedirs(log_dir, exist_ok=True)

data_file = 'plc_data.json'
timestamp_file = 'last_timestamp.txt'
log_file = os.path.join(log_dir, 'plc_log.txt')

# Set up logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

url = "https://plc.directory/export"
params = {
    "count": 1000,
}

# Function to save the last timestamp to a file
def save_last_timestamp(last_timestamp):
    with open(timestamp_file, 'w') as file:
        file.write(last_timestamp)

# Function to read the last timestamp from a file
def read_last_timestamp():
    try:
        with open(timestamp_file, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        try:
            with open(data_file, 'r') as json_file:
                lines = json_file.readlines()
                if lines:
                    last_line = lines[-1]
                    last_object = json.loads(last_line)
                    return last_object.get("createdAt", "")
        except FileNotFoundError:
            pass
    return ""

# Function to log messages
def log_message(message, level='info'):
    if level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)

# Get the last timestamp from the file
last_timestamp = read_last_timestamp()

n = 0

while True:
    if last_timestamp:
        params["after"] = last_timestamp
        log_message(f"{n}-th request, with: {last_timestamp}")

    response = requests.get(url, params=params)

    if response.status_code == 200:
        n += 1
        # Handle successful response
        data = response.text.splitlines()
        if data:
            with open(data_file, 'a+') as json_file:
                for line in data:
                    json_file.write(line + '\n')
            last_timestamp = json.loads(data[-1])["createdAt"]
            save_last_timestamp(last_timestamp)
        else:
            log_message("No data received.", level='warning')
            break
    elif response.status_code == 429:
        # Handle rate limiting by waiting for some time
        retry_after = int(response.headers.get('Retry-After', 30))
        log_message(f"Rate limited. Retrying after {retry_after} seconds...", level='warning')
        time.sleep(retry_after)
    else:
        log_message(f"Error: {response.status_code}", level='error')
        break
