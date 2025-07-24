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
import random
import logging

class PLCDataExporter:
    def __init__(self, data_file='plc_data.jsonl', timestamp_file='last_timestamp.txt', log_dir='log/', log_file='plc_export.log'):
        self.url = "https://plc.directory/export"
        self.params = {"count": 1000}
        self.data_file = data_file
        self.timestamp_file = timestamp_file
        self.log_dir = log_dir
        self.log_file = os.path.join(log_dir, log_file)
        os.makedirs(log_dir, exist_ok=True)
        self._setup_logging()
        self.running = True

        # Backoff config
        self.base_delay = 5
        self.max_delay = 300

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(self.log_file),
            ]
        )
        self.logger = logging.getLogger()

    def _log(self, message, level='info'):
        getattr(self.logger, level)(message)

    def _save_last_timestamp(self, timestamp):
        with open(self.timestamp_file, 'w') as file:
            file.write(timestamp)

    def _read_last_timestamp(self):
        try:
            with open(self.timestamp_file, 'r') as file:
                return file.read().strip()
        except FileNotFoundError:
            try:
                with open(self.data_file, 'r') as json_file:
                    lines = json_file.readlines()
                    if lines:
                        last_object = json.loads(lines[-1])
                        return last_object.get("createdAt", "")
            except FileNotFoundError:
                pass
        return ""

    def _calculate_backoff(self, attempt):
        base = self.base_delay * (2 ** attempt)
        delay = min(base, self.max_delay)
        jitter = random.uniform(0.5, 1.5)
        return int(delay * jitter)

    def fetch_all(self):
        request_count = 0
        attempt = 0
        last_created_at = self._read_last_timestamp()

        try:
            while self.running:
                if last_created_at:
                    self.params["after"] = last_created_at
                    self._log(f"{request_count}-th request, current batch starting timestamp: {last_created_at}")

                try:
                    response = requests.get(self.url, params=self.params, timeout=30)
                except requests.RequestException as e:
                    delay = self._calculate_backoff(attempt)
                    self._log(f"Network error: {e}. Retrying in {delay}s...", level='warning')
                    time.sleep(delay)
                    attempt += 1
                    continue

                if response.status_code == 200:
                    attempt = 0
                    request_count += 1
                    data = response.text.splitlines()

                    if data:
                        with open(self.data_file, 'a') as file:
                            for line in data:
                                file.write(line + '\n')
                                file.flush()

                        try:
                            last_record = json.loads(data[-1])
                            last_created_at = last_record.get("createdAt", "")
                            if last_created_at:
                                self._save_last_timestamp(last_created_at)
                        except json.JSONDecodeError:
                            self._log("Failed to parse last record for timestamp.", level='error')
                    else:
                        self._log("No more data available.", level='info')
                        print("Export complete.")
                        break

                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    self._log(f"Rate limited. Retrying after {retry_after}s...", level='warning')
                    time.sleep(retry_after)

                else:
                    delay = self._calculate_backoff(attempt)
                    self._log(f"HTTP {response.status_code}. Retrying in {delay}s...", level='error')
                    time.sleep(delay)
                    attempt += 1

        except KeyboardInterrupt:
            self._log("Interrupted by user. Exiting...", level='warning')
            print("Export interrupted.")
        except Exception as e:
            self._log(f"Unexpected exception: {e}", level='error')
            print("Export failed due to unexpected error.")
        finally:
            self._log("Shutdown complete.")

if __name__ == "__main__":
    exporter = PLCDataExporter()
    exporter.fetch_all()
