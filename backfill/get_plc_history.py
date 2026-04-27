#!/usr/bin/env python3
###########################################
# Collect ALL DID operations since day 1
# see docs on https://web.plc.directory/api/redoc#operation/Export
# input : None
# output: all plc operations saved to data_file, each line is a json object
#
# Fixes included:
# - Resume recovery no longer loads the whole JSONL file into memory.
# - Writes each response batch in one file write instead of flushing every line.
###########################################

import requests
import json
import os
import time
import random
import logging

class PLCDataExporter:
    def __init__(
        self,
        data_file="plc_data.jsonl",
        timestamp_file="last_timestamp.txt",
        log_dir="log/",
        log_file="plc_export.log",
    ):
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
        with open(self.timestamp_file, "w", encoding="utf-8") as file:
            file.write(timestamp)

    def _read_last_timestamp(self):
        """
        Resume from last_timestamp.txt if present.

        If timestamp_file is missing, recover from the last JSON object in data_file
        without reading the whole file into memory.
        """
        try:
            with open(self.timestamp_file, "r", encoding="utf-8") as file:
                timestamp = file.read().strip()
                if timestamp:
                    return timestamp
        except FileNotFoundError:
            pass

        return self._read_last_created_at_from_jsonl()

    def _read_last_created_at_from_jsonl(self):
        """
        Read the final non-empty line of a potentially huge JSONL file in bounded memory.
        """
        try:
            with open(self.data_file, "rb") as file:
                last_line = self._read_last_nonempty_line(file)
        except FileNotFoundError:
            return ""

        if not last_line:
            return ""

        try:
            last_object = json.loads(last_line.decode("utf-8"))
            return last_object.get("createdAt", "")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._log(
                f"Failed to parse last JSONL record while recovering timestamp: {e}",
                level="error",
            )
            return ""

    @staticmethod
    def _read_last_nonempty_line(file_obj, block_size=8192):
        """
        Return the last non-empty line from a binary file object.

        This scans backward in chunks, so memory usage stays bounded even for
        very large JSONL files.
        """
        file_obj.seek(0, os.SEEK_END)
        position = file_obj.tell()

        if position == 0:
            return b""

        buffer = b""

        while position > 0:
            read_size = min(block_size, position)
            position -= read_size
            file_obj.seek(position)
            chunk = file_obj.read(read_size)
            buffer = chunk + buffer

            lines = buffer.splitlines()
            if len(lines) > 1:
                for line in reversed(lines):
                    line = line.strip()
                    if line:
                        return line

        for line in reversed(buffer.splitlines()):
            line = line.strip()
            if line:
                return line

        return b""

    def _calculate_backoff(self, attempt):
        base = self.base_delay * (2 ** attempt)
        delay = min(base, self.max_delay)
        jitter = random.uniform(0.5, 1.5)
        return int(delay * jitter)

    def _write_batch(self, lines):
        """
        Write a whole response batch in one append operation.

        This is much faster than write+flush per line and still keeps memory
        bounded to the current response batch.
        """
        if not lines:
            return

        batch = "\n".join(lines) + "\n"
        with open(self.data_file, "a", encoding="utf-8") as file:
            file.write(batch)

    def fetch_all(self):
        request_count = 0
        attempt = 0
        last_created_at = self._read_last_timestamp()

        try:
            while self.running:
                params = dict(self.params)

                if last_created_at:
                    params["after"] = last_created_at
                    self._log(
                        f"{request_count}-th request, current batch starting timestamp: {last_created_at}"
                    )

                try:
                    response = requests.get(self.url, params=params, timeout=30)
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
                        self._write_batch(data)

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
