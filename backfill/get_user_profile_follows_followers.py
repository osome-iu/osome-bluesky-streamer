#############################################################
# For colleting user profile, follows, followers info - defaults to 'pds_did_list.json'
# input: a file containing a json dictionary object listing all PDSs and corresponding DIDs
# output: 3 files per did stored in folder profile_follows_follers
#############################################################

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from atproto import Client


class BskyDataCollector:
    def __init__(self, pds_did_list_file, credentials_file, output_dir='profile_follows_followers', log_dir='log'):
        self.pds_did_list_file = pds_did_list_file
        self.credentials_file = credentials_file
        self.output_dir = output_dir
        self.log_dir = log_dir
        self.client = Client()
        self.request_count = 0
        self.start_time = datetime.now()
        self.setup_logging()
        self.username, self.password = self.load_credentials()
        self.client.login(self.username, self.password)

    def setup_logging(self):
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s]: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(os.path.join(self.log_dir, 'profiles_follows_followers_stdout.log')),
            ]
        )
        sys.stderr = open(os.path.join(self.log_dir, 'profiles_follows_followers_stderr.log'), 'a')
        self.logger = logging.getLogger('bsky_data_collector')

    def load_credentials(self):
        with open(self.credentials_file, 'r') as f:
            config = json.load(f)
        return config['username'], config['password']

    def load_pds_did_list(self):
        with open(self.pds_did_list_file, 'r') as f:
            return json.load(f)

    def ensure_output_dir(self):
        os.makedirs(self.output_dir, exist_ok=True)

    def clean_existing_dids(self, pdss):
        did_files = os.listdir(self.output_dir)
        grouped = {}
        for file in did_files:
            parts = file.split("_")
            if len(parts) < 2:
                continue
            did = parts[0]
            grouped.setdefault(did, set()).add(parts[1].split(".")[0])

        complete_dids = {did for did, types in grouped.items() if {'profile', 'follows', 'followers'}.issubset(types)}

        self.logger.info(f"Skipping {len(complete_dids)} DIDs already fully processed...")
        for pds in list(pdss):
            pdss[pds] = [repo for repo in pdss[pds] if repo['did'].replace(':', '-') not in complete_dids]
            if not pdss[pds]:
                del pdss[pds]
        return pdss

    def wait_if_rate_limited(self):
        if self.request_count >= 3000:
            time_passed = datetime.now() - self.start_time
            if time_passed < timedelta(minutes=5):
                wait_time = (timedelta(minutes=5) - time_passed).total_seconds()
                self.logger.info(f"Rate limit reached, pausing for {wait_time:.2f} seconds.")
                time.sleep(wait_time)
            self.request_count = 0
            self.start_time = datetime.now()

    def fetch_paginated_data(self, fetch_function, actor):
        all_data = []
        cursor = None
        while True:
            try:
                response = fetch_function(actor=actor, limit=100, cursor=cursor) if cursor else fetch_function(actor=actor, limit=100)
                data = getattr(response, 'follows', getattr(response, 'followers', []))
                cursor = response.cursor
                for entry in data:
                    d = entry.model_dump()
                    d.pop('viewer', None)
                    all_data.append(d)
                if not cursor:
                    break
            except Exception as e:
                self.logger.error(f"Error fetching data for {actor}: {e}")
                break
        return all_data

    def process_did(self, did):
        did_str = did.replace(':', '-')
        self.wait_if_rate_limited()
        try:
            self.logger.info(f"Processing {did}")
            profile = self.client.app.bsky.actor.get_profile({'actor': did}).model_dump()
            profile.pop('viewer', None)
            self.save_data(did_str, "profile", profile)

            follows = self.fetch_paginated_data(self.client.get_follows, did)
            self.save_data(did_str, "follows", follows)

            followers = self.fetch_paginated_data(self.client.get_followers, did)
            self.save_data(did_str, "followers", followers)

            self.request_count += 1
        except Exception as e:
            self.handle_exception(e, did)

    def save_data(self, did_str, data_type, data):
        filename = os.path.join(self.output_dir, f"{did_str}_{data_type}.json")
        try:
            with open(filename + ".tmp", 'w') as f:
                json.dump(data, f)
            os.replace(filename + ".tmp", filename)
            self.logger.info(f"Saved {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save {filename}: {e}")

    def handle_exception(self, e, did):
        self.logger.error(f"Exception for {did}: {e}")
        if 'Profile not found' in str(e):
            self.logger.warning(f"Profile not found for {did}")
        elif '3000' in str(e):
            self.wait_if_rate_limited()
        else:
            self.logger.error("Unhandled error.")

    def run(self):
        self.ensure_output_dir()
        pdss = self.load_pds_did_list()
        pdss = self.clean_existing_dids(pdss)
        self.logger.info(f"Working on {sum(len(repos) for repos in pdss.values())} DIDs...")
        for repos in pdss.values():
            for repo in repos:
                self.process_did(repo['did'])


if __name__ == "__main__":
    try:
        collector = BskyDataCollector(
            pds_did_list_file='pds_did_list.json',
            credentials_file='bsky_login_info.json'
        )
        collector.run()
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user (Ctrl+C). Exiting gracefully...")
        logging.getLogger('bsky_data_collector').warning("Execution interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        error_msg = f"[CRITICAL] Unexpected error occurred: {e}"
        print(f"\n{error_msg}")
        logging.getLogger('bsky_data_collector').exception(error_msg)
        sys.exit(1)
