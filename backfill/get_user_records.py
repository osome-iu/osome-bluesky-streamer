#################################################
# Find all PDSs and download all Repo records to files
# input: plc_data_file, containing lines of json objects each is a plc operation
# output: 1 file per user/DID containing lines of records that are either json records or dict representations of MST nodes.
#################################################

import os
import json
import time
import sys
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

from atproto import Client, CAR, DidDocument
from atproto_client.models.com.atproto.sync import list_repos, get_repo


class PDSRepoDownloader:
    def __init__(self, plc_data_file, pds_did_list_file, credentials_file, log_folder='log'):
        self.plc_data_file = plc_data_file
        self.pds_did_list_file = pds_did_list_file
        self.credentials_file = credentials_file
        self.pds_user_records_folder = 'all_records_pds_user/'
        self.failed_pds_file = "failed_pds_log.json"
        self.failed_pds = self.load_failed_pds()
        self.pds_repo_data = {}

        self.username, self.password = self.load_credentials()

        os.makedirs(log_folder, exist_ok=True)
        self.logger = self.setup_logger(log_folder)
        sys.stderr = open(os.path.join(log_folder, 'user_records_stderr.log'), 'a')

    def load_credentials(self):
        if not os.path.exists(self.credentials_file):
            msg = f"Credentials file '{self.credentials_file}' not found."
            print(msg)
            self.logger.critical(msg)
            sys.exit(1)
        with open(self.credentials_file, 'r') as f:
            config = json.load(f)
        return config['username'], config['password']

    def setup_logger(self, log_folder):
        logger = logging.getLogger('PDSRepoDownloader')
        logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(
            os.path.join(log_folder, 'user_records_stdout.log'),
            maxBytes=100 * 1024 * 1024,
            backupCount=50
        )
        formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def wait_until_reset(self, reset_timestamp):
        reset_time = datetime.fromtimestamp(int(reset_timestamp))
        sleep_time = (reset_time - datetime.now()).total_seconds()
        if sleep_time > 0:
            self.logger.info(f"Rate limit exceeded. Sleeping until {reset_time} (local time)")
            time.sleep(sleep_time)

    def load_failed_pds(self):
        if os.path.exists(self.failed_pds_file):
            with open(self.failed_pds_file, "r") as f:
                return set(json.load(f))
        return set()

    def save_failed_pds(self):
        with open(self.failed_pds_file, "w") as f:
            json.dump(list(self.failed_pds), f, indent=2)

    def save_pds_repo_data(self):
        with open(self.pds_did_list_file, "w") as f:
            json.dump(self.pds_repo_data, f, indent=4)

    def get_pds_repo_dict(self):
        self.logger.info(f"Loading {self.plc_data_file} to find all PDSs")
        user_pdss = set()

        try:
            with open(self.plc_data_file) as plc_data:
                for line in plc_data:
                    try:
                        item = json.loads(line)
                        cur_pds = item["operation"].get("services", {}).get("atproto_pds", {}).get("endpoint") or item["operation"].get("service")
                        if cur_pds:
                            user_pdss.add(cur_pds)
                    except Exception as e:
                        self.logger.error(f"{e}, Failed to parse line for {line.strip()}")
        except Exception as e:
            self.logger.error(f"{e}, Failed to get PDSs from file {self.plc_data_file}")

        self.logger.info(f"Found {len(user_pdss)} PDSs: {list(user_pdss)}")
        with open("all_PDSes.json", "w") as f:
            json.dump(list(user_pdss), f)

        for pds in user_pdss:
            if pds in self.failed_pds:
                self.logger.info(f"Skipping previously failed PDS: {pds}")
                continue
            self.logger.info(f"Collecting repos from PDS: {pds}")
            try:
                client = Client(base_url=f'{pds}/xrpc')
                client.login(self.username, self.password)
            except Exception as e:
                self.logger.warning(f"Cannot connect to '{pds}', error: {e}")
                self.failed_pds.add(pds)
                self.save_failed_pds()
                continue

            cursor, success = None, False
            while not success:
                try:
                    args = {'cursor': cursor, 'limit': 1000} if cursor else {}
                    data = client.com.atproto.sync.list_repos(args)
                    repos = data.repos
                    cursor = data.cursor
                    self.pds_repo_data.setdefault(pds, []).extend([repo.model_dump() for repo in repos])
                    if not cursor:
                        success = True
                except Exception as e:
                    error_message = str(e)
                    if "RateLimitExceeded" in error_message:
                        self.logger.warning(f"Rate-Limit Reached for {pds}, waiting...")
                        reset_timestamp = json.loads(e.response.headers['ratelimit-reset'])
                        self.wait_until_reset(reset_timestamp)
                    else:
                        self.logger.error(f"Failed to collect from {pds}, exception: {e}")
                        self.failed_pds.add(pds)
                        self.save_failed_pds()
                        break

            self.save_pds_repo_data()
            self.logger.info(f"Saved repo list for PDS: {pds}")

        self.logger.info(f"Collected from {len(self.pds_repo_data)} PDSs, total users: {sum(len(v) for v in self.pds_repo_data.values())}")

    def save_repo_records(self):
        self.logger.info("Starting records collection...")

        with open(self.pds_did_list_file, "r") as f:
            pdss = json.load(f)

        os.makedirs(self.pds_user_records_folder, exist_ok=True)
        existing_files = set(os.listdir(self.pds_user_records_folder))
        self.logger.info(f"Skipping {len(existing_files)} existing DIDs")

        for pds, repos in list(pdss.items()):
            pdss[pds] = [repo for repo in repos if f"{repo['did'].replace(':', '-')}.txt" not in existing_files]
            if not pdss[pds]:
                del pdss[pds]

        self.logger.info(f"Remaining PDSs to collect from: {len(pdss)}")

        for pds, repos in pdss.items():
            self.logger.info(f"Collecting from {pds} ({len(repos)} repos)...")
            client = Client(base_url=f'{pds}/xrpc')
            client.login(self.username, self.password)
            for repo in repos:
                did = repo['did']
                filename = f"{did.replace(':', '-')}.txt"
                try:
                    repo_bytes = client.com.atproto.sync.get_repo({'did': did})
                    car_file = CAR.from_bytes(repo_bytes)
                    with open(os.path.join(self.pds_user_records_folder, filename), 'w+') as f:
                        for cid, block in car_file.blocks.items():
                            f.write(f"{cid}, {block}\n")
                    self.logger.info(f"Saved repo for {did} from {pds}")
                except Exception as e:
                    self.handle_repo_exception(e, client, did, pds, filename)

    def handle_repo_exception(self, e, client, did, pds, filename):
        error_message = str(e)
        if "InvalidRequest" in error_message:
            try:
                response = client.com.atproto.repo.describe_repo({'repo': did})
                new_pds = DidDocument.from_dict(response.did_doc).get_pds_endpoint()
                new_client = Client(base_url=f'{new_pds}/xrpc')
                repo_bytes = new_client.com.atproto.sync.get_repo({'did': did})
                car_file = CAR.from_bytes(repo_bytes)
                with open(os.path.join(self.pds_user_records_folder, filename), 'w+') as f:
                    for cid, block in car_file.blocks.items():
                        f.write(f"{cid}, {block}\n")
                self.logger.info(f"Recovered and saved repo for {did} from fallback PDS {new_pds}")
            except Exception as e2:
                self.logger.error(f"Failed again for {did}: {e2}")
        elif "RateLimitExceeded" in error_message:
            reset_timestamp = json.loads(e.response.headers['ratelimit-reset'])
            self.wait_until_reset(reset_timestamp)
        else:
            self.logger.error(f"Unhandled error for {did}: {e}")


if __name__ == '__main__':
    downloader = PDSRepoDownloader(
        plc_data_file='plc_data.jsonl',
        pds_did_list_file='pds_did_list.json',
        credentials_file='bsky_login_info.json'
    )
    
    try:
        downloader.get_pds_repo_dict()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving state and exiting gracefully...")
        if 'downloader' in locals():
            downloader.save_failed_pds()
            downloader.save_pds_repo_data()
        sys.exit(0)

    downloader.save_repo_records()