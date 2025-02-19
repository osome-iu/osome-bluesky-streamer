#################################################
# Find all PDSs and download all Repo records to files
# input: plc_data_file, containing lines of json objects each is a plc operation
# output: 1 file per user/DID containing lines of records that are either json records or dict representations of MST nodes.
#################################################
from atproto import Client, CAR, DidDocument,FirehoseSubscribeReposClient, exceptions
from atproto_client.models.app.bsky.feed import get_author_feed
from atproto_client.models.com.atproto.sync import list_repos, get_repo
import requests
import time
from datetime import datetime, timezone
from atproto_client.models.utils import get_or_create, get_model_as_json
import json
import os
import logging
from logging.handlers import RotatingFileHandler
import sys


# Load credentials from file
def load_credentials(file):
    with open(file, 'r') as f:
        config = json.load(f)
    return config['username'], config['password']


# get all PDSs from the the plc data scrap file, get all Repos currently active on each PDS
def get_pds_repo_dict(plc_data_file):
    logger.info(f"Loading {plc_data_file} to find all PDSs")
    result = {}
    user_pdss = set()

    # find all PDSs
    try:
        with open(plc_data_file) as plc_data:
            for line in plc_data:
                try:
                    item = json.loads(line)
                    cur_pds = item["operation"]["services"]["atproto_pds"]["endpoint"]
                    user_pdss.add(cur_pds)
                except:
                    try:
                        cur_pds = item["operation"]["service"]
                        user_pdss.add(cur_pds)
                    except Exception as e:
                        logger.error(f"{e}, Failed to parse line for {line.strip()}")
    except Exception as e:
        logger.error(f"{e}, Failed to get PDSs from file {plc_data_file}")

    logger.info(f"Found {len(user_pdss)} PDSs, list: {list(user_pdss)}")

    with open("all_PDSes.txt", "w") as user_pdss_file:
        json.dump(list(user_pdss), user_pdss_file)

   # get all repos/dids on each PDS
    logger.info(f"Working with {len(user_pdss)} PDSs")
    for pds in user_pdss:
        logger.info(f"Collecting repos from PDS: {pds}")
        try:
            client = Client(base_url=f'{pds}/xrpc')
            client.login(username, password)
        except Exception as e:
            logger.warning(f"Cannot connect to '{pds}', error: {e}")
            continue
        cursor = None
        success = False
        while not success:
            try:
                if not cursor:
                    data = client.com.atproto.sync.list_repos()
                else:
                    data = client.com.atproto.sync.list_repos({'cursor':cursor, 'limit':1000})
                repos = data.repos
                cursor = data.cursor
                logger.debug(cursor)
                logger.debug(len(repos))
                logger.info(f"Got {len(repos)} more Repos for {pds}")
                repos_json = [repo.model_dump() for repo in repos]
                result.setdefault(pds, []).extend(repos_json)
                if not cursor: success = True
            except Exception as e:
                error_message = str(e)
                logger.debug(f"error: {error_message}")
                if"RateLimitExceeded" in error_message: # rate-limit reached
                    logger.warning(f"Rate-Limit Reached, wait till reset time...")
                    reset_timestamp = json.loads(e.response.headers['ratelimit-reset'])
                    logger.info(f"{pds,reset_timestamp}")
                    wait_until_reset(reset_timestamp)
                    continue
                else:
                    logger.error(f"Failed to collect from PDS: {pds}, exception: {e}")
                    break

        # save to local file
    with open(pds_did_list_file, "w") as result_file:
        json.dump(result, result_file, indent=4)
    logger.info(f"There are {len(result)} PDSs reachable, and {sum([len(repos) for _,repos in result.items()])} active users")


# Wait until rate limit reset time based on status code 429
def wait_until_reset(reset_timestamp):
    reset_time = convert_unix_to_datetime(reset_timestamp)
    sleep_time = (reset_time - datetime.now()).total_seconds()
    if sleep_time > 0:
        logger.info(f"Rate limit exceeded. Sleeping until {reset_time} (local time)")
        time.sleep(sleep_time)

def convert_unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(int(unix_timestamp))


# save each repo as a file did-plc-xxxx.txt
def save_records_atproto(pds_did_list_file) -> None:
    logger.info("Starting records collection...")
    pds_user_records_folder = 'all_records_pds_user/'
    # assuming pds_did_list_file has all the latest info
    with open(pds_did_list_file, "r") as result_file:
        pdss = json.load(result_file)

    if not os.path.exists(pds_user_records_folder):
        os.makedirs(pds_user_records_folder)
    else:
        # skip the ones that's been done
        existing_files = set(os.listdir(pds_user_records_folder))
        logger.info(f"skipping {len(existing_files)} dids...")
        for pds, repos in list(pdss.items()):
            for repo in list(repos):
                expected_filename = f"{repo['did'].replace(':', '-')}.txt"
                if expected_filename in existing_files:
                    repos.remove(repo)

            if not repos:  # If no repos left for this PDS, remove the PDS key
                del pdss[pds]

    logger.info(f"Going to collect from {len(pdss)} PDSs!")
    for pds, repos in pdss.items():
        logger.info(f"Collecting user records on {pds} for {len(repos)} users...")
        client = Client(base_url=f'{pds}/xrpc')
        client.login(username, password)
        for repo in repos:
            logger.debug(repo)
            repo_did = repo['did']
            success = False
            while not success:
                try:
                    repo_response = client.com.atproto.sync.get_repo({'did': repo_did})
                    car_file = CAR.from_bytes(repo_response)
                    repo_did_filename = f'{repo_did.replace(":", "-")}.txt'
                    with open(f"{os.path.join(pds_user_records_folder, repo_did_filename)}", "w+") as f:
                        for car_file_cid, car_file_block in car_file.blocks.items():
                            line = f"{str(car_file_cid)}, {str(car_file_block)}\n"
                            f.write(line)
                    logger.info(f"Saved repo for {repo_did} on {pds}")
                    success = True
                except Exception as e:
                    error_message = str(e)
                    if "InvalidRequest" in error_message: # invalid request
                        logger.warning(f"Invalid Request, checking latest DID PDS: {repo_did, pds}")
                        try:
                            # get the latest endpoint/pds
                            response = client.com.atproto.repo.describe_repo({'repo': repo_did})
                            did_doc = DidDocument.from_dict(response.did_doc)
                            temp_pds = did_doc.get_pds_endpoint()
                            temp_client = Client(base_url=f'{temp_pds}/xrpc')
                            repo_response = temp_client.com.atproto.sync.get_repo({'did': repo_did})
                            car_file = CAR.from_bytes(repo_response)
                            repo_did_filename = f'{repo_did.replace(":", "-")}.txt'
                            with open(f"{os.path.join(pds_user_records_folder, repo_did_filename)}", "w+") as f:
                                for car_file_cid, car_file_block in car_file.blocks.items():
                                    line = f"{str(car_file_cid)}, {str(car_file_block)}\n"
                                    f.write(line)
                            logger.info(f"Saved repo for {repo_did} on {temp_pds}")
                            success = True
                        except Exception as e:
                            logger.error(f"Failed(2) to get repo for {repo_did}")
                            try:
                                logger.error(f"Error {e.status_code}: {str(e)}")
                            finally:
                                break
                    elif"RateLimitExceeded" in error_message: # rate-limit reached
                        logger.warning(f"Rate-Limit Reached, wait till reset time...")
                        reset_timestamp = json.loads(e.response.headers['ratelimit-reset'])
                        logger.info(f"{repo_did,pds,reset_timestamp}")
                        wait_until_reset(reset_timestamp)
                    else:
                        try:
                            logger.error(f"Error {e.status_code}: {str(e)}, Failed to get repo for {repo_did} on {pds}")
                        finally:
                            break


if __name__ == '__main__':
    # Configure the logger
    log_file = 'user_records_stdout.log'
    max_log_size = 100 * 1024 * 1024  # Max log file size in bytes (e.g., 100 MB)
    backup_count = 50  # Number of backup files to keep

    log_folder = "log"
    os.makedirs(log_folder, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format='%(asctime)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            RotatingFileHandler(os.path.join(log_folder,log_file), maxBytes=max_log_size, backupCount=backup_count),
            # logging.StreamHandler()  # Uncomment if you want to log to standard output as well
        ]
    )
    logger = logging.getLogger('user_records_logger')
    sys.stderr = open(os.path.join(log_folder,'user_records_stderr.log'), 'a')
    
    # file containing all did records from https://plc.directory/
    plc_data_file = "plc_data.json"
    # file where you want get_pds_repo_dict() saves the results to as {'pds':[repo_dict,...],...}
    pds_did_list_file = "pds_did_list.json"

    bsky_credentials_file = "bsky_login_info.json"

    try:
        username, password = load_credentials(bsky_credentials_file)
    except Exception as e:
        logger.critical(f"Failed to load bsky credentials from {bsky_credentials_file}, error: {e}")
        sys.exit(1)

    get_pds_repo_dict(plc_data_file)
    save_records_atproto(pds_did_list_file)
