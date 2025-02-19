#############################################################
# For colleting user profile, follows, followers info
# input: a file containing a json dictionary object listing all PDSs and corresponding DIDs
# output: 3 files per did stored in folder profile_follows_follers
#############################################################


from atproto import Client
import time
import sys
import os
import logging
import json
from datetime import datetime, timedelta

# Load credentials from file
def load_credentials(file):
    with open(file, 'r') as f:
        config = json.load(f)
    return config['username'], config['password']

if __name__ == '__main__':

    log_folder = "log"
    os.makedirs(log_folder, exist_ok=True)

    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format='%(asctime)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
        logging.FileHandler(os.path.join(log_folder,'profiles_follows_followers_stdout.log')),
        # logging.StreamHandler()
    ]
    )
    logger = logging.getLogger('profiles_follows_followers_logger')
    sys.stderr = open(os.path.join(log_folder,'profiles_follows_followers_stderr.log'), 'a')



    pds_did_list_file = "pds_did_list.json"
    profile_follows_follers = "profile_follows_followers/"

    with open(pds_did_list_file, "r") as result_file:
        pdss = json.load(result_file)

    if not os.path.exists(profile_follows_follers):
        os.makedirs(profile_follows_follers)
    else:
        # skip the ones that's been done
        existing_dids = set([file.split("_")[0] for file in os.listdir(profile_follows_follers)])
        logger.info(f"skipping {len(existing_dids)} dids...")
        for pds, repos in list(pdss.items()):
            for repo in list(repos):
                if repo['did'].replace(':', '-') in existing_dids:
                    repos.remove(repo)

            if not repos:  # If no repos left for this PDS, remove the PDS key
                del pdss[pds]

    # #########
    logger.info(f"Working on {sum([len(repos) for pds, repos in pdss.items()])} dids...")
    client = Client()
    bsky_credentials_file = "bsky_login_info.json"

    try:
        username, password = load_credentials(bsky_credentials_file)
    except Exception as e:
        logger.critical(f"Failed to load bsky credentials from {bsky_credentials_file}, error: {e}")
        sys.exit(1)
    client.login(username, password)

    request_count = 0
    start_time = datetime.now()
    for pds, repos in pdss.items():
        for repo in repos:
            DID = repo['did']
            DID_str = DID.replace(":","-")
            while True:
                try:
                    if request_count >= 3000:
                        # Calculate remaining time to wait
                        time_passed = datetime.now() - start_time
                        if time_passed < timedelta(minutes=5):
                            wait_time = (timedelta(minutes=5) - time_passed).total_seconds()
                            print(f"Rate limit reached, pausing for {wait_time} seconds")
                            time.sleep(wait_time)

                        # Reset the counter and timer
                        request_count = 0
                        start_time = datetime.now()

                    # Log information about the requests
                    logger.info(f"{DID} getting profile, follows, followers")
                    # get user profile
                    profile = client.app.bsky.actor.get_profile({'actor': DID}).model_dump()
                    del profile['viewer'] # remove viewer
                    logger.info(f"profile: {profile}")
                    
                    # get user follows
                    all_follows = []
                    cursor = None
                    while True:
                        try:
                            if not cursor:
                                response = client.get_follows(actor=DID, limit=100)
                            else:
                                response = client.get_follows(actor=DID, limit=100, cursor=cursor)
                            follows = response.follows # skipping the 'subject' attribute, which is a short actor profile
                            cursor = response.cursor
                            follows_json = [follow.model_dump() for follow in follows] # convert to json
                            for follow_json in follows_json:
                                del follow_json['viewer'] # remove viewer
                            all_follows.extend(follows_json) # append results
                            logger.info(f"got {len(follows_json)} follows")
                            if not cursor: break
                        except Exception as e:
                            logger.error(f"failed to get Follows for {DID}, exception: {e}")
                            break
                    logger.debug(f"Got {len(all_follows)} follows for {DID}: {all_follows}")

                    # get user followers
                    all_followers = []
                    cursor = None
                    while True:
                        try:
                            if not cursor:
                                response = client.get_followers(actor=DID, limit=100)
                            else:
                                response = client.get_followers(actor=DID, limit=100, cursor=cursor)
                            followers = response.followers # skipping the 'subject' attribute, which is a short actor profile
                            cursor = response.cursor
                            followers_json = [follower.model_dump() for follower in followers] # convert to json
                            for follower_json in followers_json:
                                del follower_json['viewer'] # remove viewer
                            all_followers.extend(followers_json) # append results
                            logger.info(f"got {len(followers_json)}")
                            if not cursor: break
                        except Exception as e:
                            logger.error(f"failed to get Followers for {DID}, exception: {e}")
                            break
                    logger.debug(f"Got {len(all_followers)} followers for {DID}: {all_followers}")

                    # save to files
                    data_combo = [profile, all_follows, all_followers]
                    data_kind = ["profile", "follows", "followers"]
                    for i, data in enumerate(data_combo):
                        try:
                            filename = f"{profile_follows_follers}{DID_str}_{data_kind[i]}.json"
                            with open(filename, "w+") as f:
                                json.dump(data, f, indent=4)
                                logger.info(f"Saved {filename}")
                        except Exception as e:
                            logger.error(f"failed to save {filename}, exception: {e}")
                            continue

                    # Increment request counter
                    request_count += 1
                    logger.debug(f"Request count: {request_count}")
                    break
                except Exception as e:
                    logger.error(f"{e}")
                    if 'Profile not found' in str(e):
                        logger.warn(f"The specified profile could not be found. {DID}")
                        break
                    elif '3000' in str(e):
                        # Calculate how much time has passed since the start time
                        time_passed = datetime.now() - start_time
                        if time_passed < timedelta(minutes=5):
                            # Calculate the remaining time to wait
                            wait_time = (timedelta(minutes=5) - time_passed).total_seconds()
                            logger.info(f"pausing for {wait_time:.2f} seconds.")
                            time.sleep(wait_time)
                        # Reset the start time and request counter after the wait
                        start_time = datetime.now()
                        request_count = 0
                    else:
                        logger.error(f"cannot handle error")
                        break
        


    