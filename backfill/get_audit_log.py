####################################################
# Get audit logs of all DIDs
# input: unique_did_file, unique DIDs 1 per line
# output: save 1 audit log file per DID to audit_logs_folder ("{did_str}_audit_log.json")
####################################################
import json
import os
import requests
import logging

# Directories and file paths
unique_did_file = 'unique_DIDs.txt'
audit_logs_folder = 'audit_logs/'
failed_logs_file = 'failed_logs.txt'
log_folder = 'log/'
log_file = os.path.join(log_folder, 'audit_log_fetcher.log')

# Ensure log folder exists
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configure logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_audit_log(did):
    url = f"https://plc.directory/{did}/log/audit"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            audit_log_data = response.json()
            return audit_log_data, None
        else:
            logging.warning(f"Failed to fetch audit log for DID {did} - HTTP {response.status_code}")
            return None, did
    except Exception as e:
        logging.error(f"Error fetching audit log for DID {did}: {str(e)}")
        return None, did

def is_audit_log_exists(did, existing_audit_logs):
    return did in existing_audit_logs

def load_existing_audit_logs():
    existing_audit_logs = set()
    if not os.path.exists(audit_logs_folder):
        os.makedirs(audit_logs_folder)
    
    for filename in os.listdir(audit_logs_folder):
        if filename.endswith("_audit_log.json"):
            did = filename.replace("_audit_log.json", "")
            existing_audit_logs.add(did)
    return existing_audit_logs

def fetch_all_audit_logs(unique_did_file):
    failed_dids = []
    
    existing_audit_logs = load_existing_audit_logs()
    logging.info(f"Found {len(existing_audit_logs)} existing audit logs.")
    
    with open(unique_did_file, 'r') as file:
        for line in file:
            did = line.strip()
            did_str = did.replace(":", "-")
            
            if is_audit_log_exists(did_str, existing_audit_logs):
                logging.info(f"Audit log for DID {did} already exists. Skipping.")
                continue
            
            audit_log_data, failed_did = fetch_audit_log(did)
            
            if audit_log_data:
                filename = f"{audit_logs_folder}{did_str}_audit_log.json"
                with open(filename, 'w') as audit_log_file:
                    json.dump(audit_log_data, audit_log_file)
                logging.info(f"Audit log for DID {did} saved to {filename}")
            elif failed_did:
                failed_dids.append(failed_did)
    
    with open(failed_logs_file, 'a+') as failed_file:
        for failed_did in failed_dids:
            failed_file.write(failed_did + '\n')
    
    if failed_dids:
        logging.warning(f"Failed to fetch audit logs for {len(failed_dids)} DIDs. See {failed_logs_file}.")
    
fetch_all_audit_logs(unique_did_file)
logging.info("Script execution ended.")
