###########################################
# Retrieve labelers' profiles using app.bsky.labeler.getServices
# Docs of the endpoint: https://docs.bsky.app/docs/api/app-bsky-labeler-get-services
# input : "plc_data.jsonl"
# output : 
#   - profiles save to "{labeler_profiles}/{handle}.json"
#   - handle-did list save to "labelers_handle_did.json"
###########################################
import json
import requests
import os
import logging

# Setup logging
log_dir = "log"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "labeler_profiles.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Input file path
input_file = "plc_data.jsonl"  # Replace with your actual file
output_dir = "labeler_profiles"
os.makedirs(output_dir, exist_ok=True)

# API endpoint template
API_URL = "https://public.api.bsky.app/xrpc/app.bsky.labeler.getServices?detailed=true&dids={}"

# Function to process the file and extract DIDs
def extract_dids(file_path):
    extracted_dids = set()
    
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                if "atproto_labeler" in line:
                    try:
                        strip_line = line.strip()
                        data = json.loads(strip_line)  # Parse JSON line
                        did = data.get("did")
                        if did:
                            extracted_dids.add(did)
                    except json.JSONDecodeError:
                        logging.warning("Skipping invalid JSON line.")
    except FileNotFoundError:
        logging.error(f"Input file '{file_path}' not found.")
    
    logging.info(f"Extracted {len(extracted_dids)} unique DIDs.")
    return extracted_dids

# Function to fetch API data and save each by handle
def fetch_and_save_data(dids):
    all_handles_did = {}
    
    for did in dids:
        try:
            response = requests.get(API_URL.format(did))
            if response.status_code == 200:
                response_data = response.json()
                for view in response_data.get("views", []):
                    handle = view.get("creator", {}).get("handle")
                    if handle:
                        all_handles_did[handle] = did
                        file_path = os.path.join(output_dir, f"{handle}.json")
                        with open(file_path, "w", encoding="utf-8") as f:
                            json.dump(view, f, indent=4)
                        logging.info(f"Saved data for handle: {handle}")
            else:
                logging.warning(f"Failed to fetch data for {did}, status code: {response.status_code}")
        except requests.RequestException as e:
            logging.error(f"Request error for {did}: {e}")
    
    handle_did_file = "labelers_handle_did.json"
    with open(handle_did_file, "wt") as f:
        json.dump(all_handles_did, f, indent=4)
    logging.info(f"Handle-DID mapping saved to '{handle_did_file}'")

# Main execution
if __name__ == "__main__":
    dids = extract_dids(input_file)
    fetch_and_save_data(dids)
    logging.info(f"Processing complete. Check the '{output_dir}' directory.")
