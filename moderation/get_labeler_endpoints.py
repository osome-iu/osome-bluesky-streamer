###########################################
# Retrieve labelers' service endpoints using PLC.directory given their DIDs
# Docs of the endpoint: https://web.plc.directory/api/redoc#operation/ResolveDid
# input : "labelers_handle_did.json"
# output : "labelers_service_endpoints.csv" containing ["handle", "did", "service_endpoint"]
###########################################

import json
import csv
import requests

# Load the dictionary from the JSON file
with open("labelers_handle_did.json", "r") as file:
    labelers_data = json.load(file)

results = []

# Iterate through the dictionary
for handle, did in labelers_data.items():
    url = f"https://plc.directory/{did}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Extract the AtprotoLabeler service endpoint
        service_endpoint = None
        if "service" in data:
            for service in data["service"]:
                if service.get("type") == "AtprotoLabeler":
                    service_endpoint = service.get("serviceEndpoint")
                    break
        
        results.append([handle, did, service_endpoint])
    except requests.RequestException as e:
        print(f"Error fetching {did}: {e}")
        results.append([handle, did, None])

# Save results to CSV
csv_filename = "labelers_service_endpoints.csv"
with open(csv_filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["handle", "did", "service_endpoint"])
    writer.writerows(results)

print(f"Results saved to {csv_filename}")