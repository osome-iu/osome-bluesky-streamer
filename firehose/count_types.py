import sys
import os
import json
import csv
import re

# Check if filename is passed as an argument
filename = sys.argv[1] if len(sys.argv) > 1 else None
if not filename:
    sys.exit('Missing arg for filename')
elif not os.path.exists(filename):
    sys.exit(f"The file '{filename}' does not exist.")
else:
    pass

# Extract date part (YYYY-MM-DD) from the filename
date_match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
if date_match:
    date_str = date_match.group(0)
else:
    sys.exit('Invalid filename format, expected a date in YYYY-MM-DD format.')

try:
    kinds = {}
    actions_set = set()  # To collect all unique actions dynamically
    total_count = 0

    # Open the file and read line by line
    with open(filename, 'r') as f:
        for line in f:
            try:
                # Parse the line as JSON
                record = json.loads(line)
                kind = record['type']
                action = record['action']

                # Add action to actions_set to track all unique actions
                actions_set.add(action)

                # Increment the counts based on kind and action
                if kind not in kinds:
                    kinds[kind] = {}

                if action in kinds[kind]:
                    kinds[kind][action] += 1
                else:
                    kinds[kind][action] = 1

                # Increment total processed records
                total_count += 1
            except:
                pass

    # Convert actions_set to a sorted list of actions (this will be used as CSV columns)
    action_columns = sorted(actions_set)

    # Prepare CSV output
    csv_output_filename = f"{date_str}_counts.csv"
    with open(csv_output_filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        # Write the header: 'type' followed by all unique action names
        writer.writerow(['type'] + action_columns)
        
        # Write each kind and its counts for all actions
        for kind, actions in kinds.items():
            row = [kind]  # Start the row with the 'type'
            # Add counts for each action (default to 0 if the action is missing for this kind)
            for action in action_columns:
                row.append(actions.get(action, 0))
            writer.writerow(row)

    print(f"CSV output saved to '{csv_output_filename}'")

except FileNotFoundError:
    print(f"File '{filename}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")
