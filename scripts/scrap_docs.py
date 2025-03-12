# -----------------------------------------------------------------------------
# This script demonstrates a process to scrape and compile data from a web API,
# handling pagination, rotating user agents, avoiding duplicate records, and
# gracefully persisting progress and results. It also manages scenarios such as
# detecting an IP ban (HTTP 403 response) and records partial progress to resume
# from the point of failure or completion.
#
# Usage and Main Steps:
#   1. **Configuration**:
#       - Adjust the `url` and `initial_page_url` if needed.
#       - Update the `countries` list to specify which countries to scrape.
#   2. **Data Persistence**:
#       - The script reads and writes JSON data to avoid duplicate records and
#         to keep track of which records have been processed.
#       - `completed_filters.json` tracks pagination state so if the script is
#         interrupted (e.g., ban, error), it can resume later from where it
#         left off.
#       - A new timestamped JSON file in the `datas` directory is created to
#         store newly obtained records (e.g., `dataset_<timestamp>.json`).
#   3. **Pagination & Direction**:
#       - The script loops through pages in ascending and then descending
#         order by the specified filter column.
#       - If new, unique records appear, they are added to the dataset.
#         Otherwise, a counter tracks consecutive duplicates to decide when to
#         stop.
#   4. **Request Handling**:
#       - Uses a `requests.Session()` to maintain cookies.
#       - Rotates user agents from a defined list to help reduce the chance of
#         being blocked.
#       - Incorporates simple throttling (`time.sleep`) and error handling.
#
# Note:
#   - Absolutely no functional lines or logic have been changed from the original.
#   - Only documentation and comments have been added to explain the script.
# -----------------------------------------------------------------------------

import os
import json
import time
import requests

import pandas as pd

from glob import glob
from pprint import pprint
from datetime import datetime, timedelta
from random import choice, shuffle, uniform

uniform(1,5)  # Generates a random float between 1 and 5, but not used otherwise

# Get current datetime and format
current_time = datetime.now() - timedelta(hours=5)
time_str = current_time.strftime('%y_%m_%d_%H_%M_%S')

# File paths for saving newly scraped data and for resuming progress
SAVE_PATH = f'datas/dataset_{time_str}.json'
COMPLETED_FILTERS_PATH = 'completed_filters.json'

# Session setup for making requests
session = requests.Session()
url = ""
initial_page_url = ""
session.get(initial_page_url)

# # User agents rotation - older list is commented out
# user_agents = [
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/118.0.2088.46 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/119.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
# ]

# Current rotation of user agents to help obfuscate repeated requests
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0', # ✅
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/118.0.2088.46',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/119.0', # ✅
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0', # ✅
    # 'Brave/1.60.122 (Windows NT 10.0; Win64; x64) Chrome/120.0.6099.129',
]

# Randomly choose one of the user agents for this session
u_agent = choice(user_agents)
print(f"Selected User-Agent: {u_agent}")

# Headers to be used for each request, including the randomly selected User-Agent
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json-patch+json',
    'Origin': 'https://search.fsc.org',
    'Referer': initial_page_url,
    'User-Agent': u_agent,
    'X-Xsrf-Token': session.cookies.get('XSRF-TOKEN', ''),
}

# Load existing data from previously saved JSON files to avoid re-downloading
existing_json_files = glob("dat*/dataset*json")
all_rows = []
for file_path in existing_json_files:
    with open(file_path, encoding='utf-8') as f:
        all_rows += json.load(f)

# Remove duplicates by converting dictionaries to tuples of items, ensuring uniqueness
all_rows = [dict(t) for t in {tuple(d.items()) for d in all_rows}]
existing_certificate_codes = {row['CertificateCode'] for row in all_rows if 'CertificateCode' in row}
print(f"Loaded {len(all_rows)} unique rows with {len(existing_certificate_codes)} unique certificate codes.")

# Load or create the "completed_filters" dictionary to track scraping progress
if os.path.exists(COMPLETED_FILTERS_PATH):
    with open(COMPLETED_FILTERS_PATH, 'r', encoding='utf-8') as f:
        completed_filters = json.load(f)
else:
    completed_filters = {}

# List of countries to filter by
countries = []
countries = ['Colombia']  # Example: scraping only 'Colombia'

# Payload template that will be updated with specific parameters during each request
payload_template = {}

# Threshold for consecutive duplicate pages: script stops if it hits this count
DUPLICATE_PAGE_THRESHOLD = 1e4

# Iterate through the list of countries and handle scraping for each
filter_col = "Country"
for val in countries:
    key = f"{filter_col}:{val}"

    # Initialize or retrieve the filter progress from "completed_filters"
    if key not in completed_filters:
        completed_filters[key] = {
            "status": False,
            "page": 1,
            "direction": "Asc"
        }

    # If the current filter is already fully completed, skip
    if (
        isinstance(completed_filters[key], dict)
        and completed_filters[key].get("status", False) is True
    ):
        print(f"⏩ Skipping already completed {key}")
        continue

    # If the filter's old format is a boolean True, also skip
    if completed_filters[key] is True:
        print(f"⏩ Skipping already completed {key} (old format)")
        continue

    # Attempt scraping in both ascending and descending order
    directions = ['Asc', 'Desc']
    for direction in directions:

        # If the country is fully done, skip the second pass
        if completed_filters[key].get("status") is True:
            break

        # Skip if partial progress indicates we need a different direction first
        if (
            completed_filters[key].get("direction") != direction
            and completed_filters[key].get("status") is False
        ):
            print(f"⏩ Skipping direction={direction} because partial progress is set "
                  f"to {completed_filters[key].get('direction')}")
            continue

        print(f"--- Starting {filter_col}='{val}' / direction={direction} ---")

        # Retrieve the last page number processed (or 1 if none)
        current_page = completed_filters[key].get("page", 1)
        consecutive_duplicates = 0

        # Pagination loop for the current direction
        while True:
            # Build the payload for the current request
            payload = payload_template.copy()
            payload[filter_col] = [val]
            payload['OrderColumn'] = filter_col
            payload['OrderDirection'] = direction
            payload['PageNumber'] = current_page

            # Rotate the User-Agent again to reduce the chance of detection
            headers['User-Agent'] = choice(user_agents)

            try:
                # Send the POST request
                response = session.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    # Parse JSON response and extract rows
                    rows_this_page = response.json().get("Rows", [])
                    if not rows_this_page:
                        print(f"✅ Finished {filter_col}='{val}' direction={direction}")
                        # If ascending is finished, prep partial for descending
                        if direction == 'Asc':
                            completed_filters[key] = {
                                "status": False,
                                "page": 1,
                                "direction": "Desc"
                            }
                            print("✔️  Asc done; next time we'll do Desc from page=1.")
                        else:
                            # If descending is finished, entire country is done
                            completed_filters[key] = {"status": True}

                        # Save progress after finishing either direction
                        with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                            json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                        break

                    # Filter out rows that have already been seen
                    new_rows = [
                        row for row in rows_this_page
                        if row.get('CertificateCode') not in existing_certificate_codes
                    ]
                    if new_rows:
                        all_rows.extend(new_rows)
                        for nr in new_rows:
                            existing_certificate_codes.add(nr['CertificateCode'])
                        print(f"✅ {filter_col}='{val}', Page {current_page} obtained. "
                              f"Added {len(new_rows)} new rows. Total unique rows: {len(all_rows)}")
                        consecutive_duplicates = 0
                    else:
                        consecutive_duplicates += 1
                        print(f"⏩ {filter_col}='{val}', Page {current_page} obtained. "
                              f"No new rows (all duplicates). Consecutive duplicates={consecutive_duplicates}")

                    # Check if consecutive duplicates exceed the threshold
                    if consecutive_duplicates >= DUPLICATE_PAGE_THRESHOLD:
                        print(f"🔄 Skipping {filter_col}='{val}', direction={direction} after "
                              f"{DUPLICATE_PAGE_THRESHOLD} consecutive duplicate pages.")
                        # Mark partial progress and stop to avoid infinite looping
                        completed_filters[key] = {
                            "status": False,
                            "page": current_page,
                            "direction": direction
                        }
                        with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                            json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                        break

                    # Move on to the next page, update partial progress
                    current_page += 1
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)

                    # Save data to disk every 10 pages
                    if current_page % 10 == 0:
                        with open(SAVE_PATH, "w", encoding="utf-8") as file:
                            json.dump(all_rows, file, indent=4, ensure_ascii=False)
                            print(f"💾 Saved at page {current_page}")

                    # Sleep for a random interval to reduce the risk of getting banned
                    time.sleep(uniform(4, 9))

                elif response.status_code == 403:
                    # IP banned (HTTP 403), save partial progress and stop
                    print(f"🚫 IP banned at {filter_col}='{val}', page {current_page}")
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                    break

                else:
                    # Other errors: save partial progress and stop
                    print(f"⚠️ Error {response.status_code}: {response.text}")
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                    break

            except requests.exceptions.RequestException as e:
                # Request-level exception: save partial progress and retry later
                print(f"⚠️ Request Exception: {e}. Retrying in 10 seconds.")
                time.sleep(10)
                completed_filters[key] = {
                    "status": False,
                    "page": current_page,
                    "direction": direction
                }
                with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                    json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                break

        # If the entire country is done (status=True) during Asc or Desc, skip the other direction
        if isinstance(completed_filters.get(key), dict) and completed_filters[key].get("status"):
            break

# Final save of all collected rows after finishing all countries/directions
with open(SAVE_PATH, "w", encoding="utf-8") as file:
    json.dump(all_rows, file, indent=4, ensure_ascii=False)
    print(f"💾 Final save done. Total rows: {len(all_rows)}")
