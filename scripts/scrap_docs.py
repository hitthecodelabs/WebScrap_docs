import os
import json
import time
import requests

import pandas as pd

from glob import glob
from pprint import pprint
from datetime import datetime, timedelta
from random import choice, shuffle, uniform

uniform(1,5)

# Get current datetime and format
current_time = datetime.now() - timedelta(hours=5)
time_str = current_time.strftime('%y_%m_%d_%H_%M_%S')

# File paths
SAVE_PATH = f'datas/dataset_{time_str}.json'
COMPLETED_FILTERS_PATH = 'completed_filters.json'

# Session setup
session = requests.Session()
url = ""
initial_page_url = ""
session.get(initial_page_url)

# # User agents rotation
# user_agents = [
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/118.0.2088.46 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/119.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
# ]

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0', # ✅
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/118.0.2088.46',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/119.0', # ✅
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0', # ✅
    # 'Brave/1.60.122 (Windows NT 10.0; Win64; x64) Chrome/120.0.6099.129',
]

u_agent = choice(user_agents)
print(f"Selected User-Agent: {u_agent}")
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json-patch+json',
    'Origin': 'https://search.fsc.org',
    'Referer': initial_page_url,
    'User-Agent': u_agent,
    'X-Xsrf-Token': session.cookies.get('XSRF-TOKEN', ''),
}

# Load existing data
existing_json_files = glob("dat*/dataset*json")
all_rows = []
for file_path in existing_json_files:
    with open(file_path, encoding='utf-8') as f:
        all_rows += json.load(f)

# Remove duplicates and initialize certificate codes set
all_rows = [dict(t) for t in {tuple(d.items()) for d in all_rows}]
existing_certificate_codes = {row['CertificateCode'] for row in all_rows if 'CertificateCode' in row}
print(f"Loaded {len(all_rows)} unique rows with {len(existing_certificate_codes)} unique certificate codes.")

# Load completed filters
if os.path.exists(COMPLETED_FILTERS_PATH):
    with open(COMPLETED_FILTERS_PATH, 'r', encoding='utf-8') as f:
        completed_filters = json.load(f)
else:
    completed_filters = {}

# List of countries to filter by
countries = []

countries = ['Colombia']

# Payload template
payload_template = {}

# Threshold for consecutive duplicate pages
DUPLICATE_PAGE_THRESHOLD = 1e4

# Iterate through countries
filter_col = "Country"
for val in countries:
    key = f"{filter_col}:{val}"

    # If the key isn't in completed_filters, initialize it
    if key not in completed_filters:
        completed_filters[key] = {
            "status": False,
            "page": 1,
            "direction": "Asc"
        }

    # If we have a dict with status=True, it's fully completed
    if (
        isinstance(completed_filters[key], dict)
        and completed_filters[key].get("status", False) is True
    ):
        print(f"⏩ Skipping already completed {key}")
        continue

    # If we have an old boolean format "true", skip as well
    if completed_filters[key] is True:
        print(f"⏩ Skipping already completed {key} (old format)")
        continue

    # We'll run for both directions: Asc then Desc
    directions = ['Asc', 'Desc']
    for direction in directions:

        # If the country is fully done, skip
        if completed_filters[key].get("status") is True:
            break

        # If we have partial progress and it's for a different direction, skip
        if (
            completed_filters[key].get("direction") != direction
            and completed_filters[key].get("status") is False
        ):
            print(f"⏩ Skipping direction={direction} because partial progress is set "
                  f"to {completed_filters[key].get('direction')}")
            continue

        print(f"--- Starting {filter_col}='{val}' / direction={direction} ---")

        # Retrieve last saved page (default to 1 if none)
        current_page = completed_filters[key].get("page", 1)
        consecutive_duplicates = 0

        while True:
            payload = payload_template.copy()
            payload[filter_col] = [val]
            payload['OrderColumn'] = filter_col
            payload['OrderDirection'] = direction
            payload['PageNumber'] = current_page

            headers['User-Agent'] = choice(user_agents)

            try:
                response = session.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    rows_this_page = response.json().get("Rows", [])
                    if not rows_this_page:
                        print(f"✅ Finished {filter_col}='{val}' direction={direction}")
                        # If finished Asc, set up partial for Desc (page=1)
                        if direction == 'Asc':
                            completed_filters[key] = {
                                "status": False,
                                "page": 1,
                                "direction": "Desc"
                            }
                            print("✔️  Asc done; next time we'll do Desc from page=1.")
                        else:
                            # If Desc is finished, entire country is done
                            completed_filters[key] = {"status": True}

                        # Save partial or full info
                        with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                            json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                        break

                    # Filter out duplicates
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

                    # Check threshold
                    if consecutive_duplicates >= DUPLICATE_PAGE_THRESHOLD:
                        print(f"🔄 Skipping {filter_col}='{val}', direction={direction} after "
                              f"{DUPLICATE_PAGE_THRESHOLD} consecutive duplicate pages.")
                        # Mark partial progress
                        completed_filters[key] = {
                            "status": False,
                            "page": current_page,
                            "direction": direction
                        }
                        with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                            json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                        break

                    # Increment page and save partial progress
                    current_page += 1
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)

                    # Save data every 10 pages
                    if current_page % 10 == 0:
                        with open(SAVE_PATH, "w", encoding="utf-8") as file:
                            json.dump(all_rows, file, indent=4, ensure_ascii=False)
                            print(f"💾 Saved at page {current_page}")

                    # Sleep to reduce ban risk
                    time.sleep(uniform(4, 9))

                elif response.status_code == 403:
                    print(f"🚫 IP banned at {filter_col}='{val}', page {current_page}")
                    # Mark partial progress
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                    break

                else:
                    print(f"⚠️ Error {response.status_code}: {response.text}")
                    # Mark partial progress
                    completed_filters[key] = {
                        "status": False,
                        "page": current_page,
                        "direction": direction
                    }
                    with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                    break

            except requests.exceptions.RequestException as e:
                print(f"⚠️ Request Exception: {e}. Retrying in 10 seconds.")
                time.sleep(10)
                # Mark partial progress
                completed_filters[key] = {
                    "status": False,
                    "page": current_page,
                    "direction": direction
                }
                with open(COMPLETED_FILTERS_PATH, 'w', encoding='utf-8') as f:
                    json.dump(completed_filters, f, indent=4, ensure_ascii=False)
                break

        # If the entire country is done (status=True), skip next direction
        if isinstance(completed_filters.get(key), dict) and completed_filters[key].get("status"):
            break

# Final save
with open(SAVE_PATH, "w", encoding="utf-8") as file:
    json.dump(all_rows, file, indent=4, ensure_ascii=False)
    print(f"💾 Final save done. Total rows: {len(all_rows)}")
