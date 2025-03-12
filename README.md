# WebScrap_docs

This script demonstrates a process to scrape and compile data from a web API using Python. It handles pagination in both ascending and descending order, rotates user agents to reduce detection risk, and saves partial progress to avoid repeating work if it's interrupted or the IP is temporarily banned.

## Features

- **Pagination & Direction**: Iterates through pages in both ascending and descending order of a chosen filter column (e.g., country name).
- **Duplicate Prevention**: Merges newly obtained rows with existing data and filters out already-seen entries based on a unique identifier (`CertificateCode`).
- **Progress Tracking**: Saves pagination state in `completed_filters.json` after each page, enabling the script to resume exactly where it left off in case of interruptions.
- **Data Persistence**:
  - New data is appended to a timestamped JSON file in the `datas` directory.
  - Partial progress can be resumed using `completed_filters.json`.
- **User-Agent Rotation**: Randomly selects a user agent from a list to minimize the risk of request blocking.
- **Error Handling**: Handles HTTP errors, timeouts, and potential IP bans (HTTP 403 status), pausing and recording progress accordingly.

## Quick Start

1. **Dependencies**:
   - Python 3.x
   - `requests`
   - `pandas`

2. **File Locations**:
   - `dataset_<timestamp>.json` files are stored in the `datas` directory.
   - `completed_filters.json` stores the current scraping state so that you can resume.

3. **Usage**:
   - Adjust the `url` and `initial_page_url` if needed.
   - Update the list of `countries` in the script with the desired targets.
   - Run the script. It will perform the process automatically.
   - If interrupted, simply run it again – it will resume where it left off.

4. **Important Variables**:
   - `DUPLICATE_PAGE_THRESHOLD`: Sets the maximum number of consecutive pages without any new data before stopping.
   - `SAVE_PATH`: Defines the output file path, which includes a timestamp to keep historical snapshots.
   - `COMPLETED_FILTERS_PATH`: Tracks what has already been processed to avoid repeating work.

5. **Notes**:
   - By default, the script sleeps for a random interval (4–9 seconds) between requests to reduce the risk of blocking.
   - The script uses ascending and descending order to gather comprehensive data based on the `OrderColumn`.
