#!/usr/bin/env python3

import os
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import List
from dotenv import load_dotenv
from dateutil import parser

# Load environment variables from .env file
load_dotenv('/var/log/.env')

# Configuration
LOG_FILE_DIR = 'logs'
LOG_FILE_PATH = os.path.join(LOG_FILE_DIR, '/var/log/gitlab-logs/gitlab-events.log')
LIMIT = 1000  # Maximum number of records per request

# Retrieve GitLab API key from environment variable
GITLAB_API_KEY = os.getenv('GITLAB_API_KEY')

# Ensure the log directory exists
if not os.path.exists(LOG_FILE_DIR):
    os.makedirs(LOG_FILE_DIR)

def _get_gitlab_api_key() -> dict:
    """Prepare HTTP headers with GitLab API key for SaaS."""
    return {
        'PRIVATE-TOKEN': GITLAB_API_KEY
    }

def get_logs(url: str, headers: dict, params: dict, results: List[dict] = []) -> List[dict]:
    """Fetch logs from GitLab API with pagination."""
    logging.debug(f"Requesting URL: {url} with params: {params}")
    response = requests.get(url, params=params, headers=headers)
    logging.debug(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Response content: {data}")  # Debug: Log the response content
        
        if data:
            results.extend(data)
        
            # Handle pagination
            total_pages = int(response.headers.get("x-total-pages", 1))
            current_page = int(response.headers.get("x-page", 1))

            if current_page < total_pages:
                params["page"] += 1
                return get_logs(url, headers, params, results)
        return results
    else:
        logging.error(f"Failed to fetch logs: {response.status_code} {response.text}")
        return []

def get_last_timestamp():
    """Retrieve the last timestamp from the log file or default to a timestamp 5 hours ago."""
    if os.path.exists(LOG_FILE_PATH) and os.path.getsize(LOG_FILE_PATH) > 0:
        with open(LOG_FILE_PATH, 'r') as file:
            for line in reversed(list(file)):
                try:
                    event = json.loads(line)
                    return parser.isoparse(event['created_at']).strftime('%Y-%m-%dT%H:%M:%SZ')
                except json.JSONDecodeError:
                    logging.error("Failed to decode JSON line: %s", line)
                    continue  # Skip invalid lines
    # Default to 5 hours ago if the log file is empty
    return (datetime.utcnow() - timedelta(hours=5)).strftime('%Y-%m-%dT%H:%M:%SZ')

def flatten_details_field(event):
    """Flatten the 'details' and 'registration_details' fields in the event."""
    flattened_event = event.copy()  # Copy the event to modify it
    details = event.get('details', {})

    # Flatten the 'details' fields
    if isinstance(details, dict):
        for key, value in details.items():
            field_name = f'details_{key}'
            flattened_event[field_name] = value

            # Check if 'registration_details' exists and flatten it
            if key == 'registration_details' and isinstance(value, dict):
                for reg_key, reg_value in value.items():
                    reg_field_name = f'details_registration_{reg_key}'
                    flattened_event[reg_field_name] = reg_value

    return flattened_event

def write_logs(events):
    """Write events to the log file, adding a source field and avoiding duplicates."""
    existing_ids = set()
    
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, 'r') as file:
            for line in file:
                try:
                    log_entry = json.loads(line)
                    existing_ids.add(log_entry['id'])  # Assuming logs have a unique 'id' field
                except json.JSONDecodeError:
                    logging.error("Failed to decode JSON line: %s", line)
                    continue  # Skip invalid lines
    
    with open(LOG_FILE_PATH, 'a') as file:
        for event in events:
            event_id = event['id']  # Assuming logs have a unique 'id' field
            if event_id not in existing_ids:
                # Create a new dictionary with the source field first
                modified_event = {
                    "source": "gitlab-events",
                    **event  # Unpack the original event fields
                }
                file.write(json.dumps(modified_event) + '\n')
                existing_ids.add(event_id)

def main():
    logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG
    
    # Target for GitLab SaaS
    target = ('gitlab.com', 53585858)  # Replace with your GitLab SaaS group ID
    
    params = {
        "page": 1,
        "per_page": LIMIT,
        "created_after": get_last_timestamp()  # Fetch logs after the last timestamp
    }
    
    headers = _get_gitlab_api_key()
    url = f"https://{target[0]}/api/v4/groups/{target[1]}/audit_events"
    logs = get_logs(url, headers, params)
    
    if logs:
        logging.info(f"[*] Logs length: {len(logs)}")

        # Flatten details fields for each log
        flattened_logs = [flatten_details_field(event) for event in logs]

        # Write the flattened logs to the log file
        write_logs(flattened_logs)
    else:
        logging.info("No new events found.")

if __name__ == '__main__':
    main()