#!/usr/bin/env python3

import json
import os
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv

# Load environment variables from .env file located in /var/log - the env file has chmod 600 set (root)
load_dotenv('/var/log/.env')

# Configuration
OKTA_DOMAIN = os.getenv('OKTA_DOMAIN', 'paydock.okta.com')  # Default if not set
OKTA_API_TOKEN = os.getenv('OKTA_API_TOKEN')  # Fetch from .env
LOG_FILE_DIR = 'logs'
LOG_FILE_PATH = '/var/log/okta-logs/okta-events.log'
LIMIT = 1000  # Maximum number of records per request

# Ensure the log directory exists
if not os.path.exists(os.path.dirname(LOG_FILE_PATH)):
    os.makedirs(os.path.dirname(LOG_FILE_PATH))

def fetch_events(start_time):
    """Fetch Okta events from the API starting from the given start time."""
    url = f'https://{OKTA_DOMAIN}/api/v1/logs'
    headers = {'Authorization': f'SSWS {OKTA_API_TOKEN}'}
    params = {
        'since': start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'limit': LIMIT
    }
    
    query_string = urlencode(params)
    request_url = f'{url}?{query_string}'
    
    try:
        response = requests.get(request_url, headers=headers, timeout=60)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        print(f"Response content: {response.text}")
        raise
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise

def get_last_timestamp():
    """Retrieve the last timestamp from the log file."""
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, 'r') as file:
            for line in reversed(list(file)):
                try:
                    event = json.loads(line)
                    return datetime.fromisoformat(event['published'].replace('Z', '+00:00'))
                except json.JSONDecodeError:
                    # Skip lines that cannot be decoded as JSON
                    continue
    return datetime.utcnow() - timedelta(minutes=15)

def flatten_target_field(event):
    """Flatten the 'target' field in the event if it's a JSON string."""
    flattened_event = event.copy()  # Copy the event to modify it
    if isinstance(event.get('target'), str):
        try:
            # Parse the 'target' field if it's a JSON string
            target_array = json.loads(event['target'])
        except json.JSONDecodeError:
            # Handle cases where 'target' field cannot be parsed as JSON
            target_array = []
    elif isinstance(event.get('target'), list):
        # If 'target' is already a list, use it directly
        target_array = event['target']
    else:
        # Handle unexpected types
        target_array = []

    # Flatten the list and add to the event
    for i, item in enumerate(target_array):
        if isinstance(item, dict):
            for key, value in item.items():
                field_name = f'target_{key}_{i}'
                flattened_event[field_name] = value

    return flattened_event

def write_logs(events):
    """Write events to the log file, avoiding duplicates based on timestamp."""
    existing_timestamps = set()
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, 'r') as file:
            for line in file:
                try:
                    log_entry = json.loads(line)
                    existing_timestamps.add(log_entry['published'])
                except json.JSONDecodeError:
                    # Skip lines that cannot be decoded as JSON
                    continue
    
    with open(LOG_FILE_PATH, 'a') as file:
        for event in events:
            timestamp = event.get('published', '')
            if timestamp not in existing_timestamps:
                # Add the source field at the beginning of the event
                modified_event = {
                    "source": "okta-events",
                    **event  # Unpack the original event fields
                }
                file.write(json.dumps(modified_event) + '\n')
                existing_timestamps.add(timestamp)

def main():
    start_time = get_last_timestamp()
    print(f'Fetching events since: {start_time}')
    events = fetch_events(start_time)
    
    if events:
        flattened_events = [flatten_target_field(event) for event in events]
        write_logs(flattened_events)
    else:
        print('No new events found.')

if __name__ == '__main__':
    main()