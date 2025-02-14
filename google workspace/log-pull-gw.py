#!/usr/bin/env python3

import os
import json
import datetime
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Define the scopes and load the service account credentials
SCOPES = ['https://www.googleapis.com/auth/admin.reports.audit.readonly']
SERVICE_ACCOUNT_FILE_PATH = '/var/log/gw-service-account.json'  # Path to your service account key file

# Authenticate with service account credentials from the JSON file
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE_PATH, scopes=SCOPES
)

# If using domain-wide delegation, impersonate an admin account
credentials = credentials.with_subject('radu.draghia@paydock.com')  # Replace with your admin email

# Get the Admin SDK Reports API service
service = build('admin', 'reports_v1', credentials=credentials)

# Define the event types to retrieve (use only valid application names)
event_types = ['admin', 'groups', 'saml', 'user_accounts', 'login']

# Set the directory where logs and timestamps will be stored
LOG_DIR = '/var/log/gw-logs'

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Paths to the log and timestamp files
log_filename = os.path.join(LOG_DIR, 'GW_all_logs.log')
last_timestamps_file = os.path.join(LOG_DIR, 'last_timestamps.json')

# Load last known timestamps from a JSON file
def load_last_timestamps(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            # Convert the ISO timestamp strings back to datetime objects
            return {k: datetime.datetime.fromisoformat(v) if v else None for k, v in data.items()}
    return {}

# Save last known timestamps to a JSON file
def save_last_timestamps(filename, last_timestamps):
    with open(filename, 'w') as f:
        # Convert the datetime objects to ISO timestamp strings
        data = {k: v.isoformat() if v else None for k, v in last_timestamps.items()}
        json.dump(data, f, indent=4)

# Flatten nested JSON fields
def flatten_json(y):
    """Flatten a nested JSON object."""
    def flatten(x, name=''):
        """Recursive helper function for flattening JSON."""
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    out = {}
    flatten(y)
    return out

# Log Retrieval Logic to avoid duplicates
def fetch_and_log_events(event_type, last_timestamp=None):
    logs = []
    page_token = None

    while True:
        try:
            # Call the Admin SDK Reports API
            print(f'Retrieving logs for {event_type} with pageToken: {page_token} and startTime: {last_timestamp}')
            results = service.activities().list(
                userKey='all',
                applicationName=event_type,
                startTime=last_timestamp.isoformat() + 'Z' if last_timestamp else None,
                maxResults=1000,
                pageToken=page_token
            ).execute()

            activities = results.get('items', [])

            if not activities:
                print(f'No new logs for {event_type}.')
                break

            for activity in activities:
                logs.append(activity)

            page_token = results.get('nextPageToken', None)

            # Stop if there are no more pages to fetch
            if not page_token:
                break
            
            # Pause for a short time to avoid hitting API rate limits
            time.sleep(1)

        except HttpError as error:
            print(f"An error occurred: {error}")
            break

    # Return logs
    return logs

# Function to get the latest timestamp from the logs to avoid duplicates
def get_latest_timestamp(logs):
    timestamps = [datetime.datetime.fromisoformat(log['id']['time'].replace('Z', '')) for log in logs]
    return max(timestamps) if timestamps else None

# Save logs to a single local JSON file
def save_logs_to_file(logs, filename):
    if logs:
        with open(filename, 'a') as f:
            for log in logs:
                # Flatten the log entry
                flattened_log = flatten_json(log)
                # Create a new dictionary with 'source' as the first key
                log_with_source = {'source': 'google-workspace'}
                log_with_source.update(flattened_log)
                # Write the log with 'source' as the first key to the file
                json.dump(log_with_source, f)
                f.write('\n')  # Add a newline for clarity between batches

# Main logic to pull logs and avoid duplicates across runs
def main():
    # Load the last timestamps from a file
    last_timestamps = load_last_timestamps(last_timestamps_file)

    for event_type in event_types:
        print(f'Fetching logs for {event_type}...')

        # Get the last known timestamp for the event type
        last_timestamp = last_timestamps.get(event_type)

        # Fetch logs
        logs = fetch_and_log_events(event_type, last_timestamp)

        # Update the last timestamp to avoid duplicates
        if logs:
            last_timestamps[event_type] = get_latest_timestamp(logs)

        # Save logs to a single file
        if logs:
            save_logs_to_file(logs, log_filename)
            print(f'Saved {len(logs)} logs for {event_type}.')

        # Pause for a short time between event types to avoid rate limiting
        time.sleep(2)

    # Save the updated timestamps to the file
    save_last_timestamps(last_timestamps_file, last_timestamps)

    print(f'Log fetching completed. All logs saved to {log_filename}.')

if __name__ == '__main__':
    main()