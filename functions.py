import functions_framework
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
import time

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    # Only handle finalize event
    if event_type != "google.cloud.storage.object.v1.finalized":
        print(f"Ignoring event type: {event_type}")
        return

    bucket = data["bucket"]
    filename = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {filename}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    try:
        load_bq(filename)
    except Exception as e:
        print(f"Error loading file to BigQuery: {e}")

dataset = 'sales'
table = 'orders'

def load_bq(filename):
    client = bigquery.Client()
    table_ref = client.dataset(dataset).table(table)

    job_config = LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True  # infers schema

    uri = f'gs://salesdatabucket1/{filename}'
    print(f"Loading from URI: {uri}")
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    
    print(f"Started BigQuery load job: {load_job.job_id}")
    
    # Optional: short wait (or remove completely to allow async)
    try:
        load_job.result(timeout=60)
        print(f"{load_job.output_rows} rows loaded into {table}.")
    except Exception as e:
        print(f"Job is still running or timed out: {e}. Check job status in BigQuery: {load_job.job_id}")
