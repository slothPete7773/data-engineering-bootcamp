import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage, bigquery
from google.oauth2 import service_account

DATA_PATH = "data"
FILE_NAMES = ["addresses", "events", "order_items", "orders", "products", "promos", "users"]
project_id = "data-engineer-bootcamp-384606"

def load_to_gcs(bucket_name, source_file_name, destination_file_name):
    gcs_keyfile = "data-engineer-bootcamp-384606-bb4831597dcb-bq-gcs.json"
    gcs_service_account_info = json.load(open(gcs_keyfile, "r"))
    gcs_credentials = service_account.Credentials.from_service_account_info(
        gcs_service_account_info
    )

    storage_client = storage.Client(
        project=project_id,
        credentials=gcs_credentials
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_file_name}.")

def load_gcs_to_bq(gsutil_uri, project_id, dataset_id, table_name):
# def load_gcs_to_bq(gsutil_uri):
    bq_keyfile = "data-engineer-bootcamp-384606-bb4831597dcb-bq-gcs.json"
    bq_service_account_info = json.load(open(bq_keyfile))
    bq_credentials = service_account.Credentials.from_service_account_info(
        bq_service_account_info
    )
    bq_client = bigquery.Client(
        credentials=bq_credentials
    )

    table_id = f"{project_id}-{dataset_id}-{table_name}"
    bq_table = bq_client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=bq_table.schema,
        source_format=bigquery.SourceFormat.CSV
    )



"""Uploads a file to the bucket."""
# The ID of your GCS bucket
# bucket_name = "slothpete-100033"
# The path to your file to upload
# source_file_name = "local/path/to/file"
# The ID of your GCS object
# destination_blob_name = "storage-object-name"

# keyfile = os.environ.get("KEYFILE_PATH")
if __name__ == '__main__':
    for file_name in FILE_NAMES:
        load_to_gcs("deb-bootcamp-100033", f"data/{file_name}.csv", f"{file_name}/{file_name}.csv")