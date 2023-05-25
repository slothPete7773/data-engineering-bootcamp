import json
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "data-engineer-bootcamp-384606"
BUCKET_NAME = "deb-bootcamp-personal-practice"
DATASET_ID = "loading_practice"
BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"

# This code cannot run by itself, because it require gsutil_uri from loading data to GCS.

def load_gcs_to_bq(gs_uri, src_filename):
    keyfile = "cred.json"
    bq_service_account_info = json.load(open(keyfile))
    bq_credentials = service_account.Credentials.from_service_account_file(bq_service_account_info)
    bq_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=bq_credentials,
        location=LOCATION
    )

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{src_filename}"
    job_config = bigquery.LoadJonConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        # time_partitioning=bigquery.TimePartitioning(
        #         type=bigquery.TimePartitioningType.DAY,
        #         field="created_at"
        # )
    )
    job = bq_client.load_table_from_uri(
        gs_uri,
        table_id,
        job_config=job_config
    )
    job.result()

    resulting_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(resulting_table.num_rows))
