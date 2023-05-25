from google.cloud import storage, bigquery
from google.oauth2 import service_account

import json
import os

PROJECT_ID = "data-engineer-bootcamp-384606"
BUCKET_NAME = "deb-bootcamp-personal-practice"
LOCATION = "asia-southeast1"
def load_to_gcs(bucket_name, source_filename, destnation_filename):
    keyfile = "cred.json"
    gcs_service_account_info = json.load(open(keyfile))
    gcs_credentials = service_account.Credentials.from_service_account_info(gcs_service_account_info)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=gcs_credentials
    )

    bucket_cursor = storage_client.bucket(bucket_name)
    blob_file_cursor = bucket_cursor.blob(destnation_filename) 
    blob_file_cursor.upload_from_filename(source_filename)
    print(f"File {source_filename} uploaded to {destnation_filename}.")

def load_from_gcs_to_bigq(gsutil_uri, dataset_id, table_name, source_filename):
    keyfile = "cred.json"
    bigq_service_account_info = json.load(open(keyfile))
    bigq_credentials = service_account.Credentials.from_service_account_info(bigq_service_account_info)

    bigq_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=bigq_credentials
    )

    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    try:
        bigq_client.get_table(table_id)
    except:
        print(f"Table ID: {table_id} Not Found. Creating new table...")
        bigq_client.create_table(table_id)
    # bigq_table = bigq_client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )
    # destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"


    job = bigq_client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/{source_filename}",
        table_id,
        job_config=job_config,
        location=LOCATION
    )
    job.result()

if __name__ == '__main__':
    file_names = os.listdir('./data/')
    for file in file_names:
        # print(f'./data/{file}')
        load_to_gcs(BUCKET_NAME, f'./data/{file}', f'{file}')
        load_from_gcs_to_bigq("empty", "bigq_bootcamp_my_practice", f"{file[:-4]}-table", file)
