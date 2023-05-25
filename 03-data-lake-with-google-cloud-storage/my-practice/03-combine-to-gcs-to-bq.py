import json 
import os
from google.cloud import storage, bigquery
from google.oauth2 import service_account

PROJECT_ID = "data-engineer-bootcamp-384606"
BUCKET_NAME = "deb-bootcamp-personal-practice"
BUSINESS_DOMAIN = "greenery"
DATASET_ID = "loading_practice"
LOCATION = "asia-southeast1"

items_partition = ['events.csv', 'orders.csv', 'users.csv']
items_partition_avoid_userError = ['events.csv', 'orders.csv']

def load_gcs_partitioned(bucket_name, source_file_name, destination_file_name = None):
    keyfile = "gcs-cred.json"
    gcs_service_account_info = json.load(open(keyfile))
    gcs_credentials = service_account.Credentials.from_service_account_info(gcs_service_account_info)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=gcs_credentials,
    )

    bucket_cursor = storage_client.bucket(bucket_name)

    date = "2021-02-10"
    # partition = date.replace("-", "")
    # We set partition in storage here, in a hierarchical directory
    ## Typically, a partition hierarchy in file system usually depends on the data, and each data category will require different hierarchy.
    if not (destination_file_name):
        destination_file_name = source_file_name

    destination_file_path = f"{BUSINESS_DOMAIN}/{source_file_name[:-4]}/{date}/{destination_file_name}"

    blob = bucket_cursor.blob(destination_file_path)
    source_file_path = f"./data/{source_file_name}"
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_name} uploaded to {destination_file_path} in Bucket {bucket_name}.")
    
    return destination_file_path

def load_gcs_to_bq(gs_path, src_filename):
    keyfile = "cred.json"
    bq_service_account_info = json.load(open(keyfile))
    bq_credentials = service_account.Credentials.from_service_account_info(bq_service_account_info)
    bq_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=bq_credentials,
        location="asia-southeast1"
    )

    

    if src_filename in items_partition:
        if (src_filename != 'users.csv'):
            PARTITION_DATE = "2021-02-10".replace('-', '')
        else: 
            PARTITION_DATE = "2020-10-23".replace('-', '')
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{src_filename[:-4]}${PARTITION_DATE}"
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at"
            )
        )
    else:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{src_filename[:-4]}"
        job_config = bigquery.LoadJobConfig(
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
        f"gs://{BUCKET_NAME}/{gs_path}",
        table_id,
        job_config=job_config
    )
    job.result()

    resulting_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(resulting_table.num_rows))

if __name__ == '__main__':
    file_names = os.listdir("./data/")
    for file in file_names:
        dest_uri = load_gcs_partitioned(BUCKET_NAME, file, file)
        load_gcs_to_bq(dest_uri, file)
