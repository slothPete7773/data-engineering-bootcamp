import json 
import os
from google.cloud import storage
from google.oauth2 import service_account

PROJECT_ID = "data-engineer-bootcamp-384606"
BUCKET_NAME = "deb-bootcamp-personal-practice"
BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"

def load_gcs_partitioned(bucket_name, source_file_name, destination_file_name = None):
    keyfile = "gcs-cred.json"
    gcs_service_account_info = json.load(open(keyfile))
    gcs_credentials = service_account.Credentials.from_service_account_info(gcs_service_account_info)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=gcs_credentials
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

if __name__ == '__main__':
    file_names = os.listdir("./data/")
    for file in ['events.csv']:
        load_gcs_partitioned(BUCKET_NAME, file, file)
