import os
import csv
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here
from google.cloud import storage, bigquery
from google.oauth2 import service_account

PROJECT_ID = "data-engineer-bootcamp-384606"
BUCKET_NAME = "deb-bootcamp-personal-practice"
BUSINESS_DOMAIN = "greenery"
DATASET_ID = "pipeline_practice"
LOCATION = "asia-southeast1"

DEFAULT_PATH = "/opt/airflow"
DAGS_FOLDER = "/opt/airflow/dags"
DATA_FOLDER = "/opt/airflow/dags/data"
MY_PRACTICE_FOLER = "/opt/airflow/dags/my-practice"
DATA = "order-items"

def _extract_data():
    # Your code below
    url = f"http://34.87.139.82:8000/{DATA}"
    response = requests.get(url)
    data = response.json()

    # print(os.getcwd())
    with open(f"{MY_PRACTICE_FOLER}/data/{DATA}.csv", "w") as file:
        header = data[0].keys()
        writer = csv.writer(file)
        writer.writerow(header)
        for line in data:
            writer.writerow(line.values())



def _load_data_to_gcs():
    # Your code below
    keyfile_gcs = f"{MY_PRACTICE_FOLER}/cred.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gsc = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gsc
    )
    
    bucket = storage_client.bucket(BUCKET_NAME)
    src_path = f"{MY_PRACTICE_FOLER}/data/{DATA}.csv"
    destination_path = f"pipeline-practice/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"

    blob = bucket.blob(destination_path)
    blob.upload_from_filename(src_path)
    print(f"Upload from {src_path} to {destination_path} in Bucket: {BUCKET_NAME}")




def _load_data_from_gcs_to_bigquery():
    # Your code below
    # Steps
    ## Declare credential
    ## Connect GCS for pointing to src file
    ## Connect to BQ specific dataset
    ## Confiure the Job for BQ
    ## Start the Job
    ## Execute Job

    keyfile_ = f"{MY_PRACTICE_FOLER}/cred.json"
    service_account_info_ = json.load(open(keyfile_))
    credentials_ = service_account.Credentials.from_service_account_info(service_account_info_)

    # bq_client = bigquery.Client(
    #     credentials=credentials_,

    # )
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_
    )

    bucket = storage_client.bucket(BUCKET_NAME)
    blob_src_path = f"pipeline-practice/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    # destination_path = ""
    blob = bucket.blob(blob_src_path)

    if (blob.exists()):
        # Start of BQ section
        bq_client = bigquery.Client(
            project=PROJECT_ID,
            credentials=credentials_
        )

        src_uri = f"gs://{BUCKET_NAME}/{blob_src_path}"
        table_id = f"{PROJECT_ID}.{DATASET_ID}._pipeline_{DATA}"
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True
        )

        job = bq_client.load_table_from_uri(
            src_uri,
            table_id,
            job_config=job_config,
            location="asia-southeast1"
        )
        job.result()

        table_result = bq_client.get_table(table_id)
        print(f"Loaded {table_result.num_rows} rows and {len(table_result.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="pipeline_order_items",  # Replace xxx with the data name
    default_args=default_args,
    schedule="@daily",  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery
