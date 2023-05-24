from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

import requests
import os
import configparser
import csv
import json

BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "data-engineer-bootcamp-384606"

DEFAULT_PATH = "/opt/airflow/"
DAGS_FOLDER = "/opt/airflow/dags"
DATA_FOLDER = os.path.join(DEFAULT_PATH, "dags", "data")
DATA = "addresses"

# Import credentials
parser = configparser.ConfigParser()

parser.read(os.path.join(DEFAULT_PATH, "dags", "pipeline.conf"))
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here


def _extract_data(ds):
    # Your code below
    data = "addresses"
    # date = "2021-02-10"
    response = requests.get(f"{API_URL}/{DATA}/")
    fetched = response.json()

    with open(f"{DATA_FOLDER}/{DATA}.csv", "w") as file:
        writer = csv.writer(file)
        header = fetched[0].keys()
        writer.writerow(header)
        for each in fetched:
            writer.writerow(each.values())


def _load_data_to_gcs(bucket_name: str, destnation_blob_name: str, source_file_name: str):
    # Your code below
    KEYFILE = 'cred.json'
    service_account_info = json.load(open(KEYFILE, "r"))
    credentias = service_account.Credentials.from_service_account_info(service_account_info)
    
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentias=credentias,
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destnation_blob_name)
    blob.upload_from_filename(source_file_name)


def _load_data_from_gcs_to_bigquery(gsutil_uri):
    # Your code below
    KEYFILE = "cred.json"
    bq_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(KEYFILE)
    )
    table_id = f"{PROJECT_ID}.greenery.{DATA}"
    bq_table = bq_client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=bq_table.schema,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows = 1,
    )

    load_job = bq_client.load_table_from_uri(
        gsutil_uri,
        table_id,
        job_config=job_config
    )
    load_job.result()

    destination_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_addresses_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule=None,  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={"ds": "{{ ds }}"}
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={"bucket_name": "",
                   "destnation_blob_name": "str", 
                   "source_file_name": "str"}
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery