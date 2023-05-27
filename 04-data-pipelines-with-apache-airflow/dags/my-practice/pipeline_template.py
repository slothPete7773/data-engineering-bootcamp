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
DATA = "addresses"

def _extract_data():
    # Your code below
    url = f"http://34.87.139.82:8000/{DATA}"
    response = requests.get(url)
    data = response.json()

    # print(os.getcwd())
    with open(f"./data/{DATA}.csv", "w") as file:
        header = data[0].keys()
        writer = csv.writer(file)
        writer.writerow(header)
        for line in data:
            writer.writerow(line.values())



def _load_data_to_gcs():
    # Your code below
    pass


def _load_data_from_gcs_to_bigquery():
    # Your code below
    pass


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_xxx_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule=None,  # Set your schedule here
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
