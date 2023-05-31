import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account



DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "data-engineer-bootcamp-384606"
GCS_BUCKET = "deb-bootcamp-100033"
BIGQUERY_DATASET = "networkrail"
KEYFILE = f"{DAGS_FOLDER}/cred.json"

def _extract_data(**context):
    ds = context["data_interval_start"].to_date_string()
    # Use PostgresHook to query data from Postgres database
    # Your code here
    pg_hook = PostgresHook(
        postgres_conn_id="networkrail_postgres_conn",
        schema="networkrail"
    )

    connection = pg_hook.get_conn()
    pg_cursor = connection.cursor()

    sql_query = f"""
        select *
        from movements
        where date(actual_timestamp)='{ds}'
    """
    logging.info(sql_query)

    pg_cursor.execute(sql_query)
    fetched_result = pg_cursor.fetchall()
    # If we have data, go to the "load_data_to_gcs" task; otherwise, 
    # go to the "do_nothing" task
    if fetched_result:
        with open(f"{DAGS_FOLDER}/data/{DATA}-{ds}.csv", "w", encoding="utf-8") as file:
            # prepare csv writer
            # know headers, and wrrite header to csv first
            # then, write each content to each row
            writer = csv.writer(file)
                header = [
                    "event_type",
                    "gbtt_timestamp",
                    "original_loc_stanox",
                    "planned_timestamp",
                    "timetable_variation",
                    "original_loc_timestamp",
                    "current_train_id",
                    "delay_monitoring_point",
                    "next_report_run_time",
                    "reporting_stanox",
                    "actual_timestamp",
                    "correction_ind",
                    "event_source",
                    "train_file_address",
                    "platform",
                    "division_code",
                    "train_terminated",
                    "train_id",
                    "offroute_ind",
                    "variation_status",
                    "train_service_code",
                    "toc_id",
                    "loc_stanox",
                    "auto_expected",
                    "direction_ind",
                    "route",
                    "planned_event_type",
                    "next_report_stanox",
                    "line_ind",
                ]
                
            writer.writerow(header)
            for item in fetched_result:
                logging.info(item)
                writer.writerow(item)


        return "load_data_to_gcs"
    else:
        return "do_nothing"

def _load_data_to_gcs(**context):
    ds = context["data_interval_start"].to_date_string()

    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials
    )

    bucket = storage_client.bucket(GCS_BUCKET)

    destination_path = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}-{ds}.csv"
    blob = bucket.blob(destination_path)

    src_path = f"{DAGS_FOLDER}/data/{DATA}-{ds}.csv"
    blob.upload_from_filename(src_path)

    if (blob.exists()):
        logging.info(f"Successfully upload {src_path} to {destination_path} in Bucket {GCS_BUCKET}")

def _load_data_from_gcs_to_bigquery(**context):
    ds = context['data_interval_start'].to_date_string()

    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials
    )

    bucket = storage_client.bucket(GCS_BUCKET)
    src_path = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/*.csv"
    blob = bucket.blob(src_path)
    if (blob.exists()):
        bq_client = bigquery.Client(
            project=PROJECT_ID,
            credentials=credentials,
            location=LOCATION
        )

        bigquery_schema = [
            bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("gbtt_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("original_loc_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("planned_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("timetable_variation", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("original_loc_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("current_train_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("delay_monitoring_point", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("next_report_run_time", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("reporting_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("actual_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("correction_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("event_source", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_file_address", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("division_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_terminated", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("train_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offroute_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("variation_status", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_service_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("toc_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("loc_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("auto_expected", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("direction_ind", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("route", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("planned_event_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("next_report_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("line_ind", bigquery.enums.SqlTypeNames.STRING),
        ]
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            schema=bigquery_schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="actual_timestamp"
            )
        )

        partition_identifier = ds.replace("-", "")
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}${partition_identifier}"
        job = bq_client.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{src_path}",
            table_id,
            job_config=job_config,
            location=LOCATION
        )
        job.result()

        fetched_table = bq_client.get_table(table_id)
        msg = f"Loaded {fetched_table.num_rows} rows and " \
            f"{len(fetched_table.schema)} columns to {table_id}"
        logging.info(msg)

    # Declare sstorage instance
    # Point to src file
    # Verify src file
        # declare bq instance 
        # define to the dest table with table_id
        # Define schema for the table
        # Define Job config
        # execute Job
        # Verify job

default_args = {
    "owner": "slothPete7773",
    "start_date": timezone.datetime(2023, 5, 1)
}
with DAG(
    dag_id="networkrail_movements_to_gcs_and_then_bigquery",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=3,
):
    # Start
    start = EmptyOperator(
        task_id="start"
    )

    # Extract data from NetworkRail Postgres Database
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data
    )

    # Do nothing
    do_nothing = EmptyOperator(
        task_id="do_nothing"
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery
    )

    # End
    end = EmptyOperator(
        task_id="end", 
        trigger_rule="one_success"
    )

    # Task dependencies
    start >> extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end