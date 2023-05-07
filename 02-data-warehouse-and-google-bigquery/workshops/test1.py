import json
import os 
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
SRC_PATH = "data"

keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
PROJECT_ID = "data-engineer-bootcamp-384606"
client = bigquery.Client(
    project=PROJECT_ID,
    credentials=credentials
)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
    clustering_fields=["first_name", "last_name"],
)


FILE_PATH = "users.csv"
# df = pd.read_csv(os.path.join(SRC_PATH, FILE_PATH), parse_dates=["created_at", "updated_at"])
df = pd.read_csv(os.path.join(SRC_PATH, FILE_PATH))
df.info()

table_id = f"{PROJECT_ID}.deb_bootcamp.{FILE_PATH[:-4]}"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")