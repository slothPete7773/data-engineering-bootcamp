from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _sample(ds):
    print(f"datestamp: {ds}")

with DAG(
    dag_id="test_in_nested_folder",
    start_date=timezone.datetime(2023, 5, 1),
    schedule="@daily",
    tags=["my-practice", "2023"]
):
    run_this = PythonOperator(
        task_id="greet",
        python_callable=_sample
    )

    run_this