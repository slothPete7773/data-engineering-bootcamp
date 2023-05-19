from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="first_dag",
    schedule=None,
    start_date=timezone.datetime(2023, 5, 1),
    tags=["DEB", "workshop"],
    catchup=False,
):
    t0 = EmptyOperator(task_id="t0")
    t1 = EmptyOperator(task_id="t1")

    t0 >> t1