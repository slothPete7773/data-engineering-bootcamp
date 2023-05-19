from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _world():
    print("World")

with DAG(
    dag_id="workshop_02_everyday",
    schedule="@daily",
    start_date=timezone.datetime(2023, 5, 1),
    tags=["DEB", "workshop"],
    catchup=False,
):
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'"
    )
    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello >> world