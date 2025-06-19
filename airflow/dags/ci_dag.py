from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="ci_dag",
    start_date=datetime(2025, 6, 17),
    schedule_interval="@daily",
    catchup=False,
    tags=["ci-dag"]
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
