from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.scripts.extract_step import ExtractStep
from airflow.scripts.merge_step import MergeStep
from airflow.scripts.transform_step import TransformStep

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
src_path = os.path.join(project_root, "src")

if src_path not in sys.path:
    sys.path.insert(0, src_path)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 28),
    "retries": 1,
}

with DAG(
    dag_id="toetrandro_etl_pipeline_oop",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "ETL", "OOP"],
) as dag:

    def run_extract():
        ExtractStep().run()

    def run_merge(**kwargs):
        execution_date = kwargs["ds"]
        MergeStep(execution_date).run()

    def run_transform():
        TransformStep().run()

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=run_extract,
        provide_context=True,
    )

    merge_task = PythonOperator(
        task_id="merge_processed_files",
        python_callable=run_merge,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_enriched_data",
        python_callable=run_transform,
        provide_context=True,
    )

    extract_task >> merge_task >> transform_task
