import json
import os
import sys
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)


from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from workflows.scripts.cities_config_step import CityConfigStep
from workflows.scripts.extract_step import ExtractStep
from workflows.scripts.merge_step import MergeStep
from workflows.scripts.migration_step import MigrationStep
from workflows.scripts.transform_step import TransformStep

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 29),
    "retries": 3,
}

city_list = ["New York", "Paris", "Tokyo", "Toliara", "Mahajanga", "Toamasina"]

db_config = json.loads(Variable.get("toetrandro_db_config"))


with DAG(
    dag_id="toetrandro_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["toetrandro", "etl"],
) as dag:

    def run_city_config():
        CityConfigStep(city_list).run()

    def run_extract():
        ExtractStep().run()

    def run_merge_step(step_class, **kwargs):
        execution_date = kwargs["ds"]
        step_class(execution_date).run()

    def run_transform():
        TransformStep().run()

    def run_migration():
        MigrationStep(db_config).run()

    city_config_task = PythonOperator(
        task_id="establish_city_config",
        python_callable=run_city_config,
        dag=dag,
    )

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_enriched_data",
        python_callable=run_transform,
    )

    merge_task = PythonOperator(
        task_id="merge_processed_files",
        python_callable=lambda **kwargs: run_merge_step(MergeStep, **kwargs),
    )

    migration_task = PythonOperator(
        task_id="migrate_data_to_postgres",
        python_callable=run_migration,
    )

    city_config_task >> extract_task >> transform_task >> merge_task >> migration_task
