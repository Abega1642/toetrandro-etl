import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg2
from airflow.models import DagBag, Variable
from testcontainers.postgres import PostgresContainer


@unittest.skip("Requires Airflow, skipping in CI")
class TestToetrandroETLDAG(unittest.TestCase):
    DAG_ID = "toetrandro_etl_pipeline"

    @classmethod
    def setUpClass(cls):
        cls.postgres = PostgresContainer("postgres:15")
        cls.postgres.start()
        cls.db_config = {
            "dbname": cls.postgres.env.get("POSTGRES_DB", "test"),
            "user": cls.postgres.env.get("POSTGRES_USER", "test"),
            "password": cls.postgres.env.get("POSTGRES_PASSWORD", "test"),
            "host": cls.postgres.get_container_host_ip(),
            "port": int(cls.postgres.get_exposed_port(5432)),
        }

        Variable.set("toetrandro_db_config", json.dumps(cls.db_config))

        for _ in range(10):
            try:
                psycopg2.connect(**cls.db_config).close()
                break
            except psycopg2.OperationalError:
                time.sleep(1)

        dag_folder = Path(__file__).resolve().parent.parent / "workflows"
        cls.dagbag = DagBag(dag_folder=str(dag_folder))

        if cls.dagbag.import_errors:
            raise RuntimeError(f"DAG import errors: {cls.dagbag.import_errors}")

        cls.dag = cls.dagbag.get_dag(cls.DAG_ID)

    @classmethod
    def tearDownClass(cls):
        cls.postgres.stop()

    def setUp(self):
        Variable.set("toetrandro_db_config", json.dumps(self.db_config))

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag, f"DAG '{self.DAG_ID}' failed to load.")
        self.assertEqual(self.dag.dag_id, self.DAG_ID)

    def test_all_tasks_present(self):
        expected_tasks = {
            "establish_city_config",
            "extract_weather_data",
            "transform_enriched_data",
            "merge_processed_files",
            "migrate_data_to_postgres",
        }
        self.assertEqual(set(self.dag.task_ids), expected_tasks)

    def test_task_dependencies(self):
        self.assertSetEqual(
            self.dag.get_task("establish_city_config").downstream_task_ids,
            {"extract_weather_data"},
        )
        self.assertSetEqual(
            self.dag.get_task("extract_weather_data").downstream_task_ids,
            {"transform_enriched_data"},
        )
        self.assertSetEqual(
            self.dag.get_task("transform_enriched_data").downstream_task_ids,
            {"merge_processed_files"},
        )
        self.assertSetEqual(
            self.dag.get_task("merge_processed_files").downstream_task_ids,
            {"migrate_data_to_postgres"},
        )

    @patch("workflows.scripts.cities_config_step.CityConfigStep.run")
    def test_city_config_task(self, mock_run):
        task = self.dag.get_task("establish_city_config")
        task.python_callable()
        mock_run.assert_called_once()

    @patch("workflows.scripts.extract_step.ExtractStep.run")
    def test_extract_task(self, mock_run):
        task = self.dag.get_task("extract_weather_data")
        task.python_callable()
        mock_run.assert_called_once()

    @patch("workflows.scripts.transform_step.TransformStep.run")
    def test_transform_task(self, mock_run):
        task = self.dag.get_task("transform_enriched_data")
        task.python_callable()
        mock_run.assert_called_once()

    @patch("workflows.scripts.merge_step.MergeStep")
    def test_merge_task(self, mock_merge_step_class):
        from workflows.dags import toetrandro_etl as dag_module

        dag_module.run_merge_step(mock_merge_step_class, ds="2025-07-05")
        mock_merge_step_class.assert_called_once_with("2025-07-05")
        mock_merge_step_class.return_value.run.assert_called_once()

    @patch("workflows.scripts.migration_step.MigrationStep.run")
    @patch("airflow.models.Variable.get")
    def test_migration_task(self, mock_variable_get, mock_run):
        db = '{"dbname": "test", "user": "test", "password": "test", "host": "localhost", "port": 5432}'
        mock_variable_get.return_value = db
        task = self.dag.get_task("migrate_data_to_postgres")
        task.python_callable()
        mock_run.assert_called_once()
