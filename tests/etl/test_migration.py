import subprocess
import time
import unittest
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__)

import psycopg2
from testcontainers.postgres import PostgresContainer

from workflows.scripts.migration_step import Migration


class TestMigration(unittest.TestCase):
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

        for i in range(15):
            try:
                conn = psycopg2.connect(**cls.db_config)
                conn.close()
                logger.info(
                    f"[INFO] Postgres ready at {cls.db_config['host']}:{cls.db_config['port']}"
                )
                break
            except psycopg2.OperationalError as e:
                logger.info(f"Waiting for Postgres ({i + 1}/15): {e}")
                time.sleep(1)
        else:
            raise RuntimeError("Postgres container did not start in time")

        cls.db_url = (
            f"postgres://{cls.db_config['user']}:{cls.db_config['password']}@"
            f"{cls.db_config['host']}:{cls.db_config['port']}/{cls.db_config['dbname']}"
        )

        migration_path = (
            Path(__file__).resolve().parent.parent.parent
            / "resources"
            / "db"
            / "migration"
        )

        try:
            result = subprocess.run(
                [
                    "pyway",
                    "migrate",
                    "--database-type",
                    "postgres",
                    "--database-host",
                    cls.db_config["host"],
                    "--database-port",
                    str(cls.db_config["port"]),
                    "--database-name",
                    cls.db_config["dbname"],
                    "--database-username",
                    cls.db_config["user"],
                    "--database-password",
                    cls.db_config["password"],
                    "--database-migration-dir",
                    str(migration_path),
                    "-v",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            logger.info(f"[PYWAY STDOUT]\n{result.stdout}")
            logger.error(f"[PYWAY STDERR]\n{result.stderr}")

        except subprocess.CalledProcessError as e:
            logger.info("[ERROR] Pyway migration failed!")
            logger.info(f"[PYWAY STDOUT]\n{e.stdout}")
            logger.error(f"[PYWAY STDERR]\n{e.stderr}")
            raise

    @classmethod
    def tearDownClass(cls):
        cls.postgres.stop()

    def setUp(self):
        self.migration = Migration(db_config=self.db_config)

    def test_apply_runs_without_error(self):
        try:
            self.migration.apply()
        except Exception as e:
            self.fail(f"Migration.apply() raised an exception: {e}")

    def test_weather_facts_populated(self):
        self.migration.apply()
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM weather_facts;")
                count = cur.fetchone()[0]
        self.assertGreaterEqual(count, 1, "weather_facts should have at least one row.")

    def test_dim_city_has_entries(self):
        self.migration.apply()
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dim_city;")
                count = cur.fetchone()[0]
        self.assertGreaterEqual(
            count, 1, "dim_city should have entries after migration."
        )

    def test_dim_date_has_entries(self):
        self.migration.apply()
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dim_date;")
                count = cur.fetchone()[0]
        self.assertGreaterEqual(
            count, 1, "dim_date should have entries after migration."
        )

    def test_dim_weather_has_entries(self):
        self.migration.apply()
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dim_weather;")
                count = cur.fetchone()[0]
        self.assertGreaterEqual(
            count, 1, "dim_weather should have entries after migration."
        )


if __name__ == "__main__":
    unittest.main()
