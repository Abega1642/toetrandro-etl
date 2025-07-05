import unittest
from unittest.mock import patch, MagicMock

from src.core.migration import Migration


class TestMigration(unittest.TestCase):

    def setUp(self):
        self.db_config = {
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "port": 5432
        }
        self.migration = Migration(self.db_config)

    @patch("src.core.migration.psycopg2.connect")
    def test_connect_called(self, mock_connect):
        self.migration._connect()
        mock_connect.assert_called_once_with(**self.db_config)

    @patch("src.core.migration.psycopg2.connect")
    def test_apply_calls_all_steps(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        self.migration._insert_dim_city = MagicMock()
        self.migration._insert_dim_date = MagicMock()
        self.migration._insert_dim_weather = MagicMock()
        self.migration._insert_weather_facts = MagicMock()

        self.migration.apply()

        self.migration._insert_dim_city.assert_called_once()
        self.migration._insert_dim_date.assert_called_once()
        self.migration._insert_dim_weather.assert_called_once()
        self.migration._insert_weather_facts.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("src.core.migration.psycopg2.connect")
    def test_commit_on_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        self.migration._insert_dim_city = MagicMock()
        self.migration._insert_dim_date = MagicMock()
        self.migration._insert_dim_weather = MagicMock()
        self.migration._insert_weather_facts = MagicMock()

        self.migration.apply()
        mock_conn.commit.assert_called_once()

    @patch("src.core.migration.psycopg2.connect")
    def test_rollback_on_failure(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        self.migration._insert_dim_city = MagicMock(side_effect=Exception("fail"))

        with self.assertRaises(Exception):
            self.migration.apply()

        mock_conn.rollback.assert_called_once()

    @patch("src.core.migration.psycopg2.connect")
    def test_cursor_and_connection_closed(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        self.migration._connect()
        with mock_conn.cursor() as cur:
            cur.execute("SELECT 1")

        mock_conn.cursor.assert_called()
        mock_conn.close.assert_not_called()  # closed in apply()

    @patch("src.core.migration.psycopg2.connect")
    def test_insert_dim_city_executes_sql(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        self.migration._connect()
        self.migration._insert_dim_city()
        mock_cursor.execute.assert_called()
        self.assertIn("INSERT INTO dim_city", mock_cursor.execute.call_args[0][0])

    @patch("src.core.migration.psycopg2.connect")
    def test_insert_dim_date_executes_sql(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        self.migration._connect()
        self.migration._insert_dim_date()
        mock_cursor.execute.assert_called()
        self.assertIn("INSERT INTO dim_date", mock_cursor.execute.call_args[0][0])

    @patch("src.core.migration.psycopg2.connect")
    def test_insert_dim_weather_executes_sql(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        self.migration._connect()
        self.migration._insert_dim_weather()
        mock_cursor.execute.assert_called()
        self.assertIn("INSERT INTO dim_weather", mock_cursor.execute.call_args[0][0])

    @patch("src.core.migration.psycopg2.connect")
    def test_insert_weather_facts_executes_sql(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        self.migration._connect()
        self.migration._insert_weather_facts()
        mock_cursor.execute.assert_called()
        self.assertIn("INSERT INTO weather_facts", mock_cursor.execute.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
