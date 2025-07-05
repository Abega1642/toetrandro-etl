import json
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from requests import RequestException

from src.core.extraction import Extract


class TestExtractIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_city = {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917}

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cities_path = self.temp_dir / "cities.json"
        with open(self.cities_path, "w", encoding="utf-8") as f:
            json.dump([self.test_city], f)

        self.extractor = Extract(cities_path=self.cities_path)
        self.extractor.output_dir = self.temp_dir

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_fetch_weather_returns_valid_structure(self, mock_var):
        data = self.extractor.fetch_weather(
            self.test_city["lat"], self.test_city["lon"]
        )
        self.assertIn("list", data)
        self.assertGreater(len(data["list"]), 0)
        self.assertIn("dt_txt", data["list"][0])

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_save_creates_csv_with_expected_columns(self, mock_var):
        data = self.extractor.fetch_weather(
            self.test_city["lat"], self.test_city["lon"]
        )
        self.extractor.save(self.test_city["name"], data)

        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = self.temp_dir / date_str / f"{self.test_city['name']}.csv"
        self.assertTrue(output_file.exists())
        with open(output_file, "r", encoding="utf-8") as f:
            header = f.readline()
            self.assertIn("timestamp", header)
            self.assertIn("temp_C", header)

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_apply_runs_without_error_for_valid_city(self, mock_var):
        self.extractor.apply()
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = self.temp_dir / date_str / f"{self.test_city['name']}.csv"
        self.assertTrue(output_file.exists())

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_handle_invalid_city_gracefully(self, mock_var):
        broken_city = {"name": "Atlantis", "lat": 999, "lon": 999}
        with open(self.cities_path, "w", encoding="utf-8") as f:
            json.dump([broken_city], f)

        extractor = Extract(cities_path=self.cities_path)
        extractor.output_dir = self.temp_dir
        try:
            extractor.apply()
        except RequestException as e:
            self.fail(f"apply() raised an exception unexpectedly: {e}")

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_missing_list_data_does_not_create_file(self, mock_var):
        fake_data = {"city": {"sunrise": 1234567890, "sunset": 1234567890}}
        self.extractor.save("TestCity", fake_data)

        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = self.temp_dir / date_str / "TestCity.csv"
        self.assertFalse(output_file.exists())


if __name__ == "__main__":
    unittest.main()
