import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from requests import RequestException

from src.core.extraction import Extract

load_dotenv()


class TestExtractIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api_key = os.getenv("OPENWEATHER_API_KEY")
        if not cls.api_key:
            raise unittest.SkipTest("Missing OPENWEATHER_API_KEY in environment.")

        cls.test_city = {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917}

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cities_path = os.path.join(self.temp_dir, "cities.json")
        with open(self.cities_path, "w", encoding="utf-8") as f:
            json.dump([self.test_city], f)

        self.extractor = Extract(cities_path=self.cities_path, output_dir=self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_fetch_weather_returns_valid_structure(self):
        data = self.extractor.fetch_weather(
            self.test_city["lat"], self.test_city["lon"]
        )
        self.assertIn("list", data)
        self.assertGreater(len(data["list"]), 0)
        self.assertIn("dt_txt", data["list"][0])

    def test_save_creates_csv_with_expected_columns(self):
        data = self.extractor.fetch_weather(
            self.test_city["lat"], self.test_city["lon"]
        )
        self.extractor.save(self.test_city["name"], data)

        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = Path(self.temp_dir) / date_str / f"{self.test_city['name']}.csv"
        self.assertTrue(output_file.exists())
        with open(output_file, "r", encoding="utf-8") as f:
            header = f.readline()
            self.assertIn("timestamp", header)
            self.assertIn("temp_C", header)

    def test_apply_runs_without_error_for_valid_city(self):
        self.extractor.apply()
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = Path(self.temp_dir) / date_str / f"{self.test_city['name']}.csv"
        self.assertTrue(output_file.exists())

    def test_handle_invalid_city_gracefully(self):
        broken_city = {"name": "Atlantis", "lat": 999, "lon": 999}
        with open(self.cities_path, "w", encoding="utf-8") as f:
            json.dump([broken_city], f)

        extractor = Extract(cities_path=self.cities_path, output_dir=self.temp_dir)
        try:
            extractor.apply()
        except RequestException as e:
            self.fail(f"apply() raised an exception unexpectedly: {e}")

    def test_missing_list_data_does_not_create_file(self):
        fake_data = {"city": {"sunrise": 1234567890, "sunset": 1234567890}}
        self.extractor.save("TestCity", fake_data)

        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = Path(self.temp_dir) / date_str / "TestCity.csv"
        self.assertFalse(output_file.exists())


if __name__ == "__main__":
    unittest.main()
