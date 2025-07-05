import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.base import Process
from src.core.extraction import Extract

DUMMY_3H_FORECAST = {
    "list": [
        {
            "dt": 1750863600,
            "dt_txt": "2025-06-25 09:00:00",
            "main": {
                "temp": 300.0,
                "temp_min": 298.0,
                "temp_max": 302.0,
                "feels_like": 301.0,
                "pressure": 1012,
                "humidity": 60,
            },
            "wind": {"speed": 3.2, "deg": 180, "gust": 4.1},
            "clouds": {"all": 50},
            "pop": 0.2,
            "weather": [{"main": "Clouds", "description": "broken clouds"}],
        }
    ],
    "city": {
        "sunrise": 1750822380,
        "sunset": 1750878253,
    },
}

DUMMY_WEATHER_INVALID = {"current": {"temp": 25}}

CITIES_PATH = "test_city.json"


class TestExtract(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_city_path = self.temp_dir / CITIES_PATH
        with open(self.test_city_path, "w") as f:
            json.dump([{"name": "Testville", "lat": 12.34, "lon": 56.78}], f)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_init_extract_class(self, mock_var):
        extractor = Extract(cities_path=self.test_city_path)
        self.assertIsInstance(extractor, Process)
        self.assertEqual(extractor.cities[0]["name"], "Testville")

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    @patch("src.core.extraction.requests.Session.get")
    def test_fetch_weather_success(self, mock_get, mock_var):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = DUMMY_3H_FORECAST
        extractor = Extract(cities_path=self.test_city_path)
        result = extractor.fetch_weather(12.34, 56.78)
        self.assertIn("list", result)
        self.assertIsInstance(result["list"], list)

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    @patch("src.core.extraction.requests.Session.get")
    def test_fetch_weather_failure(self, mock_get, mock_var):
        mock_get.side_effect = Exception("API error")
        extractor = Extract(cities_path=self.test_city_path)
        with self.assertRaises(Exception) as context:
            extractor.fetch_weather(12.34, 56.78)
        self.assertIn("API error", str(context.exception))

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    def test_save_skips_missing_forecast_data(self, mock_var):
        extractor = Extract(cities_path=self.test_city_path)
        extractor.output_dir = self.temp_dir / "raw"
        extractor.output_dir.mkdir(parents=True)

        with self.assertLogs("src.core.extraction", level="WARNING") as cm:
            extractor.save("Testville", DUMMY_WEATHER_INVALID)
            self.assertTrue(any("Skipping" in msg for msg in cm.output))

        saved_files = list(extractor.output_dir.rglob("*.csv"))
        self.assertEqual(len(saved_files), 0)

    @patch("src.core.extraction.Variable.get", return_value="dummy_api_key")
    @patch("src.core.extraction.Extract.fetch_weather", return_value=DUMMY_3H_FORECAST)
    @patch("src.core.extraction.Extract.save")
    def test_apply_full_pipeline(self, mock_save, mock_fetch, mock_var):
        extractor = Extract(cities_path=self.test_city_path)
        extractor.apply()
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()


if __name__ == "__main__":
    unittest.main()
