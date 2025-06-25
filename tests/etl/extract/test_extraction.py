from unittest.mock import patch

import pytest

from src.etl.base import ETLStep
from src.etl.extraction import Extract

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


class TestExtract:
    def test_init_extract_class(self):
        extractor = Extract(cities_path=CITIES_PATH)
        assert isinstance(extractor, ETLStep)
        assert extractor.cities[0]["name"] == "Testville"

    @patch("src.etl.extraction.requests.Session.get")
    def test_fetch_weather_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = DUMMY_3H_FORECAST
        extractor = Extract(cities_path=CITIES_PATH)
        result = extractor.fetch_weather(12.34, 56.78)
        assert "list" in result
        assert isinstance(result["list"], list)

    @patch("src.etl.extraction.requests.Session.get")
    def test_fetch_weather_failure(self, mock_get):
        mock_get.side_effect = Exception("API error")
        extractor = Extract(cities_path=CITIES_PATH)
        with pytest.raises(Exception, match="API error"):
            extractor.fetch_weather(12.34, 56.78)

    def test_save_skips_missing_forecast_data(self, tmp_path, caplog):
        output_dir = tmp_path / "raw"
        output_dir.mkdir()
        extractor = Extract(cities_path=CITIES_PATH, output_dir=output_dir)
        extractor.save("Testville", DUMMY_WEATHER_INVALID)
        assert "Skipping" in caplog.text
        assert len(list(output_dir.glob("*.csv"))) == 0

    @patch("src.etl.extraction.Extract.fetch_weather", return_value=DUMMY_3H_FORECAST)
    @patch("src.etl.extraction.Extract.save")
    def test_apply_full_pipeline(self, mock_save, mock_fetch):
        extractor = Extract(cities_path=CITIES_PATH)
        extractor.apply()
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()
