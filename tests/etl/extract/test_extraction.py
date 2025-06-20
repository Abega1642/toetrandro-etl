from datetime import datetime

import pytest
from unittest.mock import patch
from src.etl.extraction import Extract
from src.etl.base import ETLStep

DUMMY_WEATHER = {
    "daily": [
        {
            "dt": 1718505600,
            "temp": {"day": 25, "night": 18},
            "wind_speed": 3.2,
            "pop": 0.1,
            "humidity": 60,
            "weather": [{"main": "Clouds", "description": "overcast clouds"}]
        }
    ]
}

DUMMY_WEATHER_NO_DAILY = {
    "current": {"temp": 25}
}

cities_path = 'test_city.json'

class TestExtract:

    def test_init_extract_class(self):
        extractor = Extract(cities_path=cities_path)
        assert isinstance(extractor, ETLStep)
        assert extractor.cities[0]["name"] == "Testville"


    @patch("src.etl.extraction.requests.Session.get")
    def test_fetch_weather_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = DUMMY_WEATHER
        extractor = Extract(cities_path='test_city.json')
        result = extractor.fetch_weather(12.34, 56.78)
        assert "daily" in result
        assert isinstance(result["daily"], list)

    @patch("src.etl.extraction.requests.Session.get")
    def test_fetch_weather_failure(self, mock_get):
        mock_get.side_effect = Exception("API error")
        extractor = Extract(cities_path=cities_path)
        with pytest.raises(Exception, match="API error"):
            extractor.fetch_weather(12.34, 56.78)

    def test_save_valid_csv(self, tmp_path):
        output_dir = tmp_path / "raw"
        output_dir.mkdir()
        extractor = Extract(cities_path=cities_path, output_dir=output_dir)
        extractor.save("Testville", DUMMY_WEATHER)

        files = list((output_dir /datetime.now().strftime("%Y-%m-%d")).glob("*.csv"))
        assert files[0].name.startswith("Testville")


    def test_save_skips_invalid_json(self, tmp_path, caplog):
        output_dir = tmp_path / "raw"
        output_dir.mkdir()
        extractor = Extract(cities_path=cities_path, output_dir=output_dir)
        extractor.save("Nowhere", DUMMY_WEATHER_NO_DAILY)

        assert "Skipping" in caplog.text
        assert len(list(output_dir.glob("*.csv"))) == 0

    @patch("src.etl.extraction.Extract.fetch_weather", return_value=DUMMY_WEATHER)
    @patch("src.etl.extraction.Extract.save")
    def test_apply_full_pipeline(self, mock_save, mock_fetch):
        extractor = Extract(cities_path=cities_path)
        extractor.apply()

        mock_fetch.assert_called_once()
        mock_save.assert_called_once()
