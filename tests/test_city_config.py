import json
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

from src.core.city_config import CityConfigurer


class TestCityConfigurer(unittest.TestCase):

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_valid_city_list_creates_expected_json(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Paris", "lat": 48.8566, "lon": 2.3522}
        ]

        configurer = CityConfigurer(["Paris"])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            mocked_file.assert_called_once()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertEqual(written_data[0]["name"], "Paris")

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_empty_city_list_outputs_empty_json(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = []

        configurer = CityConfigurer([])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertEqual(written_data, [])

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_invalid_city_name_is_handled_gracefully(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Xyzabc", "lat": None, "lon": None}
        ]

        configurer = CityConfigurer(["Xyzabc"])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertIsNone(written_data[0]["lat"])

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_output_path_override(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917}
        ]

        configurer = CityConfigurer(["Tokyo"])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.establish_cities_config(
                ["Tokyo"], output_path=Path("/tmp/test_cities.json")
            )
            mocked_file.assert_called_with(Path("/tmp/test_cities.json"), "w")

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_directory_creation_if_missing(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Paris", "lat": 48.8566, "lon": 2.3522}
        ]

        configurer = CityConfigurer(["Paris"])
        with patch("builtins.open", mock_open()), patch(
            "pathlib.Path.mkdir"
        ) as mock_mkdir:
            configurer.apply()
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_multiple_cities_are_processed(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
            {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917},
        ]

        configurer = CityConfigurer(["Paris", "Tokyo"])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertEqual(len(written_data), 2)

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_unicode_city_names(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "São Paulo", "lat": -23.5505, "lon": -46.6333}
        ]

        configurer = CityConfigurer(["São Paulo"])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertEqual(written_data[0]["name"], "São Paulo")

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_large_city_list(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": f"City{i}", "lat": i, "lon": i} for i in range(50)
        ]

        configurer = CityConfigurer([f"City{i}" for i in range(50)])
        with patch("builtins.open", mock_open()) as mocked_file:
            configurer.apply()
            handle = mocked_file()
            written_data = json.loads(
                "".join(call.args[0] for call in handle.write.call_args_list)
            )
            self.assertEqual(len(written_data), 50)

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_output_is_json_serializable(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.return_value = [
            {"name": "Paris", "lat": 48.8566, "lon": 2.3522}
        ]

        configurer = CityConfigurer(["Paris"])
        with patch("builtins.open", mock_open()) as mocked_file:
            try:
                configurer.apply()
            except TypeError:
                self.fail("Output was not JSON serializable")

    @patch("src.utils.city_geo_coordinates.city_geocoder.CityGeocoder")
    def test_geocoder_exception_is_handled_gracefully(self, mock_geocoder):
        mock_geocoder = mock_geocoder.return_value
        mock_geocoder.geocode_cities.side_effect = Exception("Geocoding failed")

        configurer = CityConfigurer(["Paris"])
        with patch("builtins.open", mock_open()):
            try:
                configurer.apply()
            except Exception:
                self.fail("Exception was not handled gracefully")
