import unittest
from unittest.mock import MagicMock

from geopy.exc import GeocoderTimedOut

from src.utils.city_geo_coordinates.city import City
from src.utils.city_geo_coordinates.city_geocoder import CityGeocoder


class TestCityGeocoder(unittest.TestCase):

    def setUp(self):
        self.geocoder = CityGeocoder()
        self.mock_location = MagicMock()
        self.mock_location.latitude = 40.7128
        self.mock_location.longitude = -74.0060

    def test_geocode_city_success(self):
        city = City("New York")
        self.geocoder.geolocator.geocode = MagicMock(return_value=self.mock_location)
        self.geocoder.geocode_city(city)
        self.assertEqual(city.latitude, 40.7128)
        self.assertEqual(city.longitude, -74.0060)

    def test_geocode_city_not_found(self):
        city = City("Atlantis")
        self.geocoder.geolocator.geocode = MagicMock(return_value=None)
        self.geocoder.geocode_city(city)
        self.assertIsNone(city.latitude)
        self.assertIsNone(city.longitude)

    def test_geocode_city_handles_timeout(self):
        city = City("Timeout City")

        def raise_timeout(*args, **kwargs):
            raise GeocoderTimedOut("Timeout")

        self.geocoder.geolocator.geocode = raise_timeout
        self.geocoder.geocode_city(city)
        self.assertIsNone(city.latitude)
        self.assertIsNone(city.longitude)

    def test_geocode_multiple_cities(self):
        self.geocoder.geolocator.geocode = MagicMock(return_value=self.mock_location)
        city_names = ["New York", "London"]
        results = self.geocoder.geocode_cities(city_names)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["name"], "New York")
        self.assertEqual(results[1]["name"], "London")

    def test_city_geocoder_init_sets_user_agent(self):
        custom_agent = "custom_agent"
        custom_geocoder = CityGeocoder(user_agent=custom_agent)
        self.assertEqual(custom_geocoder.geolocator.headers["User-Agent"], custom_agent)

    def test_geocode_city_preserves_original_name(self):
        city = City("Zootopia")
        self.geocoder.geolocator.geocode = MagicMock(return_value=None)
        self.geocoder.geocode_city(city)
        self.assertEqual(city.name, "Zootopia")

    def test_geocoder_instance_type(self):
        from geopy.geocoders import Nominatim

        self.assertIsInstance(self.geocoder.geolocator, Nominatim)

    def test_partial_results_allowed(self):
        mock_geocoder = MagicMock()
        mock_geocoder.geocode.side_effect = [self.mock_location, None]
        self.geocoder.geolocator = mock_geocoder
        city_names = ["New York", "Unknown"]
        results = self.geocoder.geocode_cities(city_names)
        self.assertEqual(results[0]["lat"], 40.7128)
        self.assertIsNone(results[1]["lat"])

    def test_geocode_city_empty_name(self):
        city = City("")
        self.geocoder.geolocator.geocode = MagicMock(return_value=None)
        self.geocoder.geocode_city(city)
        self.assertIsNone(city.latitude)

    def test_geolocator_is_set(self):
        self.assertTrue(hasattr(self.geocoder, "geolocator"))
