import unittest

from src.utils.city_geo_coordinates.city import City


class TestCityLocation(unittest.TestCase):

    def test_init_sets_name_correctly(self):
        city = City("Tokyo")
        self.assertEqual(city.name, "Tokyo")

    def test_init_coordinates_none(self):
        city = City("Tokyo")
        self.assertIsNone(city.latitude)
        self.assertIsNone(city.longitude)

    def test_set_coordinates_correctly(self):
        city = City("Nairobi")
        city.set_coordinates(-1.2921, 36.8219)
        self.assertEqual(city.latitude, -1.2921)
        self.assertEqual(city.longitude, 36.8219)

    def test_to_dict_output_format(self):
        city = City("Rome")
        city.set_coordinates(41.9028, 12.4964)
        self.assertEqual(
            city.to_dict(),
            {"name": "Rome", "lat": 41.9028, "lon": 12.4964}
        )

    def test_set_coordinates_overwrite(self):
        city = City("Cairo")
        city.set_coordinates(30.0, 31.0)
        city.set_coordinates(29.9, 30.9)
        self.assertEqual(city.latitude, 29.9)
        self.assertEqual(city.longitude, 30.9)

    def test_to_dict_with_missing_coordinates(self):
        city = City("Unknown")
        self.assertEqual(
            city.to_dict(),
            {"name": "Unknown", "lat": None, "lon": None}
        )

    def test_coordinate_types(self):
        city = City("Buenos Aires")
        city.set_coordinates(-34.6037, -58.3816)
        self.assertIsInstance(city.latitude, float)
        self.assertIsInstance(city.longitude, float)

    def test_invalid_name_is_still_accepted(self):
        city = City("")
        self.assertEqual(city.name, "")

    def test_to_dict_return_type(self):
        city = City("Sydney")
        self.assertIsInstance(city.to_dict(), dict)

    def test_precision_rounding_handled_externally(self):
        city = City("Lisbon")
        city.set_coordinates(38.716893, -9.139294)
        self.assertAlmostEqual(city.latitude, 38.716893)
        self.assertAlmostEqual(city.longitude, -9.139294)
