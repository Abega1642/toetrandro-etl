from time import sleep

from geopy.geocoders import Nominatim

from src.utils.city_geo_coordinates.city import City


class CityGeocoder:
    def __init__(self, user_agent="city_locator"):
        self.geolocator = Nominatim(user_agent=user_agent)

    def geocode_city(self, city):
        try:
            location = self.geolocator.geocode(city.name)
            if location:
                city.set_coordinates(
                    round(location.latitude, 4), round(location.longitude, 4)
                )
        except Exception as e:
            print(f"Error geocoding '{city.name}': {e}")
        finally:
            sleep(1)

    def geocode_cities(self, city_names):
        cities = [City(name) for name in city_names]
        for city in cities:
            self.geocode_city(city)
        return [city.to_dict() for city in cities]
