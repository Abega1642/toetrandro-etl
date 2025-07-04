import json
from pathlib import Path

from src.core.base import Process
from src.utils.city_geo_coordinates.city_geocoder import CityGeocoder
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CityConfigurer(Process):
    def __init__(self, cities_name):
        self.city_names = cities_name

    def establish_cities_config(self, city_names, output_path=None):
        geocoder = CityGeocoder()
        cities_data = geocoder.geocode_cities(city_names)

        base_dir = Path(__file__).resolve().parents[2]
        if output_path is None:
            output_path = base_dir / "config" / "cities.json"

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(cities_data, f, indent=2)

        logger.info(f"✅ Cities config written to {output_path}")

    def apply(self):
        self.establish_cities_config(self.city_names)
