from src.core.city_config import CityConfigurer
from src.utils.logger import get_logger
from workflows.scripts.base import ETLStep

logger = get_logger(__name__)


class CityConfigStep(ETLStep):
    def __init__(self, city_names):
        self.city_names = city_names

    def run(self):
        logger.info("🌍 Starting city configuration step...")
        configurer = CityConfigurer(self.city_names)
        configurer.apply()
        logger.info("✅ City configuration step completed.")
