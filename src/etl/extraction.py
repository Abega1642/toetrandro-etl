import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import pandas as pd

from src.etl.base import ETLStep
from src.utils.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

class Extract(ETLStep):
    BASE_URL = 'https://api.openweathermap.org/data/3.0/onecall'

    def __init__(self, cities_path='config/cities.json', output_dir='data/raw'):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.output_dir = output_dir
        self.session = requests.Session()

        with open(cities_path, 'r') as f:
            self.cities = json.load(f)

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def fetch_weather(self, lat, lon, exclude='minutely,hourly,alerts', units='metric'):
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'exclude': exclude,
            'units': units
        }
        response = self.session.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def save(self, city_name: str, data: dict):
        if "daily" not in data:
            logger.warning(f"No 'daily' data found for {city_name}. Skipping.")
            return

        daily_data = data["daily"]
        df = pd.json_normalize(daily_data)
        df["city"] = city_name
        df["timestamp_utc"] = datetime.now()

        if df.empty:
            logger.warning(f"DataFrame is empty for {city_name}. Skipping CSV save.")
            return

        date_str = datetime.now().strftime("%Y-%m-%d")
        final_output_dire = f'{self.output_dir}/{date_str}'

        file_path = Path(final_output_dire)  / f"{city_name}.csv"
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            df.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Failed to save CSV for {city_name}: {e}")
            return

        if file_path.exists() and file_path.stat().st_size > 0:
            logger.info(f"Saved {len(df)} rows for {city_name} → {file_path}")
        else:
            logger.error(f"File was not written or is empty: {file_path}")

    def apply(self):
        logger.info('Starting extraction process...')
        for city in self.cities:
            try:
                name = city['name']
                logger.info(f'Fetching weather for {name}...')
                data = self.fetch_weather(city['lat'], city['lon'])
                self.save(name, data)
            except Exception as e:
                logger.error(f"Failed to extract for {city.get('name')}: {e}")
        logger.info('Extraction completed.')
