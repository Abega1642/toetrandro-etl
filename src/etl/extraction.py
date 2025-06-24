import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.etl.base import ETLStep
from src.utils.logger import get_logger

logger = get_logger(__name__)
load_dotenv()


class Extract(ETLStep):
    BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

    def __init__(self, cities_path="config/cities.json", output_dir="data/raw"):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.output_dir = output_dir
        self.session = requests.Session()

        with open(cities_path, "r") as f:
            self.cities = json.load(f)

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def fetch_weather(self, lat, lon, exclude="minutely,hourly,alerts", units="metric"):
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "exclude": exclude,
            "units": units,
        }
        response = self.session.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def save(self, city_name: str, data: dict):
        if "daily" not in data:
            logger.warning(f"No 'daily' data found for {city_name}. Skipping.")
            return

        rows = []
        for day in data["daily"]:
            try:
                row = {
                    "city": city_name,
                    "timestamp": datetime.fromtimestamp(day["dt"]),
                    "sunrise": datetime.fromtimestamp(day["sunrise"]),
                    "sunset": datetime.fromtimestamp(day["sunset"]),
                    "temp_C": day["temp"]["day"],
                    "temp_min_C": day["temp"]["min"],
                    "temp_max_C": day["temp"]["max"],
                    "feels_like_C": day["feels_like"]["day"],
                    "pressure": day["pressure"],
                    "humidity": day["humidity"],
                    "wind_speed": day.get("wind_speed"),
                    "wind_deg": day.get("wind_deg"),
                    "wind_gust": day.get("wind_gust"),
                    "cloudiness": day.get("clouds"),
                    "precipitation_prob": day.get("pop", 0.0),
                    "rain_1d": day.get("rain", 0.0),
                    "weather_main": (
                        day["weather"][0]["main"] if day.get("weather") else None
                    ),
                    "weather_description": (
                        day["weather"][0]["description"] if day.get("weather") else None
                    ),
                    "summary": day.get("summary"),
                    "extracted_at": datetime.now(),
                }
                rows.append(row)
            except (KeyError, IndexError, TypeError) as e:
                logger.error(f"Error parsing daily data for {city_name}: {e}")

        if not rows:
            logger.warning(f"No valid rows to save for {city_name}. Skipping CSV save.")
            return

        df = pd.DataFrame(rows)

        date_str = datetime.now().strftime("%Y-%m-%d")
        final_output_dir = Path(self.output_dir) / date_str
        file_path = final_output_dir / f"{city_name}.csv"
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
        logger.info("Starting extraction process...")
        for city in self.cities:
            try:
                name = city["name"]
                logger.info(f"Fetching weather for {name}...")
                data = self.fetch_weather(city["lat"], city["lon"])
                self.save(name, data)
            except Exception as e:
                logger.error(f"Failed to extract for {city.get('name')}: {e}")
        logger.info("Extraction completed.")
