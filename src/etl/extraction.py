import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import mean

import pandas as pd
import requests
from dotenv import load_dotenv

from src.etl.base import ETLStep
from src.utils.logger import get_logger

logger = get_logger(__name__)
load_dotenv()


class Extract(ETLStep):
    BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"

    def __init__(self, cities_path="config/cities.json", output_dir="data/raw"):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.output_dir = output_dir
        self.session = requests.Session()

        with open(cities_path, "r") as f:
            self.cities = json.load(f)

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def fetch_weather(self, lat, lon, units="metric"):
        params = {"lat": lat, "lon": lon, "appid": self.api_key, "units": units}
        response = self.session.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def save(self, city_name: str, data: dict):
        forecasts = data.get("list", [])
        if not forecasts:
            logger.warning(f"No forecast data for {city_name}. Skipping.")
            return

        today_str = datetime.now().strftime("%Y-%m-%d")
        today_entries = [
            f for f in forecasts if f.get("dt_txt", "").startswith(today_str)
        ]

        if not today_entries:
            logger.warning(f"No 3-hour forecasts for today for {city_name}. Skipping.")
            return

        try:

            def safe_mean(values):
                filtered = [v for v in values if v is not None]
                return mean(filtered) if filtered else None

            row = {
                "city": city_name,
                "timestamp": datetime.now(),
                "sunrise": datetime.fromtimestamp(data["city"]["sunrise"]),
                "sunset": datetime.fromtimestamp(data["city"]["sunset"]),
                "temp_C": safe_mean([e["main"]["temp"] for e in today_entries]),
                "temp_min_C": safe_mean([e["main"]["temp_min"] for e in today_entries]),
                "temp_max_C": safe_mean([e["main"]["temp_max"] for e in today_entries]),
                "feels_like_C": safe_mean(
                    [e["main"]["feels_like"] for e in today_entries]
                ),
                "pressure": safe_mean([e["main"]["pressure"] for e in today_entries]),
                "humidity": safe_mean([e["main"]["humidity"] for e in today_entries]),
                "wind_speed": safe_mean([e["wind"]["speed"] for e in today_entries]),
                "wind_deg": safe_mean([e["wind"]["deg"] for e in today_entries]),
                "wind_gust": safe_mean([e["wind"].get("gust") for e in today_entries]),
                "cloudiness": safe_mean([e["clouds"]["all"] for e in today_entries]),
                "precipitation_prob": safe_mean(
                    [e.get("pop", 0.0) for e in today_entries]
                ),
                "rain_1d": safe_mean(
                    [
                        e.get("rain", {}).get("3h", 0.0)
                        for e in today_entries
                        if "rain" in e
                    ]
                ),
                "weather_main": Counter(
                    [e["weather"][0]["main"] for e in today_entries if e.get("weather")]
                ).most_common(1)[0][0],
                "weather_description": Counter(
                    [
                        e["weather"][0]["description"]
                        for e in today_entries
                        if e.get("weather")
                    ]
                ).most_common(1)[0][0],
                "summary": None,
                "extracted_at": datetime.now(),
            }

            df = pd.DataFrame([row])
            date_str = datetime.now().strftime("%Y-%m-%d")
            final_output_dir = Path(self.output_dir) / date_str
            file_path = final_output_dir / f"{city_name}.csv"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(file_path, index=False)

            logger.info(f"Saved aggregated forecast for {city_name} → {file_path}")
        except Exception as e:
            logger.error(f"Failed to build/save forecast row for {city_name}: {e}")

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
