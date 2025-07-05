from pathlib import Path

import psycopg2

from src.core.base import Process
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Migration(Process):
    def __init__(self, db_config, csv_path=None):
        self.db_config = db_config

        base_dir = Path(__file__).resolve().parents[2]
        self.csv_path = base_dir / "data" / "merged" / "ready_data.csv"
        self.conn = None

    def apply(self):
        try:
            self._connect()
            self._load_staging_data()
            self._insert_dim_city()
            self._insert_dim_date()
            self._insert_dim_weather()
            self._insert_weather_facts()
            self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()

    def _connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        logger.info("Connected to the database.")

    def _load_staging_data(self):
        with self.conn.cursor() as cur:
            logger.info("Loading data into staging_ready_data from CSV...")
            cur.execute("TRUNCATE TABLE staging_ready_data;")
            with open(self.csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    """
                    COPY staging_ready_data FROM STDIN WITH CSV HEADER DELIMITER ',';
                    """,
                    f,
                )
            logger.info("staging_ready_data loaded successfully.")

    def _insert_dim_city(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dim_city (city_name)
                SELECT DISTINCT city FROM staging_ready_data
                ON CONFLICT (city_name) DO NOTHING;
                """
            )
            logger.info("dim_city updated.")

    def _insert_dim_date(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dim_date (date_value, year, month, day_of_week)
                SELECT DISTINCT DATE(timestamp), year, month, day_of_week
                FROM staging_ready_data
                ON CONFLICT (date_value) DO NOTHING;
                """
            )
            logger.info("dim_date updated.")

    def _insert_dim_weather(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dim_weather (weather_main, weather_description)
                SELECT DISTINCT weather_main, weather_description
                FROM staging_ready_data
                ON CONFLICT (weather_main, weather_description) DO NOTHING;
                """
            )
            logger.info("dim_weather updated.")

    def _insert_weather_facts(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weather_facts (
                    city_id, date_id, weather_id,
                    sunrise, sunset, temp_C, temp_min_C, temp_max_C, feels_like_C,
                    pressure, humidity, wind_speed, wind_deg, wind_gust, cloudiness,
                    precipitation_prob, rain_1d, summary, extracted_at,
                    is_ideal_temp, is_low_rain, is_low_wind, is_ideal_humidity,
                    comfort_score, is_ideal_day
                )
                SELECT
                    c.city_id,
                    d.date_id,
                    w.weather_id,
                    s.sunrise, s.sunset, s.temp_C, s.temp_min_C, s.temp_max_C, s.feels_like_C,
                    s.pressure, s.humidity, s.wind_speed, s.wind_deg, s.wind_gust, s.cloudiness,
                    s.precipitation_prob, s.rain_1d, s.summary, s.extracted_at,
                    s.is_ideal_temp, s.is_low_rain, s.is_low_wind, s.is_ideal_humidity,
                    s.comfort_score, s.is_ideal_day
                FROM staging_ready_data s
                JOIN dim_city c ON s.city = c.city_name
                JOIN dim_date d ON DATE(s.timestamp) = d.date_value
                LEFT JOIN dim_weather w ON s.weather_main = w.weather_main 
                    AND s.weather_description = w.weather_description
                LEFT JOIN weather_facts f ON
                    f.city_id = c.city_id AND
                    f.date_id = d.date_id
                WHERE f.fact_id IS NULL;
                """
            )
            logger.info("weather_facts updated.")
