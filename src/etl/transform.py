import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.etl.base import ETLStep

logger = logging.getLogger(__name__)


def get_now():
    return datetime.now()


class Transform(ETLStep):
    def __init__(self, input_dir="data/raw", output_dir="data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting transformation logic")

        df = df.dropna(subset=["temp_C", "rain_1d", "wind_speed", "humidity"])

        df["is_ideal_temp"] = df["temp_C"].between(22, 28)
        df["is_low_rain"] = df["rain_1d"] == 0
        df["is_low_wind"] = df["wind_speed"] < 5.0
        df["is_ideal_humidity"] = df["humidity"].between(30, 70)

        df["comfort_score"] = (
            df["is_ideal_temp"].astype(int) * 0.4
            + df["is_low_rain"].astype(int) * 0.3
            + df["is_low_wind"].astype(int) * 0.2
            + df["is_ideal_humidity"].astype(int) * 0.1
        )

        df["is_ideal_day"] = df["is_ideal_temp"] & df["is_low_rain"] & df["is_low_wind"]

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["month"] = df["timestamp"].dt.month_name()
        df["year"] = df["timestamp"].dt.year
        df["day_of_week"] = df["timestamp"].dt.day_name()

        logger.info("Transformation logic completed")
        return df

    def apply(self):
        logger.info("Starting transformation step")
        date_str = get_now().strftime("%Y-%m-%d")
        input_path = Path(self.input_dir) / date_str
        output_path = Path(self.output_dir) / date_str
        output_path.mkdir(parents=True, exist_ok=True)

        for file in input_path.glob("*.csv"):
            try:
                df = pd.read_csv(file)
                logger.info(f"Transforming file: {file.name}")
                df_clean = Transform.transform_dataframe(self, df)
                output_file = output_path / file.name
                df_clean.to_csv(output_file, index=False)
                logger.info(f"Saved transformed data → {output_file}")
            except Exception as e:
                logger.error(f"Failed to transform {file.name}: {e}")

        logger.info("Transformation step completed")
