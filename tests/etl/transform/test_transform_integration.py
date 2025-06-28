import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.etl.transform import Transform


class TestTransformIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir, "data", "raw")
        self.processed_dir = Path(self.temp_dir, "data", "processed")
        self.today = datetime.now().strftime("%Y-%m-%d")
        (self.raw_dir / self.today).mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def write_csv(self, filename, content):
        file_path = self.raw_dir / self.today / filename
        file_path.write_text(content, encoding="utf-8")

    def read_output_df(self, filename):
        output_file = self.processed_dir / self.today / filename
        return pd.read_csv(output_file)

    def test_valid_data_transforms_successfully(self):
        self.write_csv(
            "weather.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "Tokyo,2025-06-28,25.0,0.0,3.5,60\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("weather.csv")
        self.assertIn("comfort_score", df.columns)
        self.assertEqual(df["comfort_score"].iloc[0], 1.0)

    def test_computes_ideal_day_flags_correctly(self):
        self.write_csv(
            "perfect_day.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "Paris,2025-06-28,26.0,0.0,2.0,50\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("perfect_day.csv")
        self.assertTrue(df["is_ideal_day"].iloc[0])

    def test_filters_bad_weather_day(self):
        self.write_csv(
            "stormy.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "London,2025-06-28,10.0,5.0,12.0,90\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("stormy.csv")
        self.assertFalse(df["is_ideal_day"].iloc[0])

    def test_parses_timestamp_fields_and_derivatives(self):
        self.write_csv(
            "time.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "Madrid,2025-12-21,23.0,0.0,1.0,45\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("time.csv")
        self.assertIn("month", df.columns)
        self.assertEqual(df["month"].iloc[0], "December")

    def test_handles_multiple_files_in_directory(self):
        for i in range(3):
            self.write_csv(
                f"city_{i}.csv",
                "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
                f"City{i},2025-06-28,24.0,0.0,3.0,55\n",
            )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        output_files = list((self.processed_dir / self.today).glob("*.csv"))
        self.assertEqual(len(output_files), 3)

    def test_partial_null_values_are_dropped(self):
        self.write_csv(
            "nulls.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "Tokyo,2025-06-28,,0.0,3.0,50\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("nulls.csv")
        self.assertEqual(len(df), 0)

    def test_transformed_output_has_expected_columns(self):
        self.write_csv(
            "check_cols.csv",
            "city,timestamp,temp_C,rain_1d,wind_speed,humidity\n"
            "Tokyo,2025-06-28,24.0,0.0,2.0,55\n",
        )
        transform = Transform(self.raw_dir, self.processed_dir)
        transform.apply()

        df = self.read_output_df("check_cols.csv")
        expected = {
            "city",
            "timestamp",
            "temp_C",
            "rain_1d",
            "wind_speed",
            "humidity",
            "is_ideal_temp",
            "is_low_rain",
            "is_low_wind",
            "is_ideal_humidity",
            "comfort_score",
            "is_ideal_day",
            "month",
            "year",
            "day_of_week",
        }
        self.assertTrue(expected.issubset(set(df.columns)))


if __name__ == "__main__":
    unittest.main()