import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.core.transform import Transform


class TestTransform(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "data" / "raw"
        self.output_dir = self.temp_dir / "data" / "processed"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)

        self.transform = Transform()
        self.transform.input_dir = self.input_dir
        self.transform.output_dir = self.output_dir

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_transform_creates_expected_columns(self):
        df = pd.DataFrame(
            {
                "temp_C": [25, 30, None],
                "rain_1d": [0, 5, 0],
                "wind_speed": [3, 6, 2],
                "humidity": [50, 85, 60],
                "timestamp": ["2024-05-01", "2024-05-02", "2024-05-03"],
            }
        )

        result = self.transform.transform_dataframe(df)

        expected_cols = [
            "is_ideal_temp",
            "is_low_rain",
            "is_low_wind",
            "is_ideal_humidity",
            "comfort_score",
            "is_ideal_day",
            "month",
            "year",
            "day_of_week",
        ]

        for col in expected_cols:
            self.assertIn(col, result.columns)

    def test_transform_handles_missing_values_gracefully(self):
        df = pd.DataFrame(
            {
                "temp_C": [25, None],
                "rain_1d": [0, 0],
                "wind_speed": [3, 2],
                "humidity": [50, 60],
                "timestamp": ["2024-05-01", "2024-05-02"],
            }
        )

        result = self.transform.transform_dataframe(df)
        self.assertFalse(result.isnull().any().any())

    def test_comfort_score_with_expected_weights(self):
        df = pd.DataFrame(
            {
                "temp_C": [24],
                "rain_1d": [0],
                "wind_speed": [3],
                "humidity": [50],
                "timestamp": ["2024-05-01"],
            }
        )

        result = self.transform.transform_dataframe(df)
        self.assertAlmostEqual(result.iloc[0]["comfort_score"], 1.0)

    def test_transform_sets_correct_is_ideal_day_flag(self):
        df = pd.DataFrame(
            {
                "temp_C": [24],
                "rain_1d": [0],
                "wind_speed": [3],
                "humidity": [60],
                "timestamp": ["2024-05-01"],
            }
        )

        result = self.transform.transform_dataframe(df)
        self.assertTrue(result.iloc[0]["is_ideal_day"])

    def test_transform_apply_logs_and_saves(self):
        date_str = "2024-05-01"
        input_path = self.input_dir / date_str
        output_path = self.output_dir / date_str
        input_path.mkdir(parents=True)

        df = pd.DataFrame(
            {
                "temp_C": [25],
                "rain_1d": [0],
                "wind_speed": [3],
                "humidity": [50],
                "timestamp": ["2024-05-01"],
            }
        )
        df.to_csv(input_path / "test_city.csv", index=False)

        with patch("src.core.transform.get_now", return_value=datetime(2024, 5, 1)):
            self.transform.apply()

        result_files = list(output_path.glob("*.csv"))
        self.assertTrue(result_files, "No transformed file was saved.")
        result_df = pd.read_csv(result_files[0])
        self.assertIn("comfort_score", result_df.columns)


if __name__ == "__main__":
    unittest.main()
