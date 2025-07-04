import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.core.final_merge import FinalMerge


class TestFinalMerge(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        # Define file paths
        self.historical_path = self.temp_path / "historical.csv"
        self.new_data_path = self.temp_path / "new.csv"
        self.output_path = self.temp_path / "output.csv"

        # Define shared columns
        self.columns = [
            "city",
            "timestamp",
            "sunrise",
            "sunset",
            "temp_C",
            "temp_min_C",
            "temp_max_C",
            "feels_like_C",
            "pressure",
            "humidity",
            "wind_speed",
            "wind_deg",
            "wind_gust",
            "cloudiness",
            "precipitation_prob",
            "rain_1d",
            "weather_main",
            "weather_description",
            "summary",
            "extracted_at",
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

        # Create mock historical and new data
        self.historical_df = pd.DataFrame(
            [
                [
                    "CityA",
                    "2020-01-01",
                    "2020-01-01 06:00",
                    "2020-01-01 18:00",
                    25,
                    20,
                    30,
                    25,
                    1010,
                    60,
                    3,
                    180,
                    5,
                    20,
                    0,
                    0,
                    "Clear",
                    "clear sky",
                    "Nice",
                    "2025-07-04",
                    True,
                    True,
                    True,
                    True,
                    1.0,
                    True,
                    "January",
                    2020,
                    "Wednesday",
                ]
            ],
            columns=self.columns,
        )

        self.new_df = pd.DataFrame(
            [
                [
                    "CityA",
                    "2020-01-02",
                    "2020-01-02 06:00",
                    "2020-01-02 18:00",
                    26,
                    21,
                    31,
                    26,
                    1011,
                    61,
                    2,
                    190,
                    4,
                    10,
                    0,
                    0,
                    "Clouds",
                    "few clouds",
                    "Okay",
                    "2025-07-04",
                    True,
                    True,
                    True,
                    True,
                    0.9,
                    True,
                    "January",
                    2020,
                    "Thursday",
                ]
            ],
            columns=self.columns,
        )

        self.historical_df.to_csv(self.historical_path, index=False)
        self.new_df.to_csv(self.new_data_path, index=False)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_merge_adds_new_row(self):
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merged_df = merger.apply()
        self.assertEqual(len(merged_df), 2)

    def test_deduplication_removes_duplicate(self):
        # Duplicate timestamp in new data
        self.new_df["timestamp"] = "2020-01-01"
        self.new_df.to_csv(self.new_data_path, index=False)

        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merged_df = merger.apply()
        self.assertEqual(len(merged_df), 1)

    def test_schema_mismatch_raises_error(self):
        broken_df = self.new_df.drop(columns=["humidity"])
        broken_df.to_csv(self.new_data_path, index=False)

        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        with self.assertRaises(ValueError):
            merger.apply()

    def test_output_file_created(self):
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merger.apply()
        self.assertTrue(self.output_path.exists())

    def test_backup_created_when_overwriting(self):
        # Set output path to historical path to trigger backup
        merger = FinalMerge(
            str(self.historical_path),
            str(self.new_data_path),
            str(self.historical_path),
        )
        merger.apply()
        backup_path = self.historical_path.with_suffix(".bak.csv")
        self.assertTrue(backup_path.exists())

    def test_commit_overwrites_historical(self):
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merger.apply()
        merger.commit()
        hist_df = pd.read_csv(self.historical_path)
        self.assertEqual(len(hist_df), 2)

    def test_commit_skipped_if_output_is_historical(self):
        merger = FinalMerge(
            str(self.historical_path),
            str(self.new_data_path),
            str(self.historical_path),
        )
        merger.apply()
        # Should not raise or overwrite again
        merger.commit()
        self.assertTrue(self.historical_path.exists())

    def test_merge_sorts_by_city_and_timestamp(self):
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merged_df = merger.apply()
        sorted_df = merged_df.sort_values(by=["city", "timestamp"])
        pd.testing.assert_frame_equal(
            merged_df.reset_index(drop=True), sorted_df.reset_index(drop=True)
        )

    def test_empty_new_data(self):
        pd.DataFrame(columns=self.columns).to_csv(self.new_data_path, index=False)
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merged_df = merger.apply()
        self.assertEqual(len(merged_df), 1)

    def test_empty_historical_data(self):
        pd.DataFrame(columns=self.columns).to_csv(self.historical_path, index=False)
        merger = FinalMerge(
            str(self.historical_path), str(self.new_data_path), str(self.output_path)
        )
        merged_df = merger.apply()
        self.assertEqual(len(merged_df), 1)
