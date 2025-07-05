import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.core.merge import Merge


class TestMerge(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "data" / "processed"
        self.output_file = self.temp_dir / "data" / "merged" / "all_weather_data.csv"
        self.input_dir.mkdir(parents=True)
        self.output_file.parent.mkdir(parents=True)

        self.merge = Merge()
        self.merge.input_dir = self.input_dir
        self.merge.output_file = self.output_file

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_merge_combines_all_city_files(self):
        for day in ["2024-01-01", "2024-01-02"]:
            day_dir = self.input_dir / day
            day_dir.mkdir()
            df = pd.DataFrame(
                {
                    "city": ["Paris"],
                    "timestamp": [f"{day} 12:00:00"],
                    "temp_C": [25],
                    "humidity": [55],
                }
            )
            df.to_csv(day_dir / "Paris.csv", index=False)

        self.merge.apply()
        self.assertTrue(self.output_file.exists())
        merged_df = pd.read_csv(self.output_file)
        self.assertEqual(len(merged_df), 2)

    def test_merge_skips_empty_files(self):
        empty_dir = self.input_dir / "2024-01-01"
        empty_dir.mkdir()
        (empty_dir / "Empty.csv").write_text("")

        self.merge.apply()
        self.assertFalse(self.output_file.exists())

    def test_merge_deduplicates_by_city_and_timestamp(self):
        day_dir = self.input_dir / "2024-01-01"
        day_dir.mkdir()
        df = pd.DataFrame(
            {
                "city": ["Paris", "Paris"],
                "timestamp": ["2024-01-01 12:00:00"] * 2,
                "temp_C": [25, 25],
                "humidity": [55, 55],
            }
        )
        df.to_csv(day_dir / "Paris.csv", index=False)

        self.merge.apply()
        merged_df = pd.read_csv(self.output_file)
        self.assertEqual(len(merged_df), 1)

    def test_merge_drops_rows_with_missing_city_or_timestamp(self):
        day_dir = self.input_dir / "2024-01-01"
        day_dir.mkdir()
        df = pd.DataFrame(
            {
                "city": ["Paris"],
                "timestamp": [None],
                "temp_C": [25],
                "humidity": [55],
            }
        )
        df.to_csv(day_dir / "Paris.csv", index=False)

        self.merge.apply()
        self.assertFalse(self.output_file.exists())

    def test_merge_handles_missing_input_dir_gracefully(self):
        self.merge.input_dir = self.temp_dir / "nonexistent"
        self.merge.apply()
        self.assertFalse(self.output_file.exists())


if __name__ == "__main__":
    unittest.main()
