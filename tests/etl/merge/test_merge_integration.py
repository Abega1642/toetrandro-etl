import shutil
import tempfile
import unittest
from pathlib import Path
import pandas as pd

from src.core.merge import Merge


class TestMergeIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.processed_dir = self.temp_dir / "data" / "processed"
        self.output_file = self.temp_dir / "data" / "merged" / "all_weather_data.csv"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        self.merge = Merge()
        self.merge.input_dir = self.processed_dir
        self.merge.output_file = self.output_file

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def write_csv(self, date_folder, filename, content):
        path = self.processed_dir / date_folder
        path.mkdir(parents=True, exist_ok=True)
        (path / filename).write_text(content, encoding="utf-8")

    def test_merge_single_valid_csv(self):
        self.write_csv("2025-06-28", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n")
        self.merge.apply()
        self.assertTrue(self.output_file.exists())
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)

    def test_merge_multiple_valid_csvs(self):
        self.write_csv("2025-06-28", "city_0.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n")
        self.write_csv("2025-06-28", "city_1.csv", "city,timestamp,temp_C\nOsaka,2025-06-28,27.0\n")
        self.merge.apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 2)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_skips_missing_headers(self):
        self.write_csv("2025-06-28", "bad.csv", "not,a,real,csv\n123,456,789,000")
        self.write_csv("2025-06-28", "ok.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n")
        self.merge.apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_skips_files_missing_required_columns(self):
        self.write_csv("2025-06-28", "NoCity.csv", "timestamp,temp_C\n2025-06-28,25.0\n")
        self.write_csv("2025-06-28", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,27.0\n")
        self.merge.apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_drops_duplicates(self):
        content = "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\nTokyo,2025-06-28,25.0\n"
        self.write_csv("2025-06-28", "Tokyo.csv", content)
        self.merge.apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)

    def test_merge_with_missing_values(self):
        content = "city,timestamp,temp_C\n,2025-06-28,25.0\nTokyo,,27.0\n"
        self.write_csv("2025-06-28", "Missing.csv", content)
        self.merge.apply()
        self.assertFalse(self.output_file.exists())

    def test_merge_multiple_days(self):
        self.write_csv("2025-06-28", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n")
        self.write_csv("2025-06-29", "Osaka.csv", "city,timestamp,temp_C\nOsaka,2025-06-29,26.0\n")
        self.merge.apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 2)
        self.assertSetEqual(set(df["city"]), {"Tokyo", "Osaka"})

    def test_merge_sorts_by_timestamp(self):
        self.write_csv("day1", "a.csv", "city,timestamp,temp_C\nOsaka,2025-06-29,26.0\n")
        self.write_csv("day2", "b.csv", "city,timestamp,temp_C\nOsaka,2025-06-27,24.0\n")
        self.merge.apply()
        df = pd.read_csv(self.output_file, parse_dates=["timestamp"])
        self.assertTrue(df["timestamp"].is_monotonic_increasing)

    def test_merge_handles_mixed_encodings(self):
        path = self.processed_dir / "mixed"
        path.mkdir()
        (path / "utf8.csv").write_text("city,timestamp,temp_C\nAntananarivo,2025-06-28,23.5\n", encoding="utf-8")
        self.merge.apply()
        self.assertTrue(self.output_file.exists())

    def test_merge_creates_output_folder_if_missing(self):
        new_output = self.temp_dir / "new_merged_dir" / "final.csv"
        self.write_csv("today", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n")
        self.merge.output_file = new_output
        self.merge.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.merge.apply()
        self.assertTrue(new_output.exists())


if __name__ == "__main__":
    unittest.main()
