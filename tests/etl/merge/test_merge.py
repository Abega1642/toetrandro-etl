import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pytest

from src.etl.merge import Merge


class TestMerge:

    @pytest.fixture
    def create_processed_files(self, tmp_path):
        base_dir = tmp_path / "processed"
        base_dir.mkdir()
        for day in ["2024-01-01", "2024-01-02"]:
            day_dir = base_dir / day
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
        return base_dir

    def test_merge_combines_all_city_files(self, create_processed_files, tmp_path):
        merge = Merge(
            input_dir=create_processed_files, output_file=tmp_path / "merged.csv"
        )
        merge.apply()
        assert (tmp_path / "merged.csv").exists()

    def test_merge_skips_empty_files(self, tmp_path):
        directory = tmp_path / "processed"
        subdir = directory / "2024-01-01"
        subdir.mkdir(parents=True)
        empty_file = subdir / "Empty.csv"
        empty_file.write_text("")

        merge = Merge(input_dir=directory, output_file=tmp_path / "out.csv")
        merge.apply()

        assert (tmp_path / "out.csv").exists() is False

    def test_merge_deduplicates_by_city_and_timestamp(self, tmp_path):
        day = "2024-01-01"
        directory = tmp_path / "processed" / day
        directory.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            {
                "city": ["Paris", "Paris"],
                "timestamp": [f"{day} 12:00:00"] * 2,
                "temp_C": [25, 25],
                "humidity": [55, 55],
            }
        )
        df.to_csv(directory / "Paris.csv", index=False)

        merge = Merge(
            input_dir=tmp_path / "processed", output_file=tmp_path / "merged.csv"
        )
        merge.apply()

        merged_df = pd.read_csv(tmp_path / "merged.csv")
        assert len(merged_df) == 1

    def test_merge_logs_missing_input_dir(self, tmp_path):
        missing_path = tmp_path / "not_there"
        merge = Merge(input_dir=missing_path, output_file=tmp_path / "merged.csv")
        merge.apply()
        assert not (tmp_path / "merged.csv").exists()

    def test_merge_drop_invalid_rows(self, tmp_path):
        directory = tmp_path / "processed" / "2024-01-01"
        directory.mkdir(parents=True)
        df = pd.DataFrame(
            {"city": ["Paris"], "timestamp": [None], "temp_C": [None], "humidity": [55]}
        )
        df.to_csv(directory / "Paris.csv", index=False)

        merge = Merge(
            input_dir=tmp_path / "processed", output_file=tmp_path / "merged.csv"
        )
        merge.apply()

        assert not (tmp_path / "merged.csv").exists()


class TestMergeIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.processed_dir = Path(self.temp_dir, "processed")
        self.output_file = Path(self.temp_dir, "merged", "all_weather_data.csv")
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def write_csv(self, date_folder, filename, data):
        path = self.processed_dir / date_folder
        path.mkdir(parents=True, exist_ok=True)
        (path / filename).write_text(data, encoding="utf-8")

    def test_merge_single_valid_csv(self):
        content = "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n"
        self.write_csv("2025-06-28", "Tokyo.csv", content)
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        self.assertTrue(self.output_file.exists())
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)

    def test_merge_multiple_valid_csvs(self):
        rows = [
            "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n",
            "city,timestamp,temp_C\nOsaka,2025-06-28,27.0\n",
        ]
        for i, content in enumerate(rows):
            self.write_csv("2025-06-28", f"city_{i}.csv", content)

        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 2)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_skips_missing_headers(self):
        content = "not,a,real,csv\n123,456,789,000"
        self.write_csv("2025-06-28", "bad.csv", content)
        self.write_csv(
            "2025-06-28", "ok.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n"
        )
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file)

        self.assertEqual(len(df), 1)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_skips_files_missing_required_columns(self):
        content = "timestamp,temp_C\n2025-06-28,25.0\n"
        self.write_csv("2025-06-28", "NoCity.csv", content)
        self.write_csv(
            "2025-06-28", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,27.0\n"
        )

        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)
        self.assertIn("Tokyo", df["city"].values)

    def test_merge_drops_duplicates(self):
        content = (
            "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\nTokyo,2025-06-28,25.0\n"
        )
        self.write_csv("2025-06-28", "Tokyo.csv", content)
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 1)

    def test_merge_with_missing_values(self):
        content = "city,timestamp,temp_C\n,2025-06-28,25.0\nTokyo,,27.0\n"
        self.write_csv("2025-06-28", "Missing.csv", content)
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        self.assertFalse(self.output_file.exists())

    def test_merge_multiple_days(self):
        self.write_csv(
            "2025-06-28", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n"
        )
        self.write_csv(
            "2025-06-29", "Osaka.csv", "city,timestamp,temp_C\nOsaka,2025-06-29,26.0\n"
        )
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file)
        self.assertEqual(len(df), 2)
        self.assertSetEqual(set(df["city"]), {"Tokyo", "Osaka"})

    def test_merge_sorts_by_timestamp(self):
        self.write_csv(
            "day1", "a.csv", "city,timestamp,temp_C\nOsaka,2025-06-29,26.0\n"
        )
        self.write_csv(
            "day2", "b.csv", "city,timestamp,temp_C\nOsaka,2025-06-27,24.0\n"
        )
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        df = pd.read_csv(self.output_file, parse_dates=["timestamp"])
        self.assertTrue(df["timestamp"].is_monotonic_increasing)

    def test_merge_handles_mixed_encodings(self):
        path = self.processed_dir / "mixed"
        path.mkdir()
        (path / "utf8.csv").write_text(
            "city,timestamp,temp_C\nAntananarivo,2025-06-28,23.5\n", encoding="utf-8"
        )
        Merge(input_dir=self.processed_dir, output_file=self.output_file).apply()
        self.assertTrue(self.output_file.exists())

    def test_merge_creates_output_folder_if_missing(self):
        output_path = Path(self.temp_dir, "new_merged_dir", "final.csv")
        self.write_csv(
            "today", "Tokyo.csv", "city,timestamp,temp_C\nTokyo,2025-06-28,25.0\n"
        )
        Merge(input_dir=self.processed_dir, output_file=output_path).apply()
        self.assertTrue(output_path.exists())

if __name__ == "__main__":
    unittest.main()
