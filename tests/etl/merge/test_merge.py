import pandas as pd
import pytest

from src.core.merge import Merge


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
