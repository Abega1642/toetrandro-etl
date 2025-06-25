from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from src.etl.transform import Transform


class TestTransform:

    @pytest.fixture
    def dummy_raw_df(self):
        return pd.DataFrame({
            'temp_C': [25, 30, None],
            'rain_1d': [0, 5, 0],
            'wind_speed': [3, 6, 2],
            'humidity': [50, 85, 60],
            'timestamp': ["2024-05-01", "2024-05-02", "2024-05-03"]
        })

    def test_transform_creates_expected_columns(self, dummy_raw_df):
        transformer = Transform()
        df_transformed = transformer.transform_dataframe(dummy_raw_df)

        expected_cols = [
            'is_ideal_temp', 'is_low_rain', 'is_low_wind',
            'comfort_score', 'is_ideal_day', 'month', 'year', 'day_of_week'
        ]

        for col in expected_cols:
            assert col in df_transformed.columns

    def test_transform_handles_missing_values_gracefully(self, dummy_raw_df):
        transformer = Transform()
        df_cleaned = transformer.transform_dataframe(dummy_raw_df)
        assert not df_cleaned.isnull().any().any()

    def test_comfort_score_with_expected_weights(self):
        df = pd.DataFrame({
            'temp_C': [24],
            'rain_1d': [0],
            'wind_speed': [3],
            'humidity': [50],
            'timestamp': ["2024-05-01"]
        })
        transformer = Transform()
        result = transformer.transform_dataframe(df)
        assert result.iloc[0]['comfort_score'] == pytest.approx(1.0)

    def test_transform_sets_correct_is_ideal_day_flag(self):
        df = pd.DataFrame({
            'temp_C': [24],
            'rain_1d': [0],
            'wind_speed': [3],
            'humidity': [60],
            'timestamp': ["2024-05-01"]
        })
        transformer = Transform()
        result = transformer.transform_dataframe(df)
        assert result.iloc[0]['is_ideal_day'] == True

    def test_transform_apply_logs_and_saves(self, tmp_path):
        with patch("src.etl.transform.get_now") as mock_get_now:
            mock_get_now.return_value = datetime(2024, 5, 1)
            date_str = "2024-05-01"

            input_dir = tmp_path / "input" / date_str
            output_dir = tmp_path / "output"
            input_dir.mkdir(parents=True)

            file_path = input_dir / "test_city.csv"

            pd.DataFrame({
                'temp_C': [25],
                'rain_1d': [0],
                'wind_speed': [3],
                'humidity': [50],
                'timestamp': ["2024-05-01"]
            }).to_csv(file_path, index=False)

            transform = Transform(input_dir=tmp_path / "input", output_dir=output_dir)
            transform.apply()

            result_dir = output_dir / date_str
            result_files = list(result_dir.glob("*.csv"))
            assert result_files, "No transformed file was saved."



