from pathlib import Path

import pandas as pd

from src.core.base import Process
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FinalMerge(Process):

    def __init__(
        self,
        historical_path: str = None,
        new_data_path: str = None,
        output_path: str = None,
    ):
        base_dir = Path(__file__).resolve().parents[2]

        self.historical_path = (
            Path(historical_path)
            if historical_path
            else base_dir / "data" / "historical" / "cleaned_historical_data.csv"
        )
        self.new_data_path = (
            Path(new_data_path)
            if new_data_path
            else base_dir / "data" / "merged" / "all_weather_data.csv"
        )
        self.output_path = (
            Path(output_path)
            if output_path
            else base_dir / "data" / "merged" / "ready_data.csv"
        )

    def apply(self) -> pd.DataFrame:
        logger.info("🔄 FinalMerge: Starting merge process.")
        merged_df = self._merge()
        logger.info(
            f"✅ FinalMerge: Merge completed. Final dataset contains {len(merged_df)} rows."
        )
        return merged_df

    def _merge(self) -> pd.DataFrame:
        logger.info(f"📂 Reading historical data from: {self.historical_path}")
        historical_df = pd.read_csv(
            self.historical_path,
            parse_dates=["timestamp", "sunrise", "sunset", "extracted_at"],
        )
        logger.info(f"📊 Historical data loaded: {len(historical_df)} rows")

        logger.info(f"📂 Reading new extracted data from: {self.new_data_path}")
        new_df = pd.read_csv(
            self.new_data_path,
            parse_dates=["timestamp", "sunrise", "sunset", "extracted_at"],
        )
        logger.info(f"📊 New data loaded: {len(new_df)} rows")

        if not historical_df.columns.equals(new_df.columns):
            logger.error("❌ Schema mismatch between historical and new data.")
            raise ValueError("Schema mismatch: columns do not align.")

        logger.info(
            "🔗 Concatenating datasets and removing duplicates based on ['city', 'timestamp']"
        )
        combined_df = pd.concat([historical_df, new_df], ignore_index=True)
        before_dedup = len(combined_df)
        combined_df.drop_duplicates(
            subset=["city", "timestamp"], keep="last", inplace=True
        )
        after_dedup = len(combined_df)
        logger.info(f"🧹 Removed {before_dedup - after_dedup} duplicate rows")

        combined_df.sort_values(by=["city", "timestamp"], inplace=True)

        if self.output_path == self.historical_path:
            backup_path = self.historical_path.with_suffix(".bak.csv")
            historical_df.to_csv(backup_path, index=False)
            logger.info(f"📦 Backed up historical file to: {backup_path}")

        logger.info(f"💾 Saving merged dataset to: {self.output_path}")
        combined_df.to_csv(self.output_path, index=False)

        return combined_df

    def commit(self):
        if self.output_path != self.historical_path:
            logger.info(f"📥 Committing merged data to: {self.historical_path}")
            pd.read_csv(self.output_path).to_csv(self.historical_path, index=False)
            logger.info("✅ Commit complete.")
        else:
            logger.warning(
                "⚠️ Output path is already the historical path. Nothing to commit."
            )
