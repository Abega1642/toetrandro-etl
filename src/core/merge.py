from pathlib import Path

import pandas as pd

from src.core.base import Process
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Merge(Process):
    def __init__(self):
        base_dir = Path(__file__).resolve().parents[2]
        self.output_file = base_dir / "data" / "merged" / "all_weather_data.csv"
        self.input_dir = base_dir / "data" / "processed"
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def apply(self):
        logger.info("🔄 Starting merge process...")

        all_files = list(self.input_dir.glob("**/*.csv"))
        logger.info(f"📂 Found {len(all_files)} files to merge.")

        df_list = []
        for file in all_files:
            try:
                df = pd.read_csv(file, encoding="utf-8")
                df_list.append(df)
            except Exception as e:
                logger.error(f"❌ Failed to read {file}: {e}")

        if not df_list:
            logger.warning("⚠️ No files read successfully. Exiting merge.")
            return

        merged_df = pd.concat(df_list, ignore_index=True)
        logger.info(
            f"📊 Merged {len(all_files)} files with total {len(merged_df)} rows before cleanup."
        )

        merged_df.dropna(subset=["city", "timestamp"], inplace=True)
        logger.info(
            f"🧹 Dropped rows with missing city/timestamp. Remaining: {len(merged_df)}"
        )

        merged_df.drop_duplicates(inplace=True)
        logger.info(f"✨ Dropped duplicate rows. Remaining: {len(merged_df)}")

        try:
            merged_df["timestamp"] = pd.to_datetime(
                merged_df["timestamp"], errors="coerce"
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not convert 'timestamp' to datetime: {e}")

        merged_df.sort_values(by=["city", "timestamp"], inplace=True)

        if merged_df.empty:
            logger.warning(
                "No valid rows after cleanup. Merged file will not be written."
            )
            return

        try:
            merged_df.to_csv(self.output_file, index=False, encoding="utf-8")
            logger.info(f"✅ Successfully saved merged data → {self.output_file}")
        except Exception as e:
            logger.error(f"💥 Failed to save merged data: {e}")
