from pathlib import Path

import pandas as pd

from src.etl.base import ETLStep
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Merge(ETLStep):
    def __init__(
        self, input_dir="data/processed", output_file="data/merged/all_weather_data.csv"
    ):
        self.input_dir = Path(input_dir)
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def apply(self):
        logger.info("Starting merge process...")
        input_path = Path(self.input_dir)
        all_files = list(input_path.glob("**/*.csv"))
        logger.info(f"Found {len(all_files)} files to merge.")

        df_list = []
        for file in all_files:
            try:
                df = pd.read_csv(file)
                df_list.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file}: {e}")

        if not df_list:
            logger.warning("No files read successfully. Exiting merge.")
            return

        merged_df = pd.concat(df_list, ignore_index=True)
        logger.info(
            f"Merged {len(all_files)} files with total {len(merged_df)} rows before cleanup."
        )

        merged_df.dropna(subset=["city", "timestamp"], inplace=True)
        logger.info(
            f"Dropped rows with missing city or timestamp. Remaining rows: {len(merged_df)}"
        )

        merged_df.drop_duplicates(inplace=True)
        logger.info(f"Dropped duplicate rows. Remaining rows: {len(merged_df)}")

        if merged_df.empty:
            logger.warning(
                "No valid rows to save after cleanup. Merged file will not be written."
            )
            return

        try:
            merged_df.to_csv(self.output_file, index=False)
            logger.info(f"Successfully saved merged data → {self.output_file}")
        except Exception as e:
            logger.error(f"Failed to save merged data: {e}")
