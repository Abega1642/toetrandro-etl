from src.core.migration import Migration
from workflows.scripts.base import ETLStep

from src.utils.logger import get_logger

logger = get_logger(__name__)

class MigrationStep(ETLStep):
    def __init__(self, db_config):
        self.db_config = db_config
        self.migration = Migration(db_config)

    def run(self):
        logger.info("Starting MigrationStep...")
        try:
            self.migration.apply()
            logger.info("MigrationStep completed successfully.")
        except Exception as e:
            logger.error(f"MigrationStep failed: {e}")
            raise