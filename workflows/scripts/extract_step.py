from workflows.scripts.base import ETLStep
from src.core.extraction import Extract
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ExtractStep(ETLStep):
    def run(self):
        extractor = Extract()
        extractor.apply()