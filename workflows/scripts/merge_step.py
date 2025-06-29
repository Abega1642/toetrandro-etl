from workflows.scripts.base import ETLStep
from src.core.merge import Merge


class MergeStep(ETLStep):
    def __init__(self, execution_date):
        self.execution_date = execution_date

    def run(self):
        merger = Merge()
        merger.apply()
