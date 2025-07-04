from src.core.final_merge import FinalMerge
from src.core.merge import Merge
from workflows.scripts.base import ETLStep


class MergeStep(ETLStep):
    def __init__(self, execution_date):
        self.execution_date = execution_date

    def run(self):
        merger = Merge()
        final_merge = FinalMerge()
        merger.apply()
        final_merge.apply()
