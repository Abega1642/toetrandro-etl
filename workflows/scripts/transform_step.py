from workflows.scripts.base import ETLStep
from src.core.transform import Transform


class TransformStep(ETLStep):
    def run(self):
        transform = Transform()
        transform.apply()
