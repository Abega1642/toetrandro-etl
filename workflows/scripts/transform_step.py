from src.core.transform import Transform
from workflows.scripts.base import ETLStep


class TransformStep(ETLStep):
    def run(self):
        transform = Transform()
        transform.apply()
