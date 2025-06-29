from abc import ABC, abstractmethod

class ETLStep(ABC):
    @abstractmethod
    def run(self):
        pass
