from abc import ABC, abstractmethod


class ETLStep(ABC):

    @abstractmethod
    def apply(self, *args, **kwargs):
        pass
