from abc import ABC, abstractmethod


class Process(ABC):

    @abstractmethod
    def apply(self, *args, **kwargs):
        pass
