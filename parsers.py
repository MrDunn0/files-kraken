from abc import ABC, abstractmethod


class DataParser(ABC):
    @abstractmethod
    def parse(self):
        pass


class RunDateParser(DataParser):
    def parse(self, run_folder):
        pass