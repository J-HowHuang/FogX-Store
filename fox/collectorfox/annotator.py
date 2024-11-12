from abc import ABC, abstractmethod
import pyarrow as pa
import uuid

class CollectorfoxAnnotator(ABC):
    @abstractmethod
    def annotate(self, input_data):
        pass
    
    @abstractmethod
    def get_output_type(cls):
        return type(None)
    
    @abstractmethod
    def get_input_type(cls):
        return type(None)