from abc import ABC, abstractmethod
import pyarrow as pa

class CollectorfoxTransformation(ABC):
    @abstractmethod
    def get_iterator(self, src_path: str):
        pass
    
    def __iter__(self):
        return self
    

    """
    Returns the next row of episode metadata and the step data.

    This method should be implemented by subclasses to define the behavior
    of the iterator when retrieving the next item.

    Returns:
        tuple[dict, pa.Table]: A tuple containing a row in dictionary and step data in PyArrow Table.
    """
    @abstractmethod
    def __next__(self) -> tuple[dict, pa.Table]:
        return self.__iter__()
