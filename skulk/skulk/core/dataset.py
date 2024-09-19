from .schema import SkulkSchema
from dataclasses import dataclass
import pathlib
from datetime import datetime


@dataclass
class DatasetStats:
    row_count: int = 0
    size: int = 0
    recent_access: list[datetime]
    last_update_time: datetime
    create_time: datetime

class Dataset:
    """An abstract dataset class serves as catalog for database to
    lookup while processing query
    """
    def __init__(self, name: str, lance_path: pathlib.Path, metadata_path: pathlib.Path, schema: SkulkSchema) -> None:
        self.name = name
        self.lance_path = lance_path
        self.schema = schema
        self.stats = DatasetStats()
        self.metadata_path = metadata_path
    
    
    