from .schema import Schema
from dataclasses import dataclass


@dataclass
class Dataset:
    """An abstract dataset class serves as catalog for database to
    lookup while processing query
    """

    name: str
    path: str
    schema: Schema
