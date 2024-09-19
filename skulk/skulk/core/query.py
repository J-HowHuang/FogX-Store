from dataclasses import dataclass
from typing import Optional
import os
import json


@dataclass
class VectorQuery:
    column: str
    vector: list[float]
    top_k: int

    def to_sql(self):
        return f'ORDER BY array_distance({self.column}, [{", ".join(self.vector)}]::FLOAT[{len(self.vector)}]) LIMIT {self.top_k}'

    def serialize(self) -> bytes:
        return json.dumps(self.__dict__)
    
    def deserialize(serialized):
        return VectorQuery(serialized['column'], serialized['vector'], serialized['top_k'])

@dataclass
class SkulkQuery:
    dataset: str
    return_columns: list[str]
    predicate: Optional[str] = None
    # if present with predicate, search for top_k similar rows satisfying predicates
    vector_query: Optional[VectorQuery] = None

    def to_sql(self, dataset_dir):
        format_str = f'SELECT {",".join(self.return_columns) if len(self.return_columns) else "*"} FROM \'{os.path.join(dataset_dir, f"{self.dataset}.parquet")}\'{f" WHERE {self.predicate}" if self.predicate else ""} {self.vector_query.to_sql() if self.vector_query else ""};'
        return format_str

    def serialize(self) -> bytes:
        data = {
            "dataset": self.dataset,
            "return_columns": self.return_columns,
        }
        if self.predicate:
            data["predicate"] = self.predicate
        if self.vector_query:
            data["vector_query"] = self.vector_query.serialize()
        return json.dumps(data)
        
    def deserialize(serialized):
        _dict = json.loads(serialized)
        return SkulkQuery(
            _dict['dataset'],
            _dict['return_columns'],
            predicate=_dict.get('predicates', None),
            vector_query=VectorQuery.deserialize(_dict['vector_query']) if "vector_query" in _dict else None
        )