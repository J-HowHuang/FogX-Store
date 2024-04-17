from dataclasses import dataclass
from typing import Optional


@dataclass
class VectorQuery:
    vector: list[float]
    top_k: int
    
    def format(self):
        return f'search top_{self.top_k} for [{", ".join(self.vector[:5])}{"..." if len(self.vector) > 5 else ""}]'

@dataclass
class DatasetQuery:
    target_dataset_name: str
    return_columns: list[str]
    predicate: Optional[str] = None
    # if present with predicate, search for top_k similar rows satisfying predicates
    vector_query: Optional[VectorQuery] = None

    def format(self):
        format_str = f'select {",".join(self.return_columns) if len(self.return_columns) else "*"} from {self.target_dataset_name}{f" where {self.predicate}" if self.predicate else ""} {self.vector_query.format() if self.vector_query else ""}'
        return format_str