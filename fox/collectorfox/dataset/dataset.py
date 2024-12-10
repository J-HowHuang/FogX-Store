import pyarrow as pa
from transformation import CollectorfoxTransformation
from annotation import CollectorfoxAnnotator
from typing import List, Tuple, Dict

class FoxDatasetDefinition:
    def __init__(self, name, schema):
        self.name: str = name
        self.schema: pa.schema = schema
        self.transformations: Dict[str, CollectorfoxTransformation] = {}
        self.annotators: List[Tuple[str, str, CollectorfoxAnnotator]] = []
        self.schema = self.schema.append(pa.field("_episode_path", pa.string()))
        self.schema = self.schema.append(pa.field("_episode_id", pa.string()))

    def add_transformation(self, src_type, transformation):
        self.transformations[src_type] = transformation
        
    def add_annotators(self, src_col, dest_col, annotator):
        self.annotators.append((src_col, dest_col, annotator))
        self.schema = self.schema.append(pa.field(dest_col, annotator.get_output_type()))
        metadata = self.schema.metadata if self.schema.metadata is not None else {}
        self.schema = self.schema.with_metadata({f"{src_col}_model": annotator.get_annotator_name(), f"{src_col}_column": dest_col, **metadata})
        
    def get_transformation(self, src_type):
        return self.transformations.get(src_type, None)
    
    def get_annotators(self):
        return self.annotators
