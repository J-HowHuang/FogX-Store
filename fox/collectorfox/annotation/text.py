from .base_annotator import CollectorfoxAnnotator
from fastembed import TextEmbedding
import pyarrow as pa
import os

class TextAnnotator(CollectorfoxAnnotator):
    def __init__(self, model_name):
        self.embedder = TextEmbedding(model_name=model_name, cache_dir=os.getenv("CACHE_DIR"))
    
    def annotate(self, input_data):
        return list(self.embedder.embed(input_data))
    
    def get_annotator_name(cls):
        return "sentence-transformers/all-MiniLM-L6-v2"
    
    def get_output_type(cls):
        return pa.list_(pa.float32(), 384)
    