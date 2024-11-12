from annotator import CollectorfoxAnnotator
from fastembed import TextEmbedding
import numpy as np

class TextAnnotator(CollectorfoxAnnotator):
    def __init__(self, model_name):
        self.embedder = TextEmbedding(model_name=model_name)
    
    def annotate(self, input_data):
        return list(self.embedder.embed(input_data))
    
    def get_input_type(cls):
        return str
    
    def get_output_type(cls):
        return np.ndarray
    