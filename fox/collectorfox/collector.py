from flask import Flask
from typing import Type, Dict, Tuple, List
from transformation import CollectorfoxTransformation
from annotator import CollectorfoxAnnotator
import os

# The number of episodes to read from the GCS dataset (For testing only)
GCS_TOP_K = 5
# Get environment variables
PARQUET_PATH = os.getenv('PARQUET_PATH', './../../_datasets/parquet')  # Default to 'localhost' if not set
SKULK_IP_ADDR = os.getenv('SKULK_IP_ADDR') # get the skulk ip address from the environment variable
HOST_IP_ADDR = os.getenv("HOST_IP_ADDR", "0.0.0.0")
lance_path = "./data/dataset_db"



class CollectorFox:
    def __init__(self, lance_path, step_data_dir, skulk_ip_addr, my_ip_addr):
        self.flask_app = Flask(__name__)
        self.lance_path = lance_path
        self.step_data_dir = step_data_dir
        self.skulk_ip_addr = skulk_ip_addr
        self.my_ip_addr = my_ip_addr
        self.embed_models = {}
        self.transformations: Dict[Tuple[str, str], CollectorfoxTransformation] = {}
        self.annotators: Dict[str, List[Tuple[str, str, CollectorfoxAnnotator]]] = {}
        
    def run(self):
        self.flask_app.run(host=os.environ.get("HOST_IP_ADDR", "0.0.0.0"), port=11635)

    def route(self):
        return self.flask_app.route
    
    def register_transformation(self, dataset, src_type, transformation):
        self.transformations[(dataset, src_type)] = transformation
        
    def register_annotators(self, dataset, src_col, dest_col, annotator):
        self.annotators[dataset] = self.annotators.get(dataset, []) + [(src_col, dest_col, annotator)]

collectorfox = CollectorFox(lance_path, PARQUET_PATH, SKULK_IP_ADDR, HOST_IP_ADDR)