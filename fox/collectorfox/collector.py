from flask import Flask
from typing import Type, Dict, Tuple, List
from dataset import FoxDatasetDefinition
import pyarrow as pa
import lancedb
import os
import sys
import logging
import uuid
import requests

# The number of episodes to read from the GCS dataset (For testing only)
GCS_TOP_K = 5
# Get environment variables
PARQUET_PATH = os.getenv('PARQUET_PATH', './../../_datasets/parquet')  # Default to 'localhost' if not set
SKULK_IP_ADDR = os.getenv('SKULK_IP_ADDR') # get the skulk ip address from the environment variable
HOST_IP_ADDR = os.getenv("HOST_IP_ADDR", "0.0.0.0")
ADVERTISE_IP_ADDR = os.getenv("ADVERTISE_IP_ADDR")
lance_path = "./data/dataset_db"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class CollectorFox:
    def __init__(self, lance_path, step_data_dir, skulk_ip_addr, my_ip_addr):
        self.flask_app = Flask(__name__)
        self.lance_path = lance_path
        self.step_data_dir = step_data_dir
        self.skulk_ip_addr = skulk_ip_addr
        self.my_ip_addr = my_ip_addr
        self.datasets: Dict[str, FoxDatasetDefinition] = {}
        
    def run(self):
        logger.info("Starting CollectorFox...")
        self.flask_app.run(host=os.environ.get("HOST_IP_ADDR", "0.0.0.0"), port=11635)

    def route(self):
        return self.flask_app.route
    
    def register_dataset(self, dataset: FoxDatasetDefinition):
        name = dataset.name
        schema = dataset.schema
        
        # create directory for storing parquet files
        os.makedirs(f"{collectorfox.step_data_dir}/{name}", exist_ok=True)
        
        # create a new table with the given schema
        db = lancedb.connect(collectorfox.lance_path)
        db.create_table(name, schema=schema, exist_ok=True)
        logger.info(f"Dataset '{name}' registered. Schema:\n{schema}")
        
        serialized_schema = schema.serialize().to_pybytes()
        # add the dataset to the skulk catalog
        try:
            url = f"http://{self.skulk_ip_addr}:11632/dataset/{name}"
            response = requests.post(url, data=serialized_schema)
            logger.info(f"response from skulk: {response.text}")
            if response.status_code != 200:
                logger.error(f"Failed to create dataset '{name}' in Skulk")
        except Exception as e:
            logger.error(f"Failed to create dataset '{name}' in Skulk: {e}")
            
        
        # add the location to the skulk catalog
        try:
            url = f"http://{self.skulk_ip_addr}:11632/dataset/{name}/add"
            response = requests.post(url, data=self.my_ip_addr)
            if response.status_code != 200:
                logger.error(f"Failed to add this predator '{self.my_ip_addr}' to dataset '{name}' to Skulk")
            logger.info(f"response from skulk: {response.text}")
        except Exception as e:
            logger.error(f"Failed to add this predator '{self.my_ip_addr}' to dataset '{name}' to Skulk: {e}")
            
        self.datasets[name] = dataset
            
collectorfox = CollectorFox(lance_path, PARQUET_PATH, SKULK_IP_ADDR, ADVERTISE_IP_ADDR)