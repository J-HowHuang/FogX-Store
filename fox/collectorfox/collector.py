from flask import Flask, jsonify, request
import tensorflow as tf
import pyarrow as pa
import numpy as np
from collections import defaultdict
import tensorflow_datasets as tfds
import lancedb
import os
import requests
import base64
import uuid
import pyarrow.parquet as pq
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from fastembed import TextEmbedding

# The number of episodes to read from the GCS dataset (For testing only)
GCS_TOP_K = 500
# Get environment variables
PARQUET_PATH = os.getenv('PARQUET_PATH', './../../_datasets/parquet')  # Default to 'localhost' if not set
SKLK_IP_ADDR = os.environ.get('SKULK_IP_ADDR') # get the skulk ip address from the environment variable
# col to model mapping
COL_TO_MODEL = {
    # example: "language_instruction": "nomic-ai/nomic-embed-text-v1.5"
}

app = Flask(__name__)

def tensor_to_bytes(tensor):
    """
    Convert a tensor to bytes. Assumes the tensor is a NumPy array.
    """
    # Convert tensor to NumPy array if it's not already
    if not isinstance(tensor, np.ndarray):
        tensor = tensor.numpy()  # Assuming it's a TensorFlow or PyTorch tensor
    return tensor.tobytes()

# Use uuid for episode index
def get_episode_index_for_dataset(dataset_name):
    return uuid.uuid4()

# # add a new dataset with post "" request
def add_new_dataset_catalog(dataset: str, schema: pa.Schema):
    serialized_schema = schema.serialize().to_pybytes()
    url = f"http://{SKLK_IP_ADDR}:11632/dataset/" + dataset
    response = requests.post(url, data=serialized_schema)
    if response.status_code != 200:
        raise Exception("Failed to add dataset to the dataset catalog")

# add a new location to the dataset catalog with post "" request
def add_new_location_to_dataset(dataset: str):
    url = f"http://{SKLK_IP_ADDR}:11632/dataset/" + dataset + "/add"
    response = requests.post(url, data=os.environ.get("HOST_IP_ADDR", "0.0.0.0"))
    if response.status_code != 200:
        raise Exception("Failed to add location to the dataset catalog")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"message": "Collector is running"}), 200

# create a new dataset table with the given schema
@app.route('/create', methods=['POST'])
def create_table():
    try:
        uri = request.json.get('uri') # get the uri from the request
        dataset = request.json.get('dataset') # get the dataset name from the request
        encoded_schema = request.json.get('schema') # get the schema from the request
        
        # create directory for storing parquet files
        os.makedirs(f"{PARQUET_PATH}/{dataset}", exist_ok=True)
        # deserialize the schema
        decoded_schema = base64.b64decode(encoded_schema)
        # Deserialize the schema using pyarrow
        schema = pa.ipc.read_schema(pa.BufferReader(decoded_schema))
        # connect to the lancedb with the given uri
        db = lancedb.connect(uri)
        # create col to model mapping
        for col, model_name in schema.metadata.items():
            COL_TO_MODEL[col.decode('utf-8')] = TextEmbedding(model_name=model_name.decode('utf-8'))
        # create a new table with the given schema
        db.create_table(dataset, schema=schema)
        add_new_dataset_catalog(dataset, schema) # add the dataset to the dataset catalog
        add_new_location_to_dataset(dataset) # add the location to the dataset catalog
        return jsonify({"message": "Database created successfully"}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/write', methods=['POST'])
def add_data_to_lancedb(): 
    try:
        uri = request.json.get('uri')
        dataset = request.json.get('dataset')
        ds_path = request.json.get('ds_path') 
        
        # Connect to LanceDB with the given URI
        db = lancedb.connect(uri)
        tbl = db.open_table(dataset)

        if ds_path.startswith('gs://'):  # If the dataset is stored in GCS
            print("Reading dataset from GCS")
            b = tfds.builder_from_directory(builder_dir=ds_path)
            ds = b.as_dataset(split='train[:{}]'.format(GCS_TOP_K))
        else:  # If the dataset is stored locally
            print("Reading dataset from local")
            ds = tf.data.Dataset.load(ds_path)
        
        print("Writing dataset to LanceDB")
        for episode in ds:
            episode_index = get_episode_index_for_dataset(dataset)
            episode_parquet_path = f"{PARQUET_PATH}/{dataset}/{episode_index}/steps.parquet"
            step_data = defaultdict(list)

            episode_table = {}
            for key, value in episode['episode_metadata'].items():
                value = value.numpy()
                if isinstance(value, bytes):
                    value = str(value)
                episode_table[key] = [value]
            
            for step in episode['steps']:
                for key, value in step.items():
                    if key == "observation":
                        image_tensor = value.get("image")
                        image_bytes = tensor_to_bytes(image_tensor)
                        step_data["image"].append(image_bytes)
                    else:
                        value = value.numpy()
                        if isinstance(value, bytes):
                            value = str(value)
                        step_data[key].append(value)

            arrow_table = pa.table(step_data)
            os.makedirs(os.path.dirname(episode_parquet_path), exist_ok=True)
            pq.write_table(arrow_table, episode_parquet_path)
            print(f"Saved episode steps to {episode_parquet_path}")
            
            first_step = next(iter(episode['steps']))
            instruction = first_step.get("language_instruction", None)
            if instruction is not None:
                instruction_text = instruction.numpy().decode('utf-8')
                
                # Embed the instruction using FastEmbed
                embedder = COL_TO_MODEL["language_instruction"]
                vector = list(embedder.embed(instruction_text))
    
                episode_table["language_instruction"] = [instruction_text]
                episode_table["vector"] = vector  # Store as a list for Arrow compatibility
            
            episode_table["episode_path"] = [episode_parquet_path]
            episode_arrow_row = pa.table(episode_table)
            tbl.add(episode_arrow_row)
                    
        return jsonify({"message": "Dataset written successfully"}), 200
    
    except Exception as e:
        return jsonify({"message": str(e)}), 500

if __name__ == '__main__':
    app.run(host=os.environ.get("HOST_IP_ADDR", "0.0.0.0"), port=11635)




