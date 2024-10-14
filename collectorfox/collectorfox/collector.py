from flask import Flask, jsonify, request
import tensorflow as tf
import lance
import pyarrow as pa
from collections import defaultdict
import tensorflow_datasets as tfds
import lancedb
import json
import requests
import base64


# The number of episodes to read from the GCS dataset (For testing only)
GCS_TOP_K = 10

app = Flask(__name__)


# add a new dataset with post "" request
def add_new_dataset_catalog(dataset: str, schema: pa.Schema):
    serialized_schema = schema.serialize().to_pybytes()
    url = "http://localhost:11632/dataset/" + dataset
    response = requests.post(url, data=serialized_schema)
    if response.status_code != 200:
        raise Exception("Failed to add dataset to the dataset catalog")


# add a new location to the dataset catalog with post "" request
def add_new_location_to_dataset(location: str, dataset: str):
    url = "http://localhost:11632/dataset/" + dataset + "/add"
    response = requests.post(url, data=location)
    if response.status_code != 200:
        raise Exception("Failed to add location to the dataset catalog")
    
# create a new dataset table with the given schema
@app.route('/create', methods=['POST'])
def create_table():
    try:
        uri = request.json.get('uri') # get the uri from the request
        dataset = request.json.get('dataset') # get the dataset name from the request
        encoded_schema = request.json.get('schema') # get the schema from the request
        # deserialize the schema
        decoded_schema = base64.b64decode(encoded_schema)
        # Deserialize the schema using pyarrow
        schema = pa.ipc.read_schema(pa.BufferReader(decoded_schema))
        # connect to the lancedb with the given uri
        db = lancedb.connect(uri)
        #  create a table named as dataset
        db.create_table(dataset, schema=schema)
        add_new_dataset_catalog(dataset, schema) # add the dataset to the dataset catalog
        add_new_location_to_dataset(uri, dataset) # add the location to the dataset catalog
        return jsonify({"message": "Database created successfully"}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/write', methods=['POST'])
def add_data_to_lancedb(): 
    try:
        uri = request.json.get('uri')
        dataset = request.json.get('dataset')
        ds_path = request.json.get('ds_path') 
        
        # connect to the lancedb with the given uri
        db = lancedb.connect(uri)
        #  read table from the lancedb with the given uri
        tbl = db.open_table(dataset)

        if ds_path.startswith('gs://'): # if the dataset is stored in GCS
            print("Reading dataset from GCS")
            b = tfds.builder_from_directory(builder_dir=ds_path)
            ds = b.as_dataset(split='train[:{}]'.format(GCS_TOP_K))
        else: # TODO if the dataset is stored locally
            print("Reading dataset from local")
            ds = tf.data.Dataset.load(ds_path)
        
        print("Writing dataset to LanceDB")
        for episode in ds:
            episode_table = dict()
            print(episode['episode_metadata'])
            for key, value in episode['episode_metadata'].items():
                print(key, value)
                value = value.numpy()
                if isinstance(value, bytes): # serialize the bytes tensor
                    value = str(value)
                episode_table[key] = [value]
            arrow_row = pa.table(episode_table)
            tbl.add(arrow_row)
        
        return jsonify({"message": "Dataset written successfully"}), 200
    
    except Exception as e:
        return jsonify({"message": str(e)}), 500


if __name__ == '__main__':
    app.run()
    

