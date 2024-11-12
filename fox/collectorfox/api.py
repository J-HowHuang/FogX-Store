from collector import collectorfox
from flask import jsonify, request
import requests
import lancedb
import base64
import pyarrow as pa
import pyarrow.parquet as pq
import os
import uuid
from fastembed import TextEmbedding
from collections import defaultdict

# Use uuid for episode index
def get_episode_index_for_dataset(dataset_name):
    return uuid.uuid4().hex

# # add a new dataset with post "" request
def add_new_dataset_catalog(dataset: str, schema: pa.Schema):
    serialized_schema = schema.serialize().to_pybytes()
    url = f"http://{collectorfox.skulk_ip_addr}:11632/dataset/" + dataset
    response = requests.post(url, data=serialized_schema)
    if response.status_code != 200:
        raise Exception("Failed to add dataset to the dataset catalog")

# add a new location to the dataset catalog with post "" request
def add_new_location_to_dataset(dataset: str):
    url = f"http://{collectorfox.skulk_ip_addr}:11632/dataset/" + dataset + "/add"
    response = requests.post(url, data=collectorfox.my_ip_addr)
    if response.status_code != 200:
        raise Exception("Failed to add location to the dataset catalog")

@collectorfox.flask_app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"message": "Collector is running"}), 200

# create a new dataset table with the given schema
@collectorfox.flask_app.route('/create', methods=['POST'])
def create_table():
    try:
        dataset = request.json.get('dataset') # get the dataset name from the request
        encoded_schema = request.json.get('schema') # get the schema from the request
        
        # create directory for storing parquet files
        os.makedirs(f"{collectorfox.step_data_dir}/{dataset}", exist_ok=True)
        # deserialize the schema
        decoded_schema = base64.b64decode(encoded_schema)
        # Deserialize the schema using pyarrow
        schema = pa.ipc.read_schema(pa.BufferReader(decoded_schema))
        schema = schema.append(pa.field("_episode_path", pa.string()))
        schema = schema.append(pa.field("_episode_id", pa.string()))
        # connect to the lancedb with the given uri
        # create col to model mapping
        for col, model_name in schema.metadata.items():
            col_name = col.decode('utf-8')[:-6]
            model_name = model_name.decode('utf-8')
            if col_name.endswith("_model"):
                if model_name not in collectorfox.embed_models:
                   collectorfox.embed_models[model_name] = TextEmbedding(model_name=model_name.decode('utf-8'))
        # create a new table with the given schema
        db = lancedb.connect(collectorfox.lance_path)
        print(schema)
        db.create_table(dataset, schema=schema)
        add_new_dataset_catalog(dataset, schema) # add the dataset to the dataset catalog
        add_new_location_to_dataset(dataset) # add the location to the dataset catalog
        return jsonify({"message": "Database created successfully"}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@collectorfox.flask_app.route('/write', methods=['POST'])
def add_data_to_lancedb(): 
    try:
        dataset = request.json.get('dataset') 
        src_uri = request.json.get('src_uri') 
        src_type = request.json.get('src_type')
        
        # Connect to LanceDB with the given URI
        db = lancedb.connect(collectorfox.lance_path)
        tbl = db.open_table(dataset)

        transformation = collectorfox.transformations.get((dataset, src_type), None)
        if transformation is None:
            return jsonify({"message": f"Unknown source type '{src_type}' for dataset '{dataset}', there is no transformation defined for them"}), 400
        
        dataset_iter = transformation.get_iterator(src_uri)
        
        episode_table = defaultdict(list)
        for episode_meta_row, step_data_table in dataset_iter:
            episode_index = get_episode_index_for_dataset(dataset)
            episode_ids = [episode_index] * step_data_table.num_rows
            step_ids = list(range(step_data_table.num_rows))
            step_data_table = step_data_table.append_column("_episode_id", pa.array(episode_ids))
            step_data_table = step_data_table.append_column("_step_id", pa.array(step_ids))
            
            episode_parquet_path = f"{collectorfox.step_data_dir}/{dataset}/{episode_index}/steps.parquet"
            os.makedirs(os.path.dirname(episode_parquet_path), exist_ok=True)
            pq.write_table(step_data_table, episode_parquet_path)
            print(f"Saved episode steps to {episode_parquet_path}")
            
            for key, value in episode_meta_row.items():
                episode_table[key] += value
            episode_table["_episode_id"] += [episode_index]
            episode_table["_episode_path"] += [episode_parquet_path]
        for src_col, target_col, annotator in collectorfox.annotators[dataset]:
            print(f"Annotating {src_col} to {target_col}")
            if src_col in episode_table:
                annotation = annotator.annotate(episode_table[src_col])
                episode_table[target_col] = annotation
        tbl.add(pa.table(episode_table))
        return jsonify({"message": "Dataset written successfully"}), 200
    
    except Exception as e:
        return jsonify({"message": str(e)}), 500