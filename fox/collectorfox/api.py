from collector import collectorfox
from flask import jsonify, request
import lancedb
import pyarrow as pa
import pyarrow.parquet as pq
import os
import uuid
from collections import defaultdict
import traceback

# Use uuid for episode index
def get_episode_index_for_dataset(dataset_name):
    return uuid.uuid4().hex


@collectorfox.flask_app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"message": "Collector is running"}), 200


@collectorfox.flask_app.route('/write', methods=['POST'])
def add_data_to_lancedb(): 
    try:
        dataset = request.json.get('dataset') 
        src_uri = request.json.get('src_uri') 
        src_type = request.json.get('src_type')
        
        # Connect to LanceDB with the given URI
        db = lancedb.connect(collectorfox.lance_path)
        tbl = db.open_table(dataset)

        if dataset not in collectorfox.datasets:
            return jsonify({"message": f"Unknown dataset '{dataset}'"}), 400
        foxdataset = collectorfox.datasets[dataset]
        transformation = foxdataset.get_transformation(src_type)
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
        for src_col, target_col, annotator in foxdataset.get_annotators():
            print(f"Annotating {src_col} to {target_col}")
            if src_col in episode_table:
                annotation = annotator.annotate(episode_table[src_col])
                episode_table[target_col] = annotation
        tbl.add(pa.table(episode_table))
        return jsonify({"message": "Dataset written successfully"}), 200
    
    except Exception as e:
        tb = traceback.format_exc()
        return jsonify({"message": str(e), "traceback": tb}), 500