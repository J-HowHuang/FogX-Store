from flask import Flask, jsonify, request
import tensorflow as tf
import lance
import pyarrow as pa
from collections import defaultdict
import tensorflow_datasets as tfds

app = Flask(__name__)

@app.route('/')
def index():
    # Read in the TensorFlow dataset
    dataset = tf.data.Dataset.from_tensor_slices([1, 2, 3, 4, 5])

    # Connect to LanceDB
    db = lancedb.connect('your_database_url')

    # Create a new collection in LanceDB
    collection = db.create_collection('your_collection_name')

    # Write the dataset into LanceDB
    for data in dataset:
        collection.insert(data.numpy())

    return 'Data written to LanceDB successfully!'

@app.route('/write', methods=['POST'])
def rlds_to_lance(): 
    try:
        uri = request.json.get('uri')
        ds_path = request.json.get('ds_path') 
        
        if ds_path.startswith('gs://'): # if the dataset is stored in GCS
            b = tfds.builder_from_directory(builder_dir=ds_path)
            ds = b.as_dataset(split='train[:10]').shuffle(10)
        else: # if the dataset is stored locally
            ds = tf.data.Dataset.load(ds_path)
        
        episode_table = defaultdict(list)
        for episode in ds:
            # serialize the step tensor and store it into step colomn
            for step_id, step in enumerate(episode['steps']):
                for key, data in step.items():
                    if key == 'observation':
                        continue
                    data = data.numpy()
                    episode_table[f"step_{step_id}_{key}"].append(data)

            for key, value in episode['episode_metadata'].items():
                value = value.numpy()
                if isinstance(value, bytes): # serialize the bytes tensor
                    value = str(value)
                episode_table[key].append(value)
            arrow_table = pa.table(episode_table)
        for k, v in episode_table.items():
            print(len(v))
        lance.write_dataset(arrow_table, uri, max_rows_per_group=8192, max_rows_per_file=1024*1024)
        return jsonify({"message": "Dataset written successfully"}), 200
    
    except Exception as e:
        return jsonify({"message": str(e)}), 500



if __name__ == '__main__':
    app.run()