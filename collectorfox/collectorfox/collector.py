from flask import Flask
import tensorflow as tf
import lance
import pyarrow as pa
from collections import defaultdict

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

@app.route('/')
def rlds_to_lance(ds: tf.data.Dataset, uri: str):  
    episode_table = defaultdict(list)
    for episode in ds:
        episode.pop('steps') # Remove steps from the episode (Rest of the data is metadata)
        for key, value in episode['episode_metadata'].items():
            for key, value in episode['episode_metadata'].items():
                value = value.numpy()
                if isinstance(value, bytes):
                    value = str(value)
                episode_table[key].append(value)
        arrow_table = pa.table(episode_table)

    lance.write_dataset(arrow_table, uri, max_rows_per_group=8192, max_rows_per_file=1024*1024)


if __name__ == '__main__':
    app.run()