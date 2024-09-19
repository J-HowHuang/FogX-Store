from flask import Flask
import tensorflow as tf
import lancedb

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

if __name__ == '__main__':
    app.run()