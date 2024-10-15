import requests
import sys
import pyarrow as pa
import base64

if __name__ == "__main__":

    # Define the API URL
    url = f"{sys.argv[1]}/create"
    schema = pa.schema([
        pa.field("disclaimer", pa.string()),
        pa.field("file_path", pa.string()),
        pa.field("n_transitions", pa.int32()),
        pa.field("success", pa.bool_()),  # Corrected boolean field
        pa.field("success_labeled_by", pa.string())
    ])

    serialized_schema = schema.serialize().to_pybytes()
    encoded_schema = base64.b64encode(serialized_schema).decode('utf-8')

    # Define the payload
    data = {
        "dataset": "ucsd_pick_and_place_dataset_converted_externally_to_rlds", # create a dataset table with this name
        "uri": "./data/dataset_db", # load lancedb from this uri
        "schema" : encoded_schema # user defined data schema
    }

    # Send the POST request
    response = requests.post(url, json=data)

    # Print the response
    print(response.status_code)
    print(response.json())


    # Define the API URL
    url = f"{sys.argv[1]}/write"

    # Define the payload
    data = {
        "ds_path": "gs://gresearch/robotics/ucsd_pick_and_place_dataset_converted_externally_to_rlds/0.1.0",
        "dataset" : "ucsd_pick_and_place_dataset_converted_externally_to_rlds",
        "uri": "./data/dataset_db"
    }

    # Send the POST request
    response = requests.post(url, json=data)
