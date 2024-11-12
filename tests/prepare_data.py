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
        pa.field("success_labeled_by", pa.string()),
        pa.field("language_embedding", pa.list_(pa.float32(), 384)),
        pa.field("language_instruction", pa.string())
    ],metadata={
            "language_instruction_model" : "sentence-transformers/all-MiniLM-L6-v2",
            "language_instruction_column" : "language_embedding"
            })

    serialized_schema = schema.serialize().to_pybytes()
    encoded_schema = base64.b64encode(serialized_schema).decode('utf-8')

    # Define the payload
    data = {
        "dataset": "ucsd", # create a dataset table with this name
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
        "dataset": "ucsd",
        "src_type": "rtx", # load lancedb from this uri
        "src_uri": "gs://gresearch/robotics/ucsd_pick_and_place_dataset_converted_externally_to_rlds/0.1.0", # load lancedb from this uri
    }

    # Send the POST request
    response = requests.post(url, json=data)
    print(response.status_code)
    print(response.json())
    
    url_1 = f"{sys.argv[1]}/create"
    url_2 = f"{sys.argv[2]}/create"
    schema = pa.schema([
        pa.field("file_path", pa.string()),
        pa.field("language_embedding", pa.list_(pa.float32(), 384)),
        pa.field("language_instruction", pa.string())
        ],
        metadata={
            "language_instruction_model" : "sentence-transformers/all-MiniLM-L6-v2",
            "language_instruction_column" : "language_embedding",
        })

    serialized_schema = schema.serialize().to_pybytes()
    encoded_schema = base64.b64encode(serialized_schema).decode('utf-8')

    # Define the payload
    data = {
        "dataset": "cmu_stretch", # create a dataset table with this name
        "schema" : encoded_schema # user defined data schema
    }

    # Send the POST request
    response = requests.post(url_1, json=data)

    # Print the response
    print(response.status_code)
    print(response.json())

    response = requests.post(url_2, json=data)
    print(response.status_code)
    print(response.json())

    # Define the API URL
    url_1 = f"{sys.argv[1]}/write"
    url_2 = f"{sys.argv[2]}/write"

    # Define the payload
    data = {
        "src_type": "rtx", # load lancedb from this uri
        "src_uri": "gs://gresearch/robotics/cmu_stretch/0.1.0",
        "dataset" : "cmu_stretch",
    }

    # Send the POST request
    response = requests.post(url_1, json=data)
    print(response.status_code)
    print(response.json())

    response = requests.post(url_2, json=data)
    print(response.status_code)
    print(response.json())