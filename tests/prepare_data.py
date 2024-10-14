import requests
import json
import pyarrow as pa

# Define the API URL
url = "http://127.0.0.1:5000/create"
schema = pa.schema([
    pa.field("disclaimer", pa.string()),
    pa.field("file_path", pa.string()),
    pa.field("n_transitions", pa.int32()),
    pa.field("success", pa.bool_()),  # Corrected boolean field
    pa.field("success_labeled_by", pa.string())
])


fields_dict = {
    "fields": [
        {"name": field.name, "type": str(field.type) if str(field.type) != 'bool' else 'bool_'} for field in schema
    ]
}

# Define the payload
data = {
    "dataset": "ucsd_pick_and_place_dataset_converted_externally_to_rlds",
    "uri": "./db/dataset_db",
    "fields" : json.dumps(fields_dict)
}

# Send the POST request
response = requests.post(url, json=data)

# Print the response
print(response.status_code)
print(response.json())


# Define the API URL
url = "http://127.0.0.1:5000/write"

# Define the payload
data = {
    "ds_path": "gs://gresearch/robotics/ucsd_pick_and_place_dataset_converted_externally_to_rlds/0.1.0",
    "dataset" : "ucsd_pick_and_place_dataset_converted_externally_to_rlds",
    "uri": "./db/dataset_db"
}

# Send the POST request
response = requests.post(url, json=data)

# Print the response
print(response.status_code)
print(response.json())

import lancedb
uri = "../collectorfox/collectorfox/db/dataset_db"
db = lancedb.connect(uri)
tbl = db.open_table("ucsd_pick_and_place_dataset_converted_externally_to_rlds")
print(tbl.to_arrow())

result = (
    tbl.search()
    .where("success = true", prefilter=True)
    .to_arrow()
)
print(result)