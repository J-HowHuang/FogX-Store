import requests
import sys
import pyarrow as pa
import base64

if __name__ == "__main__":
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