import requests

url = 'http://localhost:11632/dataset'
data = {'key1': 'value1', 'key2': 'value2'}  # Replace with your desired payload

response = requests.post(f"{url}/demo_ds_2", data="")

print(response.status_code, response.text)

response = requests.post(f"{url}/demo_ds_2/add", data="http://localhost:50052")

print(response.status_code, response.text)