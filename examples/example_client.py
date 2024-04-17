from fogxstore.client import FogXStoreClient

client = FogXStoreClient("localhost:11634")
table = client.get_dataset("test")

print(table.to_pandas())
