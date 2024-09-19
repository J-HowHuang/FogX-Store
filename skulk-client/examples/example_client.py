from skulk.client import SkulkClient
from skulk.predatorfox.cmd_pb2 import SkulkQuery

client = SkulkClient("localhost:11634")
        
table = client.get_dataset(SkulkQuery(
    dataset="demo_ds_1",
    columns=[],
    predicates="reward_count > 100"
))

print(table.to_pandas())
