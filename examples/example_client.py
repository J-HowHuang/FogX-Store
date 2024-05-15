from skulk.client import SkulkClient
from skulk.core.query import SkulkQuery

client = SkulkClient("localhost:11634")
        
table = client.get_dataset(SkulkQuery(
    dataset="demo_ds_1",
    return_columns=[],
    predicate="reward_count > 100"
))

print(table.to_pandas())
