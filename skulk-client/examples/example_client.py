from skulkclient.client import SkulkClient
from skulkclient.predatorfox.cmd_pb2 import SkulkQuery
import logging
import sys


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

client = SkulkClient("localhost:50052")

table = client.get_dataset(SkulkQuery(
    dataset="demo_ds_2",
    columns=[],
    predicates="reward_count > 100"
))

print(table.to_pandas())
