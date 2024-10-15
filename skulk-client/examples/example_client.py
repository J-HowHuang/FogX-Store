from skulkclient.client import SkulkClient
from skulkclient.predatorfox.cmd_pb2 import SkulkQuery
import logging
import sys


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

client = SkulkClient("[::1]:50052")

table = client.get_dataset(SkulkQuery(
    dataset="ucsd_pick_and_place_dataset_converted_externally_to_rlds",
    columns=[]
))

print(table.to_pandas())
