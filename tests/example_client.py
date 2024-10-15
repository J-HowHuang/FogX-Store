from skulkclient.client import SkulkClient
from skulkclient.predatorfox.cmd_pb2 import SkulkQuery
import logging
import sys

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    client = SkulkClient(sys.argv[1])

    table = client.get_dataset(SkulkQuery(
        dataset="ucsd_pick_and_place_dataset_converted_externally_to_rlds",
        columns=[]
    ))

    print(table.to_pandas())
