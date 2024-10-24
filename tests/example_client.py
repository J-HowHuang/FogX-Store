from skulkclient.client import SkulkClient
from skulkclient.predatorfox.cmd_pb2 import SkulkQuery, VectorQuery
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
    
    table = client.get_dataset(SkulkQuery(
        dataset="ucsd_pick_and_place_dataset_converted_externally_to_rlds",
        columns=["file_path", "n_transitions", "success"],
        predicates="success=True"
    ))
    
    print(table.to_pandas())
    
    table = client.get_dataset(SkulkQuery(
        dataset="cmu_stretch",
        columns=[]
    ))
    
    print(table.to_pandas())
