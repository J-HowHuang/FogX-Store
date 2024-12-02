from skulkclient.client import SkulkClient
from skulkclient.predatorfox.cmd_pb2 import SkulkQuery, VectorQuery
import logging
import sys

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    
    print("Usage: python example_client.py <skulk_ip_addr>")

    client = SkulkClient(sys.argv[1])
    
    table = client.get_dataset(SkulkQuery(
        dataset="lerobot_universal",
        with_step_data=True,
        limit=1
    ))
    
    print("Table:")
    print(table) 
    
    table = client.get_dataset(SkulkQuery(
        dataset="lerobot_universal",
        columns=["language_instruction"],
        vector_query=VectorQuery(
            column="language_instruction",
            text_query="dish washer",
            top_k=10
        ),
        with_step_data=True,
        limit=1
    ))
    
    print("Table:")
    print(table)
