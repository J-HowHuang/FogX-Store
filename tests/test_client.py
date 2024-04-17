from fogxstore.client import FogXStoreClient
from fogxstore.server import FogXStore
import pyarrow as pa
import lance
from threading import Thread
import pathlib

def _run_fogx_store(server):
    server.serve()

def test_client_get_dataset():
    server = FogXStore(location="grpc://localhost:11634", repo=pathlib.Path("./_datasets"))
    t = Thread(target=_run_fogx_store, args=[server])
    t.start()
    
    client = FogXStoreClient("localhost:11634")
    table = client.get_dataset("test")
    true_table = lance.dataset("tests/datasets/test.lance").to_table()
    print(table.to_pandas())
    try:
        assert table.schema == true_table.schema
        assert table == true_table
    except Exception as e:
        server.shutdown()
        raise e
    server.shutdown()
    