from skulk.client import SkulkClient
from skulk.server import SkulkServer
from skulk.core.query import SkulkQuery
import duckdb
from threading import Thread
import pathlib


def _run_fogx_store(server):
    server.serve()


def test_client_get_dataset():
    server = SkulkServer(
        location="grpc://localhost:11634", repo=pathlib.Path("tests/datasets")
    )
    t = Thread(target=_run_fogx_store, args=[server])
    t.start()

    client = SkulkClient("localhost:11634")
    table = client.get_dataset(SkulkQuery(
        dataset="demo_ds_1",
        return_columns=[]
    ))
    true_table = duckdb.sql("SELECT * FROM 'tests/datasets/demo_ds_1.parquet'").arrow()
    print(table.to_pandas())
    try:
        assert table.schema == true_table.schema
        assert table == true_table
    except Exception as e:
        server.shutdown()
        raise e
    server.shutdown()
