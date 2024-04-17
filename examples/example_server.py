from fogxstore.server import FogXStore
import pathlib

server = FogXStore(
    location="grpc://localhost:11634", repo=pathlib.Path("./tests/datasets")
)
server.serve()
