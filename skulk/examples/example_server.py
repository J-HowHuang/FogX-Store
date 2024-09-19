from skulk.server import SkulkServer
import pathlib

server = SkulkServer(
    location="grpc://localhost:11634", repo=pathlib.Path("examples/_datasets")
)
server.serve()
