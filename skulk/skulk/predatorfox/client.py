import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet

from .cmd_pb2 import SkulkQuery, Command, CommandType



class PredatorfoxClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.client = pa.flight.connect(f"grpc://{self.endpoint}")

    def query(self, query: SkulkQuery) -> pa.Table:
        upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(
           Command(cmd_type=CommandType.QUERY, query=query).SerializeToString()
        )
        flight = self.client.get_flight_info(upload_descriptor)
        print(flight)

        # reader = self.client.do_get(flight.endpoints[0].ticket)
        # read_table = reader.read_all()

        return None
