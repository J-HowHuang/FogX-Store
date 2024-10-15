import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import logging

from .predatorfox.cmd_pb2 import SkulkQuery


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SkulkClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.client = pyarrow.flight.connect(f"grpc://{self.endpoint}")

    def get_dataset(self, skulk_query: SkulkQuery) -> pa.Table:
        upload_descriptor = pa.flight.FlightDescriptor.for_command(
            # TODO: replace pickle to other serialization method
            skulk_query.SerializeToString()
        )
        flight = self.client.get_flight_info(upload_descriptor)
        descriptor = flight.descriptor
        # logging.debug(
        #     "SQL:",
        #     descriptor.command.decode("utf-8"),
        #     "Rows:",
        #     flight.total_records,
        #     "Size:",
        #     flight.total_bytes,
        # )
        logger.debug("=== Endpoints ===")
        logger.debug(flight.endpoints)
        logger.debug("==============")

        for endpoint in flight.endpoints:
            logger.debug(f"Endpoint: {endpoint.ticket}")
            for loc in endpoint.locations:
                logger.debug(f"Location: {loc}")
                client = pyarrow.flight.connect(loc)
                reader = client.do_get(endpoint.ticket)
                read_table = reader.read_all()

                return read_table
    
