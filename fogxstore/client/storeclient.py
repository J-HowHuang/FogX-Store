import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import logging


class FogXStoreClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.client = pyarrow.flight.connect(f"grpc://{self.endpoint}")

    def get_dataset(self, dataset_name) -> pa.Table:
        upload_descriptor = pa.flight.FlightDescriptor.for_path(
            f"{dataset_name}.lance"
        )
        flight = self.client.get_flight_info(upload_descriptor)
        descriptor = flight.descriptor
        logging.debug(
            "Path:",
            descriptor.path[0].decode("utf-8"),
            "Rows:",
            flight.total_records,
            "Size:",
            flight.total_bytes,
        )
        logging.debug("=== Schema ===")
        logging.debug(flight.schema)
        logging.debug("==============")

        reader = self.client.do_get(flight.endpoints[0].ticket)
        read_table = reader.read_all()

        return read_table
