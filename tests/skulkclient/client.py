import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq
import pandas as pd
import logging

from .predatorfox.cmd_pb2 import SkulkQuery


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SkulkClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.client = pyarrow.flight.connect(f"grpc://{self.endpoint}")

    def get_dataset(self, skulk_query: SkulkQuery) -> pd.DataFrame:
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
        result = []
        for endpoint in flight.endpoints:
            logger.debug(f"Endpoint: {endpoint.ticket}")
            for loc in endpoint.locations:
                logger.debug(f"Location: {loc}")
                client = pyarrow.flight.connect(loc)
                reader = client.do_get(endpoint.ticket)
                read_table = reader.read_all()
                result.append(read_table)
        res = pa.concat_tables(result)
        if skulk_query.with_step_data:
            parquets = res.column("step_data").combine_chunks().to_pylist()
            tables = []
            for parquet in parquets:
                reader = pyarrow.BufferReader(parquet)
                tables.append(pq.read_table(reader))
            step_data = pa.concat_tables(tables).to_pandas()
            step_data["_episode_id"] = step_data["_episode_id"].astype(str)
            res = res.remove_column(res.schema.get_field_index("step_data"))
            res = res.to_pandas()
            res["_episode_id"] = res["_episode_id"].astype(str)
            return res.merge(step_data, on="_episode_id")
        
        res = res.to_pandas()
        res["_episode_id"] = res["_episode_id"].astype(str)
        return res
    