import pyarrow as pa
import pyarrow.flight
import os
import pathlib
import duckdb

from ..predatorfox.cmd_pb2 import SkulkQuery, VectorQuery


class SkulkServer(pa.flight.FlightServerBase):
    def __init__(
        self,
        location="grpc://0.0.0.0:11634",
        repo=pathlib.Path("./_datasets"),
        **kwargs
    ):
        super(SkulkServer, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo
        self.ticket_booth = {}

    def _make_flight_info(self, descriptor):
        # TODO: replace pickle to other serialization method
        query: SkulkQuery = SkulkQuery()
        query.ParseFromString(descriptor.command)
        sql = self.query_to_sql(query)
        print(sql)
        dataset: pa.Table = duckdb.sql(sql).arrow()
        # TODO: require small table size and fast query
        self.ticket_booth[sql] = dataset
        endpoints = [pa.flight.FlightEndpoint(sql.encode('utf-8'), [self._location])]
        return pa.flight.FlightInfo(
            dataset.schema, descriptor, endpoints, dataset.num_rows, dataset.nbytes
        )
    
    def query_to_sql(self, query: SkulkQuery):
        return f'SELECT {",".join(query.columns) if len(query.columns) else "*"} FROM \'{os.path.join(self._repo, f"{query.dataset}.parquet")}\' {f" WHERE {query.predicates}" if query.predicates else ""} {self.vector_query_to_sql(query.vector_query) if query.HasField("vector_query") else ""};'
    
    def vector_query_to_sql(self, vector_query: VectorQuery):
        return f'ORDER BY array_distance({vector_query.column}, [{", ".join(vector_query.target_vector)}]::FLOAT[{len(vector_query.target_vector)}]) LIMIT {vector_query.top_k}'
    
    def list_flights(self, context, criteria):
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor)

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode("utf-8")
        dataset_path = self._repo / dataset
        data_table = reader.read_pandas()
        # lance.write_dataset(data_table, dataset_path)

    def do_get(self, context, ticket):
        return pa.flight.RecordBatchStream(
            self.ticket_booth[ticket.ticket.decode("utf-8")]
        )

    def list_actions(self, context):
        return [
            ("drop_dataset", "Delete a dataset."),
        ]

    def do_action(self, context, action):
        if action.type == "drop_dataset":
            self.do_drop_dataset(action.body.to_pybytes().decode("utf-8"))
        else:
            raise NotImplementedError

    def do_drop_dataset(self, dataset):
        dataset_path = self._repo / dataset
        dataset_path.unlink()
