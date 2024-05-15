import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import pathlib
import duckdb
import pickle

from ..core.query import SkulkQuery


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
        print("===============")
        print(descriptor.descriptor_type)
        print(descriptor.command[:50])
        query: SkulkQuery = pickle.loads(descriptor.command)
        sql = query.to_sql(self._repo)
        print(sql)
        dataset: pa.Table = duckdb.sql(sql).arrow()
        self.ticket_booth[sql] = dataset
        print(dataset.num_rows)
        endpoints = [pa.flight.FlightEndpoint(sql.encode('utf-8'), [self._location])]
        return pa.flight.FlightInfo(
            dataset.schema, descriptor, endpoints, dataset.num_rows, dataset.nbytes
        )

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
