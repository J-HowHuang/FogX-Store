import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import pathlib
import lance


class FogXStore(pa.flight.FlightServerBase):
    def __init__(
        self,
        location="grpc://0.0.0.0:11634",
        repo=pathlib.Path("./_datasets"),
        **kwargs
    ):
        super(FogXStore, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo

    def _make_flight_info(self, dataset):
        dataset_path = self._repo / dataset
        lance_ds = lance.dataset(dataset_path)
        schema = lance_ds.schema
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode("utf-8")
        )
        endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
        return pa.flight.FlightInfo(
            schema, descriptor, endpoints, lance_ds.count_rows(), -1
        )

    def list_flights(self, context, criteria):
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode("utf-8"))

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode("utf-8")
        dataset_path = self._repo / dataset
        data_table = reader.read_pandas()
        lance.write_dataset(data_table, dataset_path)

    def do_get(self, context, ticket):
        dataset = ticket.ticket.decode("utf-8")
        dataset_path = self._repo / dataset
        return pa.flight.RecordBatchStream(
            lance.dataset(dataset_path).to_table()
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
