import pyarrow as pa


class SkulkSchema:
    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema