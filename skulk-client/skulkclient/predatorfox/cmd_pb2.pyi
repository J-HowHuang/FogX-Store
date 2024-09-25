from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommandType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNKNOWN: _ClassVar[CommandType]
    QUERY: _ClassVar[CommandType]
UNKNOWN: CommandType
QUERY: CommandType

class Command(_message.Message):
    __slots__ = ("cmd_type", "query")
    CMD_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    cmd_type: CommandType
    query: SkulkQuery
    def __init__(self, cmd_type: _Optional[_Union[CommandType, str]] = ..., query: _Optional[_Union[SkulkQuery, _Mapping]] = ...) -> None: ...

class SkulkQuery(_message.Message):
    __slots__ = ("dataset", "columns", "predicates", "vector_query", "limit")
    DATASET_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    PREDICATES_FIELD_NUMBER: _ClassVar[int]
    VECTOR_QUERY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    predicates: str
    vector_query: VectorQuery
    limit: int
    def __init__(self, dataset: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., predicates: _Optional[str] = ..., vector_query: _Optional[_Union[VectorQuery, _Mapping]] = ..., limit: _Optional[int] = ...) -> None: ...

class VectorQuery(_message.Message):
    __slots__ = ("column", "target_vector", "top_k")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    TARGET_VECTOR_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    column: str
    target_vector: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    def __init__(self, column: _Optional[str] = ..., target_vector: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ...) -> None: ...
