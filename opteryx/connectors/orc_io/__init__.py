# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ORC IO - the planning-side pieces of an ORC scan (docs/ORC_READER_DESIGN.md).

Execution is native (src/cpp/engine/native_orc_scan_source.hpp). Planning needs two
things: the relation schema of a folder of .orc files, which the first file's footer
states (`header_schema`), and the reader columns every file is read as, from the bound
relation (`reader_columns`). The ORC-to-Draken type mapping itself lives in rugo alone
(rugo/src/orc/orc_reader.cpp); this module only spells its decisions as ColumnType.
"""

from typing import List, Tuple

from draken.draken_native import DrakenType

from opteryx.exceptions import NotSupportedError
from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import ColumnType


def _column_type(column: dict) -> ColumnType:
    physical = DrakenType(column["type"])
    if physical in (DrakenType.DECIMAL, DrakenType.DECIMAL128):
        return _lt.DECIMAL(column["precision"], column["scale"])
    if physical == DrakenType.UUID:
        return _lt.UUID
    if physical == DrakenType.TIMESTAMP64:
        return _lt.TIMESTAMP(_lt.TimestampUnit.MICROSECONDS)
    if physical == DrakenType.TIME64:
        # Declared TIME, as READ_PARQUET declares a time column; the scan returns INT64 microseconds
        # since midnight carrying the TIME logical type (parquet's convention).
        return _lt.TIME(_lt.TimestampUnit.MICROSECONDS)
    if physical == DrakenType.ARRAY:
        array = ColumnType(DrakenType(column["child_type"]))
        for _ in range(column["array_depth"]):
            array = _lt.ARRAY(array)
        return array
    return ColumnType(physical)


def header_schema(data, path: str) -> List[Tuple[str, ColumnType]]:
    """The file's top-level columns, (name, ColumnType) in file order, from its footer
    alone. A STRUCT / MAP / LIST is NVARCHAR (JSON text) or, for a LIST of a plain scalar,
    ARRAY (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1). Raises RuntimeError for a file rugo
    refuses (not ORC, corrupt, encrypted, LZO) and NotSupportedError, naming the column, for
    one the reader does not read (a UNION, a MAP with non-string keys, a nanosecond timestamp)."""
    from rugo.rugo_native import read_orc_metadata

    columns = []
    for column in read_orc_metadata(data)["columns"]:
        if not column["readable"]:
            raise NotSupportedError(
                f"Cannot read column '{column['name']}' of the ORC file {path}: {column['refusal']}."
            )
        columns.append((column["name"], _column_type(column)))
    return columns


def _reader_type(column_type: ColumnType):
    """(DrakenType ordinal, precision, scale, ARRAY innermost element ordinal, ARRAY depth) the
    ORC reader reads this column as, or None for a type it does not read. The element is
    INT64 and the depth 1, both unused, for anything but an ARRAY."""
    physical = column_type.physical
    if physical in (DrakenType.BOOL, DrakenType.INT8, DrakenType.INT16, DrakenType.INT32,
                    DrakenType.INT64, DrakenType.FLOAT32, DrakenType.FLOAT64,
                    DrakenType.VARCHAR, DrakenType.VARBINARY, DrakenType.DATE32, DrakenType.UUID,
                    DrakenType.NVARCHAR):
        return physical.value, 0, 0, DrakenType.INT64.value, 1
    if physical in (DrakenType.TIMESTAMP64, DrakenType.TIME64):
        if column_type.logical.unit != _lt.TimestampUnit.MICROSECONDS:
            return None
        return physical.value, 0, 0, DrakenType.INT64.value, 1
    if physical in (DrakenType.DECIMAL, DrakenType.DECIMAL128):
        return physical.value, column_type.logical.precision, column_type.logical.scale, DrakenType.INT64.value, 1
    if physical == DrakenType.ARRAY and column_type.element is not None:
        depth, element = 1, column_type.element
        while element.physical == DrakenType.ARRAY and element.element is not None:
            depth, element = depth + 1, element.element
        return physical.value, 0, 0, element.physical.value, depth
    return None


def reader_columns(dataset: str, columns, match_by_id: bool, declared=None) -> List[tuple]:
    """What each projected column is to be read as: (name, field_id, DrakenType ordinal,
    precision, scale, nullable, ARRAY element ordinal, ARRAY depth, declared nested shape).
    `declared` is {field id: NestedField} from the table's connector; a nested column with one is
    verified against it in each file (None for the rest). `columns` are bound columns. Raises NotSupportedError,
    naming the column, for one the ORC scan cannot read - a table column with no field
    id when files are matched by id, or a type the reader does not read."""
    out = []
    seen = set()
    for column in columns:
        if column.name in seen:
            continue
        seen.add(column.name)
        if match_by_id and column.field_id is None:
            raise NotSupportedError(
                f"Cannot read column '{column.name}' of {dataset}: it has no field id, "
                "which ORC data files are matched on."
            )
        spec = _reader_type(column.column_type)
        if spec is None:
            raise NotSupportedError(
                f"Cannot read column '{column.name}' of {dataset}: ORC data files are read "
                f"for primitive columns only, not {column.column_type}."
            )
        shape = None
        if declared and column.field_id is not None and column.field_id in declared:
            shape = declared[column.field_id].flatten()
        out.append((column.name, column.field_id if match_by_id else None, *spec[:3], column.nullable, spec[3], spec[4], shape))
    return out
