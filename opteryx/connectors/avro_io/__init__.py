# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Avro IO — the planning-side piece of READ_AVRO.

Execution is native (src/cpp/engine/native_avro_scan_source.hpp); planning needs only
the relation schema, which an Avro file states in its header. `header_schema` reads it
without decoding a block: rugo compiles the file's schema exactly as the scan will and
reports what each column decodes to, so the bound types and the decoded vectors cannot
disagree. This module only spells those decisions as ColumnType — the Avro-to-Draken
type mapping itself lives in rugo alone (rugo/src/avro/avro_reader.cpp).
"""

from typing import List, Tuple

from draken.draken_native import DrakenType

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import md_cause
from opteryx.exceptions import md_code
from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import ColumnType

# draken/logical_type.h LogicalKind ordinals, as rugo reports them.
_LOGICAL_TIMESTAMP = 1
_LOGICAL_TIME = 2
_LOGICAL_DECIMAL = 3
_LOGICAL_IPV4 = 5


def _column_type(column: dict) -> ColumnType:
    kind = column["logical_kind"]
    if kind == _LOGICAL_IPV4:
        return _lt.IPV4
    if kind == _LOGICAL_TIMESTAMP:
        return _lt.TIMESTAMP(_lt.TimestampUnit.MICROSECONDS)
    if kind == _LOGICAL_TIME:
        # Declared TIME, as READ_PARQUET declares a time column; the scan returns INT64 microseconds
        # since midnight carrying the TIME logical type (parquet's convention).
        return _lt.TIME(_lt.TimestampUnit.MICROSECONDS)
    if kind == _LOGICAL_DECIMAL:
        return _lt.DECIMAL(column["precision"], column["scale"])
    physical = DrakenType(column["type"])
    if physical == DrakenType.ARRAY:
        array = ColumnType(DrakenType(column["child_type"]))
        for _ in range(column["array_depth"]):
            array = _lt.ARRAY(array)
        return array
    return ColumnType(physical)


def header_schema(data) -> Tuple[str, List[Tuple[str, ColumnType]]]:
    """The file's writer schema (JSON text) and, per top-level field in schema order,
    (name, ColumnType) — read from the header alone. Raises RuntimeError for a file
    rugo refuses (corrupt header, an unsupported construct)."""
    from rugo.rugo_native import read_avro_column_types
    from rugo.rugo_native import read_avro_metadata

    schema_json = read_avro_metadata(data)["schema"]
    return schema_json, [(c["name"], _column_type(c)) for c in read_avro_column_types(data)]


def first_file_reader_schema(filesystem, path: str) -> str:
    """A folder dataset's reader schema: the first file's writer schema (JSON text), read
    from its header alone, as READ_AVRO's binder does. Raises DatasetReadError for a file
    rugo refuses."""
    file_obj = filesystem.open_input_file(path)
    try:
        return header_schema(file_obj.memoryview)[0]
    except RuntimeError as err:
        raise DatasetReadError(
            f"The Avro file {md_code(path)} could not be read. {md_cause(err)}"
        ) from err
    finally:
        file_obj.close()


# ---------------------------------------------------------------------------
# A catalog table's Avro data files (docs/ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md §4.2)
#
# READ_AVRO reads every file as the FIRST file's writer schema. A table's files are read
# as the TABLE's schema instead: an Iceberg Avro data file carries a `field-id` on every
# field, and only the id says which column a field is once columns have been renamed,
# added or reordered. The reader schema below names each field after its id, so the
# native resolver (rugo/src/avro, matching by field-id when both sides carry one)
# lines every file up with the current schema; a column a file never had resolves to
# NULL, and a file without ids fails loud (ids on one side only is an error).
# ---------------------------------------------------------------------------

_RECORD_NAME = "table_reader_schema"


def _decimal_fixed_size(precision: int) -> int:
    """Bytes Iceberg's Avro writer gives a decimal(precision, _): the fewest that hold
    10^precision - 1 as a two's-complement integer."""
    return ((10**precision - 1).bit_length() + 1 + 7) // 8


def _avro_type(column_type: ColumnType, type_name: str):
    """The Avro type an Iceberg writer gives this column, or None for a type the Avro
    table scan does not read. The reader's KIND must be one the file's kind promotes
    to (fixed and bytes do not promote), so this spells Iceberg's own mapping; the
    logical type is the file's (rugo follows the file's logical types). `type_name` names the
    `fixed` types this may define: an Avro named type is defined ONCE per schema, so two columns
    cannot both define "decimal_9_2" - each gets a name of its own."""
    physical = column_type.physical
    if physical == DrakenType.BOOL:
        return "boolean"
    if physical == DrakenType.INT32:
        return "int"
    if physical == DrakenType.INT64:
        return "long"
    if physical == DrakenType.FLOAT32:
        return "float"
    if physical == DrakenType.FLOAT64:
        return "double"
    if physical == DrakenType.VARCHAR:
        return "string"
    if physical == DrakenType.VARBINARY:
        return "bytes"
    if physical == DrakenType.DATE32:
        return {"type": "int", "logicalType": "date"}
    if physical == DrakenType.TIMESTAMP64:
        if column_type.logical.unit != _lt.TimestampUnit.MICROSECONDS:
            return None
        return {"type": "long", "logicalType": "timestamp-micros"}
    if physical == DrakenType.TIME64:
        if column_type.logical.unit != _lt.TimestampUnit.MICROSECONDS:
            return None
        return {"type": "long", "logicalType": "time-micros"}
    if physical == DrakenType.UUID:
        # Iceberg's Avro writer: fixed(16) with the uuid logical type.
        return {"type": "fixed", "name": type_name, "size": 16, "logicalType": "uuid"}
    if physical in (DrakenType.DECIMAL, DrakenType.DECIMAL128):
        precision = column_type.logical.precision
        return {
            "type": "fixed",
            "name": type_name,
            "size": _decimal_fixed_size(precision),
            "logicalType": "decimal",
            "precision": precision,
            "scale": column_type.logical.scale,
        }
    return None


def _table_field(column):
    """The reader-schema field for a bound table column, or None when the Avro scan
    cannot read it (no field id, or a type it does not read)."""
    if column.field_id is None:
        return None
    avro_type = _avro_type(column.column_type, f"f{column.field_id}_type")
    if avro_type is None:
        return None
    return {
        "name": f"f{column.field_id}",
        "type": ["null", avro_type] if column.nullable else avro_type,
        "field-id": column.field_id,
    }


def table_field_name(dataset: str, column) -> str:
    """The reader-schema field name a projected column decodes from. Raises
    NotSupportedError, naming the column, for one the Avro scan cannot read."""
    if _table_field(column) is not None:
        return f"f{column.field_id}"
    if column.field_id is None:
        reason = "it has no field id, which Avro data files are matched on"
    else:
        reason = f"Avro data files are read for primitive columns only, not {column.column_type}"
    raise NotSupportedError(f"Cannot read column '{column.name}' of {dataset}: {reason}.")


def table_reader_schema(columns) -> str:
    """The Avro reader schema (JSON text) every data file of a table is read as: one
    field per column the Avro scan can read, in the table's order."""
    import json

    fields = [field for field in map(_table_field, columns) if field is not None]
    return json.dumps({"type": "record", "name": _RECORD_NAME, "fields": fields})
