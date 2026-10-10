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

from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import ColumnType

# draken/logical_type.h LogicalKind ordinals, as rugo reports them.
_LOGICAL_TIMESTAMP = 1
_LOGICAL_TIME = 2
_LOGICAL_DECIMAL = 3


def _column_type(column: dict) -> ColumnType:
    kind = column["logical_kind"]
    if kind == _LOGICAL_TIMESTAMP:
        return _lt.TIMESTAMP(_lt.TimestampUnit.MICROSECONDS)
    if kind == _LOGICAL_TIME:
        return _lt.TIME(_lt.TimestampUnit.MICROSECONDS)
    if kind == _LOGICAL_DECIMAL:
        return _lt.DECIMAL(column["precision"], column["scale"])
    physical = DrakenType(column["type"])
    if physical == DrakenType.ARRAY:
        return _lt.ARRAY(ColumnType(DrakenType(column["child_type"])))
    return ColumnType(physical)


def header_schema(data) -> Tuple[str, List[Tuple[str, ColumnType]]]:
    """The file's writer schema (JSON text) and, per top-level field in schema order,
    (name, ColumnType) — read from the header alone. Raises RuntimeError for a file
    rugo refuses (corrupt header, an unsupported construct)."""
    from rugo.rugo_native import read_avro_column_types
    from rugo.rugo_native import read_avro_metadata

    schema_json = read_avro_metadata(data)["schema"]
    return schema_json, [(c["name"], _column_type(c)) for c in read_avro_column_types(data)]
