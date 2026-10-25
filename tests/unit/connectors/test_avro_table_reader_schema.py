# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The reader schema a catalog table's Avro data files are read as
(opteryx.connectors.avro_io.table_reader_schema; docs/ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md §4.2).

A bound column cannot be constructed outside a query's ColumnTable, so the builder is
driven with stand-ins carrying the four attributes it reads. The end-to-end behaviour
(field-id matching, evolution, refusals) is tested through SQL in opteryx-iceberg's
tests/test_avro_data_files.py.
"""

import json
import os
import sys
from collections import namedtuple

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.connectors.avro_io import table_field_name
from opteryx.connectors.avro_io import table_reader_schema
from opteryx.exceptions import NotSupportedError
from opteryx.types import logical_type as lt

Column = namedtuple("Column", "name field_id column_type nullable")


def _fields(columns):
    return json.loads(table_reader_schema(columns))["fields"]


def test_each_field_is_named_by_its_field_id_and_carries_it():
    columns = [
        Column("id", 1, lt.INT64, False),
        Column("renamed_later", 7, lt.VARCHAR, True),
    ]
    assert _fields(columns) == [
        {"name": "f1", "type": "long", "field-id": 1},
        {"name": "f7", "type": ["null", "string"], "field-id": 7},
    ]
    assert table_field_name("t", columns[1]) == "f7"


@pytest.mark.parametrize(
    "column_type, avro_type",
    [
        (lt.BOOLEAN, "boolean"),
        (lt.INT32, "int"),
        (lt.INT64, "long"),
        (lt.FLOAT32, "float"),
        (lt.FLOAT64, "double"),
        (lt.VARCHAR, "string"),
        (lt.VARBINARY, "bytes"),
        (lt.DATE, {"type": "int", "logicalType": "date"}),
        (lt.TIMESTAMP(), {"type": "long", "logicalType": "timestamp-micros"}),
    ],
)
def test_primitive_types(column_type, avro_type):
    (field,) = _fields([Column("c", 3, column_type, False)])
    assert field["type"] == avro_type


@pytest.mark.parametrize(
    "precision, size",
    # Iceberg's decimalRequiredBytes: the fewest bytes holding 10^p - 1 signed.
    [(1, 1), (2, 1), (3, 2), (9, 4), (10, 5), (18, 8), (19, 9), (38, 16)],
)
def test_decimal_is_a_fixed_of_the_width_iceberg_writes(precision, size):
    (field,) = _fields([Column("c", 3, lt.DECIMAL(precision, 0), False)])
    assert field["type"]["type"] == "fixed"
    assert field["type"]["size"] == size
    assert field["type"]["precision"] == precision


def test_uuid_is_a_fixed_16_named_by_its_field_id():
    (field,) = _fields([Column("c", 3, lt.UUID, False)])
    assert field["type"] == {"type": "fixed", "name": "f3_type", "size": 16, "logicalType": "uuid"}


def test_time_is_time_micros():
    (field,) = _fields([Column("c", 3, lt.TIME(), False)])
    assert field["type"] == {"type": "long", "logicalType": "time-micros"}


def test_two_decimals_of_one_shape_get_distinct_type_names():
    # Avro names a fixed type once per schema; two same-shaped decimals must not share one.
    a, b = _fields([Column("a", 3, lt.DECIMAL(9, 2), False), Column("b", 4, lt.DECIMAL(9, 2), False)])
    assert a["type"]["name"] != b["type"]["name"]
    assert (a["type"]["size"], a["type"]["precision"]) == (b["type"]["size"], b["type"]["precision"])


def test_a_column_the_scan_cannot_read_is_left_out_of_the_schema():
    columns = [
        Column("keep", 1, lt.INT64, True),
        Column("no_id", None, lt.INT64, True),
        Column("nanos", 3, lt.TIMESTAMP(lt.TimestampUnit.NANOSECONDS), True),
        Column("array", 4, lt.ARRAY(lt.INT64), True),
    ]
    assert [f["name"] for f in _fields(columns)] == ["f1"]


def test_projecting_a_column_without_a_field_id_names_it():
    with pytest.raises(NotSupportedError, match=r"column 'x' of ns\.t.*no field id"):
        table_field_name("ns.t", Column("x", None, lt.INT64, True))


def test_projecting_a_type_the_scan_does_not_read_names_it():
    with pytest.raises(NotSupportedError, match=r"column 'ts' of ns\.t.*primitive columns only"):
        table_field_name("ns.t", Column("ts", 2, lt.TIMESTAMP(lt.TimestampUnit.NANOSECONDS), True))
    with pytest.raises(NotSupportedError, match=r"column 'a' of ns\.t.*primitive columns only"):
        table_field_name("ns.t", Column("a", 2, lt.ARRAY(lt.INT64), True))


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
