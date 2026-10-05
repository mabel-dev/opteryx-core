"""VARIANT (JSON text) columns and JSON-annotated parquet strings scan NATIVELY.

A VARIANT column is German-string storage holding JSON text (draken/core/buffers.h),
so the native scan treats it as a string column, and the footer gate admits a
`json`-annotated byte_array.

The oracle is the plain-Python values the test wrote, and the column type the schema
binder declares for a `json`-annotated parquet string (connectors/_rugo_schema.py
PARQUET_LOGICAL_TYPE_MAP: "json" → NVARCHAR). The JSON text is passed through
verbatim, not re-rendered.

Physical parquet STRUCT/MAP columns are covered by test_nested_json_scan.py.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from draken.draken_native import DrakenType
from opteryx.managers.execution.compiler import _Compiler


class _Col:
    def __init__(self, physical):
        self.column_type = type("CT", (), {"physical": physical, "logical": None})()


def test_classifier_admits_variant_as_varchar_kind():
    kinds, string_types, _dec, _lc, _wid, bad = _Compiler._classify_scan_columns(
        None, [_Col(DrakenType.VARIANT), _Col(DrakenType.NVARCHAR)])
    assert bad is None
    assert kinds == ["varchar", "varchar"]
    # VARIANT is JSON text in German-string storage: it is scanned and tagged as VARCHAR.
    assert string_types == [DrakenType.VARCHAR.value, DrakenType.NVARCHAR.value]


def _drain(sql):
    session = opteryx.session()
    rows, tags = [], {}
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        for n in names:
            tags[n.decode()] = morsel.column(n).type
        rows.extend(zip(*(morsel.column(n).to_pylist() for n in names)))
    sources = set(session.telemetry["scan_sources"].values())
    return sorted(rows, key=repr), tags, sources


@pytest.mark.parametrize("use_dictionary", [False, True])
def test_json_annotated_string_column_native_matches_oracle(tmp_path, use_dictionary):
    values = ['{"a": 1, "b": [2, 3]}', None, '{"k": "v\\"q"}', '[]', '{"a": 1, "b": [2, 3]}']
    table = pa.table({
        "id": pa.array(range(len(values)), pa.int64()),
        "doc": pa.array(values, pa.json_(pa.string())),
    })
    d = tmp_path / "jsonds"
    d.mkdir()
    pq.write_table(table, d / "part.parquet", use_dictionary=use_dictionary)

    rows, tags, sources = _drain(f"SELECT id, doc FROM '{d}'")

    assert sources == {"NativeParquetScanSource"}, sources
    assert tags == {"id": DrakenType.INT64, "doc": DrakenType.NVARCHAR}, tags
    assert rows == sorted(enumerate(values), key=repr)
