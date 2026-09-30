"""VARIANT (JSON text) columns and JSON-annotated parquet strings scan NATIVELY.

A VARIANT column is German-string storage holding JSON text (draken/core/buffers.h),
so the native scan treats it as a string column. Before this, the classifier refused
it (`non_admissible_kind:VARIANT`) and the footer gate refused a `json`-annotated
byte_array, so both ran on the Python-driven StreamingScanSource trampoline.

The oracle is the forced-trampoline path over the same file: native and trampoline
must agree on values and on the column's DrakenType tag.

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
from opteryx.connectors.parquet_io import pool_reader
from opteryx.managers.execution.compiler import _Compiler


class _Col:
    def __init__(self, physical):
        self.column_type = type("CT", (), {"physical": physical, "logical": None})()


def test_classifier_admits_variant_as_varchar_kind():
    kinds, string_types, _dec, _lc, _wid, bad = _Compiler._classify_scan_columns(
        None, [_Col(DrakenType.VARIANT), _Col(DrakenType.NVARCHAR)])
    assert bad is None
    assert kinds == ["varchar", "varchar"]
    # VARIANT is tagged VARCHAR exactly as the trampoline's `_string_type_for` does.
    assert string_types == [DrakenType.VARCHAR.value, DrakenType.NVARCHAR.value]


def _drain(sql, force_trampoline, monkeypatch):
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows, tags = [], {}
    for morsel in session.execute_to_morsels(sql):
        for n in morsel.column_names:
            tags[n] = morsel.column(n).type
            rows.extend((n, v) for v in morsel.column(n).to_pylist())
    sources = set(session.telemetry["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    return sorted(map(repr, rows)), tags, sources


@pytest.mark.parametrize("use_dictionary", [False, True])
def test_json_annotated_string_column_native_matches_trampoline(
        tmp_path, monkeypatch, use_dictionary):
    values = ['{"a": 1, "b": [2, 3]}', None, '{"k": "v\\"q"}', '[]', '{"a": 1, "b": [2, 3]}']
    table = pa.table({
        "id": pa.array(range(len(values)), pa.int64()),
        "doc": pa.array(values, pa.json_(pa.string())),
    })
    d = tmp_path / "jsonds"
    d.mkdir()
    pq.write_table(table, d / "part.parquet", use_dictionary=use_dictionary)

    sql = f"SELECT id, doc FROM '{d}'"
    native = _drain(sql, False, monkeypatch)
    oracle = _drain(sql, True, monkeypatch)

    assert native[2] == {"NativeParquetScanSource"}, native[2]
    assert oracle[2] == {"StreamingScanSource"}, oracle[2]
    assert native[0] == oracle[0]
    assert native[1] == oracle[1]
    assert sorted(v for n, v in (eval(r) for r in native[0]) if n == b"doc" and v is not None) \
        == sorted(v for v in values if v is not None)
