# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet schema field ids (SchemaElement field 9) from rugo's writer: `field_ids=`
on write_parquet / open_parquet_writer (docs/PARQUET_WRITER_DESIGN.md). Iceberg
readers match columns by these ids. Oracles: rugo's read_metadata, and pyarrow
(test-only, CLAUDE.md §4) for the `PARQUET:field_id` it reports per field.
"""

import datetime
import decimal
import io
import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from draken.vectors.vector import Vector  # noqa: E402
from rugo import parquet  # noqa: E402


def _m(**cols):
    return Morsel.from_vectors(list(cols), [Vector(v) for v in cols.values()])


def _every_branch():
    """One column per schema-element branch of write_schema (each annotation
    places field 9 differently)."""
    return _m(
        i64=dn.vector_int64_from_sequence([1, None, 3]),
        i8=dn.vector_int8_from_sequence([1, -2, None]),           # INTEGER(8) annotation
        u32=dn.vector_uint32_from_sequence([1, 2, 4294967295]),   # INTEGER(32, unsigned)
        f=dn.vector_float64_from_sequence([1.5, None, -2.0]),    # no annotation
        s=vector_from_sequence(["a", None, "ccc"], dtype="VARCHAR"),  # UTF8
        b=vector_from_sequence([b"\x00", b"", None], dtype="VARBINARY"),  # bare BYTE_ARRAY
        d=dn.vector_date32_from_sequence([datetime.date(1970, 1, 1), datetime.date(2022, 1, 9), None]),     # DATE
        ts=dn.vector_timestamp_from_sequence([datetime.datetime(2020, 1, 1), datetime.datetime(2023, 11, 14, 22, 13), None]),  # TIMESTAMP
        t=dn.vector_time64_from_sequence([datetime.time(0), datetime.time(1, 2, 3), None]),  # TIME
        dec=dn.vector_decimal_from_sequence([decimal.Decimal("1.25"), None, decimal.Decimal("-3.50")], 9, 2),
        u=dn.vector_uuid_from_sequence([uuid.UUID(int=1), None, uuid.UUID(int=2)]),  # UUID
        a=dn.vector_array_from_sequence([[1, None], [], None]),                    # LIST depth 1
        aa=dn.vector_array_from_sequence([[["x"], []], None, [["y", None]]]),     # LIST depth 2
    )


IDS = {"i64": 1, "i8": 2, "u32": 3, "f": 4, "s": 5, "b": 6, "d": 7, "ts": 8, "t": 9,
       "dec": 10, "u": 11, "a": [12, 13], "aa": (14, 15, 16)}


def _arrow_ids(data):
    """{column: [field id, element id, ...]} as pyarrow reports them."""
    out = {}
    for f in pq.read_schema(io.BytesIO(data)):
        ids, cur = [], f
        while True:
            ids.append(int(cur.metadata[b"PARQUET:field_id"]) if cur.metadata else None)
            if not pa.types.is_list(cur.type):
                break
            cur = cur.type.value_field
        out[f.name] = ids
    return out


def _want_ids():
    return {k: [v] if type(v) is int else list(v) for k, v in IDS.items()}


def test_every_branch_writes_its_field_id_and_data_is_unchanged():
    m = _every_branch()
    data = parquet.write_parquet(m, field_ids=IDS)
    assert _arrow_ids(data) == _want_ids()
    # Same values as the id-less file: field 9 is metadata only.
    plain = parquet.write_parquet(m)
    assert pq.read_table(io.BytesIO(data)).to_pylist() == pq.read_table(io.BytesIO(plain)).to_pylist()
    assert _arrow_ids(plain) == {k: [None] * len(v) for k, v in _want_ids().items()}


def test_rugo_reads_the_ids_back():
    meta = parquet.read_metadata(parquet.write_parquet(_every_branch(), field_ids=IDS))
    got = {c.name: [c.field_id, *c.element_field_ids] for c in meta.schema_columns}
    assert got == _want_ids()
    plain = parquet.read_metadata(parquet.write_parquet(_every_branch()))
    assert all(c.field_id == -1 for c in plain.schema_columns)
    assert {c.name: c.element_field_ids for c in plain.schema_columns}["aa"] == (-1, -1)


def test_without_field_ids_bytes_are_identical():
    m = _every_branch()
    assert parquet.write_parquet(m) == parquet.write_parquet(m, field_ids=None)


def test_streaming_writer_applies_ids_to_every_row_group():
    chunks = []
    ids = {"k": 7, "v": [8, 9]}
    with parquet.open_parquet_writer(chunks.append, field_ids=ids) as w:
        for base in (0, 10):
            w.write_row_group(_m(k=dn.vector_int64_from_sequence([base, base + 1]),
                                 v=dn.vector_array_from_sequence([[base], None])))
    data = b"".join(chunks)
    assert _arrow_ids(data) == {"k": [7], "v": [8, 9]}
    assert pq.read_table(io.BytesIO(data)).column("k").to_pylist() == [0, 1, 10, 11]


def test_patch_and_append_keep_the_ids():
    ids = {"k": 7, "s": 8}
    a = parquet.write_parquet(_m(k=dn.vector_int64_from_sequence([1]),
                                 s=vector_from_sequence(["x"], dtype="VARCHAR")), field_ids=ids)
    b = parquet.write_parquet(_m(k=dn.vector_int64_from_sequence([2]),
                                 s=vector_from_sequence(["y"], dtype="VARCHAR")), field_ids=ids)
    assert _arrow_ids(parquet.append_row_groups(a, b)) == {"k": [7], "s": [8]}
    assert _arrow_ids(parquet.patch_columns(a, rename={"s": "t"})) == {"k": [7], "t": [8]}
    # Different ids are different schemas: refused, never relabelled.
    c = parquet.write_parquet(_m(k=dn.vector_int64_from_sequence([3]),
                                 s=vector_from_sequence(["z"], dtype="VARCHAR")), field_ids={"k": 7, "s": 99})
    with pytest.raises(RuntimeError):
        parquet.append_row_groups(a, c)


@pytest.mark.parametrize("ids, match", [
    ({"k": 1}, r"field_ids has no id for column 'v'"),
    ({"k": 1, "v": [2, 3], "zz": 4}, r"field_ids names 'zz', which is not a column"),
    ({"k": 1, "v": 2}, r"column 'v' is an ARRAY of depth 1; field_ids\['v'\] must be a sequence of 2 ids"),
    ({"k": 1, "v": [2]}, r"must be a sequence of 2 ids"),
    ({"k": -1, "v": [2, 3]}, r"field_ids\['k'\] must be an int in 0..2\^31-1"),
    ({"k": 2**31, "v": [2, 3]}, r"field_ids\['k'\] must be an int"),
    ({"k": True, "v": [2, 3]}, r"field_ids\['k'\] must be an int"),
    ({"k": 1, "v": [2, "3"]}, r"field_ids\['v'\] must be an int"),
])
def test_refusals_name_the_column(ids, match):
    m = _m(k=dn.vector_int64_from_sequence([1]), v=dn.vector_array_from_sequence([[1]]))
    with pytest.raises(ValueError, match=match):
        parquet.write_parquet(m, field_ids=ids)
    with pytest.raises(ValueError, match=match):
        with parquet.open_parquet_writer(lambda b: None, field_ids=ids) as w:
            w.write_row_group(m)


def test_coalescing_append_needs_the_field_ids():
    ids = {"k": 7}
    a = parquet.write_parquet(_m(k=dn.vector_int64_from_sequence([1])), field_ids=ids)
    b = parquet.write_parquet(_m(k=dn.vector_int64_from_sequence([2])), field_ids=ids)
    # max_small_row_groups=0 forces every fragment to be coalesced (re-written).
    with pytest.raises(ValueError, match=r"pass the same field_ids="):
        parquet.append_row_groups(a, b, max_small_row_groups=0)
    out = parquet.append_row_groups(a, b, max_small_row_groups=0, field_ids=ids)
    assert _arrow_ids(out) == {"k": [7]}
    assert pq.read_table(io.BytesIO(out)).column("k").to_pylist() == [1, 2]
