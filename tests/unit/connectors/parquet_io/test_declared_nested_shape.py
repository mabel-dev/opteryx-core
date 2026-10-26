# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The Parquet footer gate and a nested column's DECLARED shape
(docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.3, option (c)).

A catalog table declares the shape of a nested column; the scan renders it with the TABLE's member names,
matching each file's fields by field id - so a file written before a rename, an add or a drop reads as
the table says. The gate only refuses a file that cannot be matched at all: no field ids, a different
kind of thing under an id, or none of a struct's declared fields present. The end-to-end reads are in
opteryx-iceberg's tests/test_nested_columns.py. pyarrow writes the files, with Iceberg-style field ids in
the schema metadata."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
from opteryx.types.schema import NestedField


def _f(name, typ, field_id):
    return pa.field(name, typ, metadata={b"PARQUET:field_id": str(field_id).encode()})


def _write(path, a_name="a", extra=False):
    members = [_f(a_name, pa.int64(), 11), _f("b", pa.string(), 12)]
    if extra:
        members.append(_f("c", pa.int64(), 13))
    schema = pa.schema(
        [
            _f("id", pa.int64(), 1),
            _f("s", pa.struct(members), 2),
            _f("l", pa.list_(_f("element", pa.struct([_f("k", pa.int64(), 15)]), 14)), 3),
        ]
    )
    rows = {"id": [1], "s": [{m.name: None for m in members}], "l": [[{"k": 1}]]}
    pq.write_table(pa.table(rows, schema=schema), str(path))
    return str(path)


DECLARED_S = NestedField(
    2, "s", "struct", (NestedField(11, "a", "scalar"), NestedField(12, "b", "scalar"))
).flatten()
DECLARED_L = NestedField(
    3, "l", "list", (NestedField(14, "element", "struct", (NestedField(15, "k", "scalar"),)),)
).flatten()


def _gate(path, declared, columns=("s",)):
    return native_scan_rejection([path], list(columns), ["varchar"] * len(columns), declared_shapes=declared)


def test_a_file_that_says_what_the_table_declares_is_admitted(tmp_path):
    path = _write(tmp_path / "ok.parquet")
    assert _gate(path, {"s": DECLARED_S}) is None
    assert _gate(path, {"l": DECLARED_L}, ("l",)) is None


def test_a_renamed_member_is_admitted_because_fields_are_matched_by_id(tmp_path):
    path = _write(tmp_path / "old.parquet", a_name="old_a")
    assert _gate(path, {"s": DECLARED_S}) is None


def test_a_member_the_file_lacks_or_has_extra_is_admitted(tmp_path):
    assert _gate(_write(tmp_path / "wide.parquet", extra=True), {"s": DECLARED_S}) is None   # dropped since
    wider = NestedField(
        2, "s", "struct",
        (NestedField(11, "a", "scalar"), NestedField(12, "b", "scalar"), NestedField(77, "new", "scalar")),
    ).flatten()
    assert _gate(_write(tmp_path / "narrow.parquet"), {"s": wider}) is None                  # added since


def test_a_declared_struct_none_of_whose_fields_are_in_the_file_is_refused(tmp_path):
    path = _write(tmp_path / "none.parquet")
    none = NestedField(2, "s", "struct", (NestedField(88, "x", "scalar"), NestedField(89, "y", "scalar"))).flatten()
    with pytest.raises(NotImplementedError, match=r"none of the fields the table declares for 's' is in this file"):
        _gate(path, {"s": none})


def test_a_different_kind_under_a_field_id_is_refused(tmp_path):
    path = _write(tmp_path / "kind.parquet")
    wrong = NestedField(2, "s", "struct", (NestedField(11, "a", "struct", (NestedField(5, "z", "scalar"),)),
                                           NestedField(12, "b", "scalar"))).flatten()
    with pytest.raises(NotImplementedError, match=r"declares 'a' \(id 11\) as a struct but the file has a scalar"):
        _gate(path, {"s": wrong})


def test_the_top_level_field_id_must_agree(tmp_path):
    path = _write(tmp_path / "top.parquet")
    wrong = NestedField(99, "s", "struct", (NestedField(11, "a", "scalar"), NestedField(12, "b", "scalar"))).flatten()
    with pytest.raises(NotImplementedError, match=r"declares 's' as field id 99 but the file has id 2"):
        _gate(path, {"s": wrong})


def test_a_file_without_field_ids_cannot_be_matched(tmp_path):
    path = str(tmp_path / "bare.parquet")
    pq.write_table(pa.table({"id": [1], "s": [{"a": 1, "b": "x"}]}), path)
    with pytest.raises(NotImplementedError, match=r"carries no field id"):
        _gate(path, {"s": DECLARED_S})


def test_no_declared_shape_means_the_file_is_the_schema(tmp_path):
    path = _write(tmp_path / "free.parquet", a_name="whatever")
    assert _gate(path, None) is None
