# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A file may store a column NARROWER than the relation's schema declares — that is
a legal schema evolution, and exactly what `ALTER COLUMN ... TYPE` leaves behind,
since it rewrites no data. Before the scan coerced those files to the declared
width, their vectors reached the engine at the stored width and the first
operation that had to put two files' columns together failed with
"concat: all inputs must share one type" — which is how an
`OPTIMIZE TABLE public.geopolitics.gdelt_events` died on 2026-09-05.

The widening policy is `is_legal_widen`, the same predicate ALTER is checked
against: strictly up one ladder (signed int / unsigned int / float), and never a
type carrying a logical descriptor. A mismatch that is NOT a legal widening must
still fail — the last test here is the one that keeps this from becoming a
silent-coercion hole.

PyArrow is the writer (test-only dependency): it is the straightforward way to
put two different physical widths for one column into two files.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _write(folder: str, name: str, values, arrow_type):
    os.makedirs(folder, exist_ok=True)
    pq.write_table(
        pa.table({"n": pa.array(values, arrow_type)}), os.path.join(folder, name)
    )


def _read(folder: str):
    """Return (rows, the result column's type name). Forces the concat that the
    delivery buffer performs when several small morsels are combined."""
    session = opteryx.session()
    values = []
    type_names = set()
    for morsel in session.execute_to_morsels(f"SELECT n FROM {folder}"):
        if morsel is None or morsel.num_rows == 0:
            continue
        column = morsel.column(morsel.column_names[0])
        type_names.add(column.type.name)
        values.extend(column[i] for i in range(morsel.num_rows))
    return values, type_names


def test_narrow_file_widens_to_the_declared_int_width():
    """INT32 file + INT64 declared: read as one relation, at the declared width."""
    folder = "widen_int_tmp"
    try:
        # The FIRST file supplies the declared schema, so the wide one is named
        # to sort first — this is the ALTER-widened relation's shape.
        _write(folder, "a_wide.parquet", [1, 2, 3], pa.int64())
        _write(folder, "b_narrow.parquet", [4, 5], pa.int32())
        values, type_names = _read(folder)
        assert sorted(values) == [1, 2, 3, 4, 5], values
        assert type_names == {"INT64"}, type_names
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_narrow_file_widens_to_the_declared_float_width():
    """FLOAT32 file + FLOAT64 declared: the float ladder, same rule."""
    folder = "widen_float_tmp"
    try:
        _write(folder, "a_wide.parquet", [1.5, 2.5], pa.float64())
        _write(folder, "b_narrow.parquet", [4.5], pa.float32())
        values, type_names = _read(folder)
        assert sorted(values) == [1.5, 2.5, 4.5], values
        assert type_names == {"FLOAT64"}, type_names
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_single_width_relation_is_untouched():
    """The common path: every file already at the declared width, no coercion."""
    folder = "widen_noop_tmp"
    try:
        _write(folder, "a.parquet", [1, 2], pa.int64())
        _write(folder, "b.parquet", [3], pa.int64())
        values, type_names = _read(folder)
        assert sorted(values) == [1, 2, 3], values
        assert type_names == {"INT64"}, type_names
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_unwidenable_mismatch_still_fails():
    """A divergence that is NOT a legal widening must still fail, loudly.

    VARCHAR against INT64 is on no ladder, so nothing coerces it — this is the
    test that keeps the coercion from becoming a silent-mismatch hole. The error
    names the column and both types, so the failure is diagnosable from the
    message alone (it was not: it used to say only "all inputs must share one
    type", which is the whole of what an OPTIMIZE of gdelt_events reported).

    A declared width NARROWER than a file's stored width is the other refusal
    `is_legal_widen` makes, and it is covered at the mechanism instead of here:
    a relation whose schema is derived from these files never declares narrower
    than the files store, so it cannot be built with the writer available to
    this test.
    """
    folder = "widen_mismatch_tmp"
    try:
        _write(folder, "a.parquet", [1, 2], pa.int64())
        _write(folder, "b.parquet", ["x"], pa.string())
        with pytest.raises(Exception) as err:
            _read(folder)
        message = str(err.value)
        assert "must share one type" in message, message
        assert "'n'" in message, message
        assert "VARCHAR" in message and "INT64" in message, message
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_widening_refuses_a_narrowing_and_a_descriptor_type():
    """The mechanism's own refusals, at the level a relation cannot reach.

    `vector_widen` is the single widening primitive both scan paths drive. A
    narrowing and a descriptor-carrying source are the two cases that must never
    be coerced — a narrowing can lose a value, and a width cast drops the
    descriptor (a DECIMAL's scale, a TIMESTAMP's unit, IPv4-ness) that gives the
    values their meaning.
    """
    import decimal

    from draken.draken_native import DrakenType, vector_widen
    from draken.interop.vector_sequence import vector_from_sequence

    with pytest.raises(Exception) as err:
        vector_widen(vector_from_sequence([1], DrakenType.INT64), DrakenType.INT32)
    assert "is not a widening" in str(err.value)

    with pytest.raises(Exception) as err:
        vector_widen(
            vector_from_sequence([decimal.Decimal("1.5")], DrakenType.DECIMAL),
            DrakenType.INT64,
        )
    assert "logical-type descriptor" in str(err.value)


def test_widening_preserves_nulls_and_values():
    """The widened column keeps every row, null rows included."""
    from draken.draken_native import DrakenType, vector_widen
    from draken.interop.vector_sequence import vector_from_sequence

    widened = vector_widen(
        vector_from_sequence([1, None, 3], DrakenType.INT32), DrakenType.INT64
    )
    assert widened.type == DrakenType.INT64
    assert [widened[i] for i in range(3)] == [1, None, 3]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
