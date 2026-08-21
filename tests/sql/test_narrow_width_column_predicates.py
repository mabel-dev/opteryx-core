# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Predicates over 4-byte columns must return the same rows as their 8-byte peers.

WHAT THIS PINS. A scan declares a column's type at PLAN time and the expression
compiler binds its kernels from that declared physical type; the reader then
decodes the column at the width the FILE stores. When the two disagree the kernel
reads eight bytes per element out of a four-byte buffer and answers from whatever
follows it in memory — no error, no pruning, just wrong rows.

`_rugo_schema._integer_column_type` already declared parquet's integer widths
exactly for this reason. The float side did not: `PARQUET_PHYSICAL_TYPE_MAP`
resolved float32 and float64 alike to `LogicalCategory.FLOAT`, and
`_CATEGORY_TO_CANONICAL` hands that back as FLOAT64. So every parquet float32
column bound as FLOAT64 over a FLOAT32 vector. Measured on the five rows below,
before `_float_column_type` existed:

    WHERE f32 > 0.25   ->  0 rows (3 correct)
    WHERE f32 = 0.5    ->  0 rows (1 correct)

⛔ THE TRAP: THIS TEST MUST MATERIALISE ROWS. Every one of those failing cases
returns the CORRECT answer to `SELECT COUNT(*) ... WHERE <pred>` — the count is
answered without the row-shaped path that mis-reads the buffer. A COUNT-based
assertion passes with the defect fully present. Assert on the returned VALUES.

The same divergence is reachable from a catalog rather than a file: a catalog
that declares a 4-byte column as its 8-byte namesake produces exactly this
failure (opteryx-iceberg <= 0.1.3 mapped Iceberg IntegerType/FloatType onto
INT64/FLOAT64 and did precisely that). Nothing in the engine detects the
mismatch, which is why the fix is to declare the true width rather than to
tolerate a wrong one.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import opteryx

# The fixture is written with PyArrow, not rugo, deliberately: what is under
# test is opteryx INFERRING a column's type from a file it did not write, so the
# file must come from an independent writer. (rugo's writer widened FLOAT32 to a
# parquet float64 column until 2026-08-21, which would have made a rugo-written
# fixture test FLOAT64 twice; that is fixed — see
# `tests/rugo/test_parquet_float_width_roundtrip.py` — but an outside oracle is
# still the right fixture here.) PyArrow is sanctioned in `tests/` (CLAUDE.md
# §4) for exactly this: generating data the engine must then read on its own.
import pyarrow as pa
import pyarrow.parquet as pq

# Row-aligned. The float values are all exactly representable in binary32, so a
# failure here is a WIDTH failure and can never be read as a rounding artefact —
# the same matrix run with awkward values (0.1 .. 0.5) failed identically.
VALUES = {
    "i64": [1, 2, 3, 4, 5],
    "i32": [1, 2, 3, 4, 5],
    "f64": [6.5, 7.0, 8.5, 9.0, 10.5],
    "f32": [0.125, 0.25, 0.5, 0.75, 1.0],
    "s": ["a", "b", "c", "d", "e"],
}

# (column, sql literal, python literal). Both `>` and `=` are run against each.
CASES = [
    ("i64", "3", 3),
    ("i32", "3", 3),
    ("f64", "8.5", 8.5),
    ("f32", "0.25", 0.25),
    ("s", "'b'", "b"),
]

# The width each column must be BOUND at. INT32/FLOAT32 here are the whole point:
# a plan that widens either of them to its 8-byte namesake reproduces the defect.
EXPECTED_TYPES = {
    "i64": dn.DrakenType.INT64,
    "i32": dn.DrakenType.INT32,
    "f64": dn.DrakenType.FLOAT64,
    "f32": dn.DrakenType.FLOAT32,
    "s": dn.DrakenType.VARCHAR,
}


def _table():
    return pa.table(
        {
            "i64": pa.array(VALUES["i64"], pa.int64()),
            "i32": pa.array(VALUES["i32"], pa.int32()),
            "f64": pa.array(VALUES["f64"], pa.float64()),
            "f32": pa.array(VALUES["f32"], pa.float32()),
            "s": pa.array(VALUES["s"], pa.string()),
        }
    )


@pytest.fixture(scope="module")
def dataset():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "widths")
        os.makedirs(data_dir)
        pq.write_table(_table(), os.path.join(data_dir, "data.parquet"))
        yield data_dir


def _rows(dataset, column, sql):
    """The VALUES of `column` for every row the query returns — not a count.

    A count is answered without the row-shaped read that carries this defect; see
    the module docstring.
    """
    session = opteryx.session()
    collected = []
    for morsel in session.execute_to_morsels(sql):
        if morsel.num_rows == 0:
            continue
        collected.extend(morsel.column(column.encode()).to_pylist())
    return collected


def _expected(column, op, literal):
    keep = (lambda a, b: a > b) if op == ">" else (lambda a, b: a == b)
    return [v for v in VALUES[column] if keep(v, literal)]


@pytest.mark.parametrize("op", [">", "="])
@pytest.mark.parametrize("column,sql_literal,py_literal", CASES)
def test_predicate_returns_the_right_rows(dataset, column, sql_literal, py_literal, op):
    sql = f"SELECT * FROM '{dataset}' WHERE {column} {op} {sql_literal}"
    expected = _expected(column, op, py_literal)
    actual = _rows(dataset, column, sql)
    if column == "s":
        actual = [v.decode() if isinstance(v, bytes) else v for v in actual]
    assert sorted(actual) == sorted(expected), f"{sql} returned {actual}, want {expected}"


@pytest.mark.parametrize("op", [">", "="])
@pytest.mark.parametrize("column,sql_literal,py_literal", CASES)
def test_predicate_row_count_agrees_with_the_count_form(
    dataset, column, sql_literal, py_literal, op
):
    """`SELECT *` and `SELECT COUNT(*)` must agree under the same WHERE.

    They did NOT under the defect: the count was right while the rows were wrong.
    Pinning the agreement catches a regression that only reopens one of the two
    paths.
    """
    session = opteryx.session()
    where = f"WHERE {column} {op} {sql_literal}"
    counted = None
    for morsel in session.execute_to_morsels(
        f"SELECT COUNT(*) AS c FROM '{dataset}' {where}"
    ):
        counted = morsel.column(b"c").to_pylist()[0]
    assert counted == len(_rows(dataset, column, f"SELECT * FROM '{dataset}' {where}"))
    assert counted == len(_expected(column, op, py_literal))


@pytest.mark.parametrize("column", list(EXPECTED_TYPES))
def test_scan_binds_the_columns_real_width(dataset, column):
    """The narrow columns must reach execution at their stored width.

    This is the cause, not the symptom: `test_predicate_returns_the_right_rows`
    only fails once a widened binding meets a kernel that reads past the buffer,
    which is data-dependent. A widened BINDING is always wrong.
    """
    session = opteryx.session()
    for morsel in session.execute_to_morsels(f"SELECT * FROM '{dataset}'"):
        index = morsel.column_names.index(column.encode())
        assert morsel.column_types[index] == EXPECTED_TYPES[column]
        return
    pytest.fail("scan produced no morsel")


def test_the_fixture_actually_carries_narrow_columns(dataset):
    """Guards the fixture: if the file ever stopped STORING 4-byte columns, every
    case above would still pass while testing nothing about narrow reads. Read
    the footer rugo itself reports, not the arrow table we handed the writer."""
    import rugo.parquet as rp

    stored = {
        column.name: column.physical_type
        for column in rp.read_metadata(os.path.join(dataset, "data.parquet")).schema_columns
    }
    assert stored["i32"] == "int32", stored
    assert stored["f32"] == "float32", stored
    assert stored["i64"] == "int64", stored
    assert stored["f64"] == "float64", stored


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
