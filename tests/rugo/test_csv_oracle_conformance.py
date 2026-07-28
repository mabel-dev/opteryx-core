# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rugo <-> PyArrow oracle conformance suite for CSV — mirrors
test_oracle_conformance.py's Parquet matrix, scoped to what CSV (an untyped
text format) actually carries: PyArrow is the independent oracle for typed
CSV generation and parsing (allowed in tests only — CLAUDE.md SS4).

  READ  : PyArrow writes a typed column -> rugo reads it      -> values match.
  WRITE : rugo writes that column        -> PyArrow reads it   -> values match.

Type scope is deliberately narrow: rugo's CSV reader infers exactly three
column types -- int64, float64, VARCHAR (see test_csv_reader.py's docstring
and rugo/src/csv/core/csv_column_builder.cpp) -- so that is the whole matrix.
There is no bool/date/decimal inference to test; CSV doesn't carry a schema.

Shapes: DENSE (all valid), SPARSE (one null), ALLNULL (every value null), and
EMPTY (zero rows), matching the Parquet oracle's convention.

A CSV-specific wrinkle: an unquoted empty field is NULL, a quoted empty
field ("") is an empty string. Both rugo's reader (see
`is_null = raw_len == 0 && !was_quoted` in csv_column_builder.cpp) and
rugo's writer (see `csv_field` in draken/interop/value_format.hpp, which
quotes zero-length fields) honour that distinction, matching PyArrow, so
"" is exercised as a first-class value in both the READ- and
WRITE-direction matrices.
"""

import io
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # test oracle only
import pyarrow.csv as pcsv  # test oracle only

import draken  # noqa: F401 — must precede rugo native imports
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.csv import write_csv
from rugo.rugo_native import read_csv as _read_csv

# ─────────────────────────────────────────────────────────────────────────────
# Oracle plumbing
# ─────────────────────────────────────────────────────────────────────────────


def pa_write_csv(pa_type, values) -> bytes:
    table = pa.table({"v": pa.array(values, type=pa_type)})
    buf = io.BytesIO()
    pcsv.write_csv(table, buf)
    return buf.getvalue()


def rugo_read_csv(data: bytes) -> list:
    r = _read_csv(data)
    assert r["success"], "rugo read_csv reported failure"
    if not r["columns"]:
        return []
    return r["columns"][0].to_pylist()


def rugo_write_csv(values, dtype_name: str) -> bytes:
    vec = vector_from_sequence(values, dtype=dtype_name)
    morsel = Morsel.from_vectors(["v"], [vec])
    return write_csv(morsel)


def pa_read_csv_typed(data: bytes, pa_type) -> list:
    # ignore_empty_lines=False: a single-column all-empty-field row IS a blank
    # line, and PyArrow's default (True) silently drops blank lines rather than
    # reading them as one NULL-field row. Without this, an ALLNULL/SPARSE
    # single-column round-trip loses rows on the oracle's read side, not
    # rugo's write side — this is a PyArrow parsing default, not a rugo gap.
    # strings_can_be_null=True: PyArrow's default (False) never nulls a string
    # column on an empty field. quoted_strings_can_be_null=False: PyArrow's
    # default (True) nulls a quoted empty field too, collapsing "" into NULL;
    # setting it False makes PyArrow distinguish a quoted empty field ("")
    # from an unquoted empty field (NULL) — matching rugo's writer, which
    # quotes "" but never emits anything for NULL.
    table = pcsv.read_csv(
        io.BytesIO(data),
        parse_options=pcsv.ParseOptions(ignore_empty_lines=False),
        convert_options=pcsv.ConvertOptions(
            column_types={"v": pa_type},
            strings_can_be_null=True,
            quoted_strings_can_be_null=False,
        ),
    )
    return table.to_pydict()["v"]


# ─────────────────────────────────────────────────────────────────────────────
# Type matrix — rugo's CSV reader only ever infers int64 / float64 / VARCHAR.
# ─────────────────────────────────────────────────────────────────────────────

READ_CASES = [
    ("int64", pa.int64(), "INT64", [-(2**63), 0, 2**63 - 1]),
    ("float64", pa.float64(), "DOUBLE", [-1.5, 0.0, 2.5]),
    # "" is deliberately included here: PyArrow's writer quotes it, rugo's
    # reader must tell it apart from an unquoted-empty NULL (see module docstring).
    ("string", pa.string(), "VARCHAR", ["", "abc", "a longer utf-8 ☃ string, with a comma"]),
]

WRITE_CASES = [
    ("int64", pa.int64(), "INT64", [-(2**63), 0, 2**63 - 1]),
    ("float64", pa.float64(), "DOUBLE", [-1.5, 0.0, 2.5]),
    ("string", pa.string(), "VARCHAR", ["", "abc", "a longer utf-8 ☃ string, with a comma"]),
]

SHAPES = ["dense", "sparse", "allnull", "empty"]


def _shape_values(dense: list, shape: str) -> list:
    if shape == "dense":
        return list(dense)
    if shape == "sparse":
        return [dense[0], None, dense[2]]
    if shape == "allnull":
        return [None, None, None]
    if shape == "empty":
        return []
    raise AssertionError(shape)


# ─────────────────────────────────────────────────────────────────────────────
# READ oracle: PyArrow writes → rugo reads → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", READ_CASES, ids=[c[0] for c in READ_CASES])
@pytest.mark.parametrize("shape", SHAPES)
def test_read_matches_pyarrow(case, shape):
    case_id, pa_type, _dtype_name, dense = case
    values = _shape_values(dense, shape)
    data = pa_write_csv(pa_type, values)

    got = rugo_read_csv(data)
    if shape == "empty":
        assert got == []
        return
    assert got == values, f"READ mismatch {case_id}/{shape}: rugo={got!r} expected={values!r}"


# ─────────────────────────────────────────────────────────────────────────────
# WRITE oracle: rugo writes → PyArrow reads → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", WRITE_CASES, ids=[c[0] for c in WRITE_CASES])
@pytest.mark.parametrize("shape", SHAPES)
def test_write_roundtrips_through_pyarrow(case, shape):
    case_id, pa_type, dtype_name, dense = case
    values = _shape_values(dense, shape)
    data = rugo_write_csv(values, dtype_name)

    got = pa_read_csv_typed(data, pa_type)
    assert got == values, (
        f"WRITE round-trip mismatch {case_id}/{shape}: rugo->pa={got!r} expected={values!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Regression: rugo's CSV writer must distinguish "" from NULL, round-tripping
# through both rugo's own reader and PyArrow's. See `csv_field` in
# draken/interop/value_format.hpp (quotes zero-length fields) and
# `is_null = raw_len == 0 && !was_quoted` in csv_column_builder.cpp.
# ─────────────────────────────────────────────────────────────────────────────


def test_write_empty_string_distinguished_from_null():
    data = rugo_write_csv(["", "abc", None], "VARCHAR")

    got_rugo = rugo_read_csv(data)
    assert got_rugo == ["", "abc", None], (
        f"rugo round-trip: expected ['', 'abc', None], got {got_rugo!r}"
    )

    got_pa = pa_read_csv_typed(data, pa.string())
    assert got_pa == ["", "abc", None], (
        f"PyArrow round-trip: expected ['', 'abc', None], got {got_pa!r}"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
