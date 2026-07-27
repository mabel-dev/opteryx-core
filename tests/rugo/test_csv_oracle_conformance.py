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

A CSV-specific wrinkle gets its own dedicated section below: an unquoted
empty field is NULL, a quoted empty field ("") is an empty string. rugo's
reader honours that distinction (see `is_null = raw_len == 0 && !was_quoted`
in csv_column_builder.cpp) and PyArrow's writer emits it correctly, so the
READ-direction matrix exercises "" as a first-class value. rugo's WRITER
currently does NOT emit that distinguishing quote -- see
test_write_empty_string_collapses_to_null_KNOWN_GAP below.
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
    # column on an empty field — reasonable in general (it can't tell apart an
    # unquoted-empty NULL from a quoted-empty "" once both are set to null),
    # but WRITE_CASES never feeds "" through this path (see module docstring /
    # the KNOWN_GAP test below), so an empty field here is unambiguously NULL.
    table = pcsv.read_csv(
        io.BytesIO(data),
        parse_options=pcsv.ParseOptions(ignore_empty_lines=False),
        convert_options=pcsv.ConvertOptions(
            column_types={"v": pa_type}, strings_can_be_null=True
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

# Write-direction case list mirrors READ_CASES but keeps "" out of the string
# values — see test_write_empty_string_collapses_to_null_KNOWN_GAP for why.
WRITE_CASES = [
    ("int64", pa.int64(), "INT64", [-(2**63), 0, 2**63 - 1]),
    ("float64", pa.float64(), "DOUBLE", [-1.5, 0.0, 2.5]),
    ("string", pa.string(), "VARCHAR", ["plain", "has,comma", "a longer utf-8 ☃ string"]),
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
# KNOWN GAP: rugo's CSV writer does not distinguish "" from NULL.
#
# The reader tells them apart correctly (see is_null = raw_len==0 && !was_quoted
# in csv_column_builder.cpp, and test_read_matches_pyarrow[string-*] above,
# which round-trips PyArrow-quoted "" through rugo's reader intact). The
# writer, however, does not emit the distinguishing quote for an empty
# string, so both "" and NULL currently serialize to the same unquoted empty
# field and both come back as NULL on re-read. This is a real, reader/writer
# asymmetry — pinned down here rather than silently avoided so it stays
# visible. Flagged to the architect; not fixed as part of this test-only change.
# ─────────────────────────────────────────────────────────────────────────────


def test_write_empty_string_collapses_to_null_KNOWN_GAP():
    data = rugo_write_csv(["", "abc", None], "VARCHAR")
    got = rugo_read_csv(data)
    assert got == [None, "abc", None], (
        f"expected current (gap) behaviour [None, 'abc', None], got {got!r} — "
        "if this now reads ['', 'abc', None], the writer has started quoting "
        "empty strings: update this test (and READ_CASES/WRITE_CASES) to drop "
        "the KNOWN_GAP label and merge '' back into the shared case list."
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
