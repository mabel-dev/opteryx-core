"""
Rugo ⇄ PyArrow oracle conformance suite — the marketable-floor regression.

Rugo is a standalone Parquet/CSV/JSONL engine; its promise is that it reads and
writes Parquet that other tools produce and consume correctly. This suite pins
that promise down with PyArrow as the independent oracle (PyArrow is allowed in
tests only — CLAUDE.md §4):

  READ  : PyArrow writes a typed column  → rugo reads it      → values match.
  WRITE : rugo (re)writes that column     → PyArrow reads it   → values match.

Coverage is a deliberate matrix, not a spot check — every physical type rugo's
writer dispatches on, a representative sample of parameterised types (decimal
precision/scale, timestamp unit), single- and nested-array element types, and
for each: DENSE (all valid), SPARSE (some null), ALLNULL (typed, every value
null), and EMPTY (zero rows).

Comparison is at the LOGICAL-VALUE level: representation differences that don't
change the value (rugo hands timestamps back tz-aware, time as µs, decimal as
the unscaled integer, binary as str) are normalised away — but value corruption
(e.g. an unsigned integer coming back negative) is NEVER masked, so the floor
actually catches regressions.
"""

import datetime
import decimal
import io
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # test oracle only
import pyarrow.parquet as pq  # test oracle only

import draken  # noqa: F401 — must precede rugo native imports
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
import rugo.parquet as rp

# ─────────────────────────────────────────────────────────────────────────────
# Oracle plumbing
# ─────────────────────────────────────────────────────────────────────────────


def pa_write(table: "pa.Table", data_page_version=None) -> bytes:
    buf = io.BytesIO()
    if data_page_version is None:
        pq.write_table(table, buf, compression=None)
    else:
        pq.write_table(table, buf, compression=None, data_page_version=data_page_version)
    return buf.getvalue()


def pa_read(data: bytes) -> dict:
    return pq.read_table(io.BytesIO(data)).to_pydict()


def _morsels(data: bytes):
    return list(rp.read_parquet(data))


def rugo_read(data: bytes) -> dict:
    """rugo.read_parquet → {column_name: [values...]}."""
    out: dict = {}
    for m in _morsels(data):
        for name in m.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            col = m.column(name.encode() if isinstance(name, str) else name)
            out.setdefault(key, []).extend(col.to_pylist())
    return out


def rugo_reread_rewrite(data: bytes) -> dict:
    """PyArrow bytes → rugo read → rugo write → PyArrow read.

    Exercises rugo's WRITER over a genuine rugo-produced morsel and confirms the
    bytes it emits are read back identically by the oracle. (Conflates read+write
    by design — a clean single-morsel round-trip is the marketable guarantee.)
    """
    morsels = _morsels(data)
    assert len(morsels) == 1, (
        f"oracle expects small single-row-group fixtures, got {len(morsels)} morsels"
    )
    out_bytes = rp.write_parquet(morsels[0], compression="none", bloom_filters=False)
    return pa_read(out_bytes)


def rugo_reread_stream_rewrite(data: bytes) -> dict:
    """PyArrow bytes → rugo read → rugo STREAMING write → PyArrow read.

    Same guarantee as rugo_reread_rewrite but through open_parquet_writer. The
    streaming writer shares _encode's morsel→ColumnInput conversion with the
    one-shot write_parquet but diverges in row-group/footer assembly (see
    StreamingParquetWriter in _parquet_writer.hpp) — this floor test exists so a
    regression specific to that assembly can't slip past a rugo release.
    """
    morsels = _morsels(data)
    assert len(morsels) == 1, (
        f"oracle expects small single-row-group fixtures, got {len(morsels)} morsels"
    )
    chunks = []
    with rp.open_parquet_writer(chunks.append, compression="none", bloom_filters=False) as w:
        w.write_row_group(morsels[0])
    return pa_read(b"".join(chunks))


# ─────────────────────────────────────────────────────────────────────────────
# Logical-value normalisation (by kind)
# ─────────────────────────────────────────────────────────────────────────────


def _norm_scalar(kind: str, v):
    if v is None:
        return None
    if kind == "binary":
        # rugo hands VARBINARY back as str; PyArrow as bytes. Compare as bytes.
        return v.encode("utf-8") if isinstance(v, str) else bytes(v)
    if kind == "timestamp":
        # rugo attaches UTC tzinfo; PyArrow (unzoned) is naive. Same wall-clock.
        return v.replace(tzinfo=None) if isinstance(v, datetime.datetime) else v
    if kind == "time":
        # rugo returns integer µs-since-midnight; PyArrow a datetime.time.
        if isinstance(v, datetime.time):
            return ((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond
        return v
    if kind == "decimal":
        # rugo and PyArrow both return decimal.Decimal; equality is by value, so
        # differing exponents (0E-2 vs 0.00) still compare equal.
        return v
    return v


def normalise(kind: str, values: list) -> list:
    def rec(x):
        if isinstance(x, (list, tuple)):
            return [rec(e) for e in x]
        return _norm_scalar(kind, x)

    return [rec(v) for v in values]


# ─────────────────────────────────────────────────────────────────────────────
# Type matrix
# ─────────────────────────────────────────────────────────────────────────────
# Each case: (id, pyarrow_type, kind, dense_values). `kind` selects the value
# normaliser. `dense_values` is a 3-element all-valid sample; shape variants
# (sparse/allnull/empty) are derived from it.

_TS = datetime.datetime(2021, 6, 15, 13, 30, 45, 123456)
_D = datetime.date(2021, 6, 15)
_T = datetime.time(13, 30, 45, 123456)

SCALAR_CASES = [
    ("bool", pa.bool_(), "id", [True, False, True]),
    ("int8", pa.int8(), "id", [-128, 0, 127]),
    ("int16", pa.int16(), "id", [-32768, 0, 32767]),
    ("int32", pa.int32(), "id", [-(2**31), 0, 2**31 - 1]),
    ("int64", pa.int64(), "id", [-(2**63), 0, 2**63 - 1]),
    ("uint8", pa.uint8(), "id", [0, 200, 255]),
    ("uint16", pa.uint16(), "id", [0, 60000, 65535]),
    ("uint32", pa.uint32(), "id", [0, 4_000_000_000, 2**32 - 1]),
    ("uint64", pa.uint64(), "id", [0, 2**63 + 5, 2**64 - 1]),
    ("float32", pa.float32(), "id", [-1.5, 0.0, 2.5]),
    ("float64", pa.float64(), "id", [-1.5, 0.0, 2.5]),
    ("string", pa.string(), "id", ["", "abc", "a longer utf-8 ☃ string"]),
    ("binary", pa.binary(), "binary", [b"", b"xy", b"bytes"]),
    ("date32", pa.date32(), "id", [_D, datetime.date(1970, 1, 1), datetime.date(2400, 1, 1)]),
    ("ts_us", pa.timestamp("us"), "timestamp", [_TS, datetime.datetime(1970, 1, 1), _TS]),
    ("ts_ms", pa.timestamp("ms"), "timestamp",
     [_TS.replace(microsecond=123000), datetime.datetime(1970, 1, 1), _TS.replace(microsecond=0)]),
    ("time64_us", pa.time64("us"), "time", [_T, datetime.time(0, 0, 0), datetime.time(23, 59, 59, 999999)]),
    ("decimal_9_2", pa.decimal128(9, 2), "decimal",
     [decimal.Decimal("1.23"), decimal.Decimal("-9.99"), decimal.Decimal("0.00")]),
    ("decimal_38_2", pa.decimal128(38, 2), "decimal",
     [decimal.Decimal("1.23"), decimal.Decimal("-12345678901234567890.99"), decimal.Decimal("0.00")]),
]

# Array cases: single-level and nested, over int/uint/string leaves (the shapes
# that exercise the list reconstruction + unsigned-leaf + nesting paths).
ARRAY_CASES = [
    ("list_int64", pa.list_(pa.int64()), "id", [[1, 2], [], [3]]),
    ("list_uint64", pa.list_(pa.uint64()), "id", [[1, 2**63 + 7], [], [2**64 - 1]]),
    ("list_string", pa.list_(pa.string()), "id", [["a", "bb"], [], ["c"]]),
    ("list_float64", pa.list_(pa.float64()), "id", [[1.5, 2.5], [], [3.5]]),
    ("list_bool", pa.list_(pa.bool_()), "id", [[True, False], [], [True]]),
    ("nested_int64", pa.list_(pa.list_(pa.int64())), "id", [[[1], [2, 3]], [], [[4]]]),
    ("nested_uint64", pa.list_(pa.list_(pa.uint64())), "id",
     [[[1], [2**63 + 5]], [], [[2**64 - 1]]]),
]

ALL_CASES = SCALAR_CASES + ARRAY_CASES
SHAPES = ["dense", "sparse", "allnull", "empty"]


def _shape_values(dense: list, shape: str) -> list:
    if shape == "dense":
        return list(dense)
    if shape == "sparse":
        return [dense[0], None, dense[2]] if len(dense) >= 3 else [dense[0], None]
    if shape == "allnull":
        return [None, None, None]
    if shape == "empty":
        return []
    raise AssertionError(shape)


def _make_table(pa_type, values):
    return pa.table({"c": pa.array(values, type=pa_type)})


# ─────────────────────────────────────────────────────────────────────────────
# READ oracle: PyArrow writes → rugo reads → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
@pytest.mark.parametrize("shape", SHAPES)
def test_read_matches_pyarrow(case, shape):
    case_id, pa_type, kind, dense = case
    values = _shape_values(dense, shape)
    data = pa_write(_make_table(pa_type, values))

    got = rugo_read(data)
    if shape == "empty":
        # rugo may legitimately emit zero morsels for an empty file.
        assert got.get("c", []) == []
        return
    assert "c" in got, f"rugo dropped column for {case_id}/{shape}"
    assert normalise(kind, got["c"]) == normalise(kind, values), (
        f"READ mismatch {case_id}/{shape}: rugo={got['c']!r} expected={values!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# WRITE oracle: rugo writes → PyArrow reads → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
@pytest.mark.parametrize("shape", ["dense", "sparse", "allnull"])
def test_write_roundtrips_through_pyarrow(case, shape):
    case_id, pa_type, kind, dense = case
    values = _shape_values(dense, shape)
    data = pa_write(_make_table(pa_type, values))

    got = rugo_reread_rewrite(data)
    assert "c" in got, f"rugo write dropped column for {case_id}/{shape}"
    assert normalise(kind, got["c"]) == normalise(kind, values), (
        f"WRITE round-trip mismatch {case_id}/{shape}: rugo→pa={got['c']!r} expected={values!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING WRITE oracle: rugo streams (open_parquet_writer) → PyArrow reads →
# values match. Mirrors test_write_roundtrips_through_pyarrow exactly but over
# the streaming encode path, since it diverges from the one-shot writer in
# row-group/footer assembly (not in per-column encoding).
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
@pytest.mark.parametrize("shape", ["dense", "sparse", "allnull"])
def test_streaming_write_roundtrips_through_pyarrow(case, shape):
    case_id, pa_type, kind, dense = case
    values = _shape_values(dense, shape)
    data = pa_write(_make_table(pa_type, values))

    got = rugo_reread_stream_rewrite(data)
    assert "c" in got, f"rugo streaming write dropped column for {case_id}/{shape}"
    assert normalise(kind, got["c"]) == normalise(kind, values), (
        f"STREAMING WRITE round-trip mismatch {case_id}/{shape}: "
        f"rugo→pa={got['c']!r} expected={values!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# READ oracle — DATA_PAGE_V2: PyArrow writes V2 pages → rugo reads → values match
# ─────────────────────────────────────────────────────────────────────────────
# PyArrow's default page format is V1; passing data_page_version="2.0" makes it
# emit DATA_PAGE_V2 (page type 3), where the repetition/definition levels are
# stored uncompressed at the front of the page with explicit byte lengths and no
# 4-byte prefix (the V1 behaviour). Nulls (sparse) are the key risk: def levels
# are encoded in a separate region. This mirrors test_read_matches_pyarrow but
# over a representative type subset and only the V2 writer path.

_V2_IDS = {"int32", "int64", "uint64", "float64", "string", "bool", "list_int64"}
V2_CASES = [c for c in ALL_CASES if c[0] in _V2_IDS]
V2_SHAPES = ["dense", "sparse", "empty"]


@pytest.mark.parametrize("case", V2_CASES, ids=[c[0] for c in V2_CASES])
@pytest.mark.parametrize("shape", V2_SHAPES)
def test_read_v2_matches_pyarrow(case, shape):
    case_id, pa_type, kind, dense = case
    values = _shape_values(dense, shape)
    data = pa_write(_make_table(pa_type, values), data_page_version="2.0")

    got = rugo_read(data)
    if shape == "empty":
        assert got.get("c", []) == []
        return
    assert "c" in got, f"rugo dropped column for {case_id}/{shape} (V2)"
    assert normalise(kind, got["c"]) == normalise(kind, values), (
        f"READ V2 mismatch {case_id}/{shape}: rugo={got['c']!r} expected={values!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Multi-row-group: the rest of this suite's fixtures are 3-row / single-row-group
# tables — rugo_reread_rewrite even asserts len(morsels) == 1. Row-group boundary
# handling (row-group-relative validity-bit offsets, per-row-group stats, morsel
# splitting on read) is untested by every case above. A contrived 50-row file
# split across 5 row groups exercises that boundary explicitly, both directions.
# ─────────────────────────────────────────────────────────────────────────────

_MRG_N = 50
_MRG_IDS = list(range(_MRG_N))
_MRG_VALS = [float(i) * 1.5 if i % 7 else None for i in range(_MRG_N)]
_MRG_NAMES = [f"name_{i}" if i % 5 else None for i in range(_MRG_N)]


def test_read_multi_rowgroup_matches_pyarrow():
    """PyArrow writes 50 rows in 5 row groups (row_group_size=10) → rugo reads
    → one morsel per row group, values match in order across the boundary."""
    table = pa.table(
        {
            "id": pa.array(_MRG_IDS, type=pa.int64()),
            "val": pa.array(_MRG_VALS, type=pa.float64()),
            "name": pa.array(_MRG_NAMES, type=pa.string()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=None, row_group_size=10)
    data = buf.getvalue()

    pf = pq.ParquetFile(io.BytesIO(data))
    assert pf.num_row_groups == 5, "fixture setup: expected 5 row groups"

    morsels = _morsels(data)
    assert len(morsels) == 5, f"expected one rugo morsel per row group, got {len(morsels)}"
    assert [m.num_rows for m in morsels] == [10, 10, 10, 10, 10]

    ids, vals, names = [], [], []
    for m in morsels:
        ids.extend(m.column(b"id").to_pylist())
        vals.extend(m.column(b"val").to_pylist())
        names.extend(m.column(b"name").to_pylist())

    assert ids == _MRG_IDS
    assert vals == _MRG_VALS
    assert names == _MRG_NAMES


def test_write_multi_rowgroup_roundtrips_through_pyarrow():
    """rugo writes 40 rows with max_rows_per_row_group=8 → 5 row groups →
    PyArrow reads back, values match in order across the boundary.

    40/8 (not 50/10): WriteParquet rounds max_rows_per_rg up to the nearest
    multiple of 8 so validity bit-offsets stay byte-aligned (see the
    max_rows_per_rg contract in rugo/src/parquet/_parquet_writer.hpp) — asking
    for 10 would silently round up to 16 and only produce 4 groups, not 5.
    """
    n = 40
    ids = list(range(n))
    vals = [float(i) * 1.5 if i % 7 else None for i in range(n)]
    names = [f"name_{i}" if i % 5 else None for i in range(n)]

    morsel = Morsel.from_vectors(
        ["id", "val", "name"],
        [
            vector_from_sequence(ids, dtype="INT64"),
            vector_from_sequence(vals, dtype="DOUBLE"),
            vector_from_sequence(names, dtype="VARCHAR"),
        ],
    )

    data = rp.write_parquet(morsel, compression="none", bloom_filters=False, max_rows_per_row_group=8)

    pf = pq.ParquetFile(io.BytesIO(data))
    assert pf.num_row_groups == 5, f"expected 5 row groups, got {pf.num_row_groups}"
    assert [pf.metadata.row_group(i).num_rows for i in range(5)] == [8, 8, 8, 8, 8]

    got = pa_read(data)
    assert got["id"] == ids
    assert got["val"] == vals
    assert got["name"] == names


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
