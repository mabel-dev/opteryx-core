"""
Tests for the parquet reader's decode correctness on highly-repetitive
dict-encoded columns.

The reader's decoder detects run-length structure in dict-encoded pages
internally (see rle_run_lengths / _expand_rle_int64_into in
rugo/src/parquet/parquet_reader.pxi) and uses it to expand values faster than
a naive per-row dict lookup. Per the Vector Model contract (CLAUDE.md §11):
"RLE does not exist past the scan boundary. Rugo expands RLE into one of the
[dense/constant/dict] shapes above before handing data to the execution
engine." So the returned Vector never reports an "RLE" shape — there is no
`.encoding` property to assert against. What IS worth verifying, and what
these tests check, is that highly-repetitive data decodes to the same correct
values, aggregates, and derived vectors (take/equals) as a naive path would.
"""

import sys
from pathlib import Path
from array import array

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rugo.rugo_native as rp
import draken.draken_native as dn
from draken.vectors.vector import Vector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(table: pa.Table) -> bytes:
    """Write PyArrow Table to in-memory parquet bytes."""
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, use_dictionary=True, compression=None)
    return buf.getvalue().to_pybytes()


def _read_columns(raw: bytes, col_names: list) -> dict:
    """Read named columns from raw parquet bytes. Returns {name: vector}."""
    all_cols = [c.name for c in rp.read_metadata_from_bytes(raw).schema_columns]
    morsels = rp.read_parquet(raw, all_cols)
    out = {}
    for morsel in morsels:
        for name in col_names:
            if name not in out:
                out[name] = morsel.column(name)
            # First row group wins; multiple groups are concatenated by caller if needed
    return out


def _ref_values(raw: bytes, col_name: str) -> list:
    """Read column via PyArrow as reference (ground truth)."""
    tbl = pq.read_table(pa.BufferReader(raw), columns=[col_name])
    return tbl[col_name].to_pylist()


def _equals_mask(vec, value) -> list:
    """Elementwise vec == value, via a constant vector (Vector has no bare
    scalar-comparison method — equals_vector compares two Vectors)."""
    const = Vector(dn.vector_from_constant(value, len(vec)))
    return vec.equals_vector(const).to_pylist()


# ---------------------------------------------------------------------------
# Synthetic parquet builders
# ---------------------------------------------------------------------------

def _build_rle_eligible_int64_parquet() -> bytes:
    """
    Int64 column with 5 distinct values, each repeated 100 times.
    Dictionary: [10, 20, 30, 40, 50]
    Rows: [10]*100 + [20]*100 + [30]*100 + [40]*100 + [50]*100 = 500 rows
    Runs: 5 → 5*4=20 < 500 → internal RLE decode path fires.

    Must be non-nullable so parquet omits definition-level pages — otherwise
    the Cython decoder populates valid_bits even for all-present columns,
    which disqualifies the RLE fast path.
    """
    values = []
    for v in [10, 20, 30, 40, 50]:
        values.extend([v] * 100)
    schema = pa.schema([pa.field("score", pa.int64(), nullable=False)])
    tbl = pa.table({"score": pa.array(values, type=pa.int64())}, schema=schema)
    return _write_parquet(tbl)


def _build_high_cardinality_int64_parquet() -> bytes:
    """
    Int64 column where every value is unique → no runs → plain dict path.
    Non-nullable to match the same definition-level conditions as the RLE test.
    """
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    tbl = pa.table({"id": pa.array(list(range(500)), type=pa.int64())}, schema=schema)
    return _write_parquet(tbl)


def _build_nullable_int64_parquet() -> bytes:
    """
    Int64 column with nulls and high repetition.
    Nullable — definition levels are written, so valid_bits.size() > 0,
    which disqualifies the RLE fast path → decoded via the plain dict path.
    """
    values = [10 if i % 2 == 0 else None for i in range(500)]
    tbl = pa.table({"val": pa.array(values, type=pa.int64())})
    return _write_parquet(tbl)


def _build_rle_eligible_int32_parquet() -> bytes:
    """
    Int32 column (widened to int64 on decode) with 4 distinct values, each
    repeated 125 times. Runs: 4 → 4*4=16 < 500 → internal RLE decode fires.
    Non-nullable to avoid definition-level pages.
    """
    values = []
    for v in [100, 200, 300, 400]:
        values.extend([v] * 125)
    schema = pa.schema([pa.field("code", pa.int32(), nullable=False)])
    tbl = pa.table({"code": pa.array(values, type=pa.int32())}, schema=schema)
    return _write_parquet(tbl)


# ---------------------------------------------------------------------------
# Int64 highly-repetitive column tests
# ---------------------------------------------------------------------------

def test_int64_rle_materializes_to_correct_values():
    """Highly-repetitive int64 dict column must expand to the original values."""
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]

    result = vec.to_pylist()
    expected = _ref_values(raw, "score")

    assert result == expected, (
        f"Mismatch: first 10 got={result[:10]}, expected={expected[:10]}"
    )


def test_int64_rle_len_is_correct():
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    assert len(cols["score"]) == 500


def test_int64_rle_sum_is_correct():
    """sum() on a highly-repetitive parquet column must equal sum of original values."""
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]

    # 100 each of [10, 20, 30, 40, 50] → sum = 100 * 150 = 15000
    assert vec.sum() == 15000


def test_int64_rle_min_is_correct():
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    assert cols["score"].min() == 10


def test_int64_rle_max_is_correct():
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    assert cols["score"].max() == 50


def test_int64_rle_take_produces_dense_correct():
    """take() on a highly-repetitive parquet column returns correct values."""
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]

    # Take one index from each run
    indices = array("i", [0, 100, 200, 300, 400])
    taken = vec.take(indices)

    assert taken.to_pylist() == [10, 20, 30, 40, 50]


def test_int64_rle_equals_produces_correct_mask():
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]

    result = _equals_mask(vec, 30)

    # Rows 200-299 should be True, rest False
    assert all(result[200:300])
    assert not any(result[:200])
    assert not any(result[300:])


# ---------------------------------------------------------------------------
# High-cardinality tests
# ---------------------------------------------------------------------------

def test_high_cardinality_int64_values_are_correct():
    raw = _build_high_cardinality_int64_parquet()
    cols = _read_columns(raw, ["id"])
    result = cols["id"].to_pylist()
    expected = _ref_values(raw, "id")
    assert result == expected


# ---------------------------------------------------------------------------
# Nullable column tests
# ---------------------------------------------------------------------------

def test_nullable_int64_values_are_correct():
    raw = _build_nullable_int64_parquet()
    cols = _read_columns(raw, ["val"])
    result = cols["val"].to_pylist()
    expected = _ref_values(raw, "val")
    assert result == expected


# ---------------------------------------------------------------------------
# Int32-as-Int64 highly-repetitive column tests
# ---------------------------------------------------------------------------

def test_int32_as_int64_rle_values_are_correct():
    raw = _build_rle_eligible_int32_parquet()
    cols = _read_columns(raw, ["code"])
    vec = cols["code"]

    result = vec.to_pylist()
    expected = _ref_values(raw, "code")
    # Reference returns int32; compare as int
    assert result == [int(v) for v in expected]


def test_int32_as_int64_rle_sum_is_correct():
    raw = _build_rle_eligible_int32_parquet()
    cols = _read_columns(raw, ["code"])
    # 125 each of [100, 200, 300, 400] → 125 * 1000 = 125000
    assert cols["code"].sum() == 125000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
