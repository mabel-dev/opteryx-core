"""
Tests for the RLE fast path in the parquet reader.

The RLE path in _make_typed_int64_dictionary_vector (and float64 variants)
fires when a dict-encoded column has >4:1 run compression AND no nulls.
We create a synthetic parquet file with highly repetitive dict-encoded values
and verify:

  1. Int64 columns with consecutive repetitions trigger the RLE path
  2. The resulting vector has DRAKEN_ENCODING_RLE (encoding == 2)
  3. The RLE vector materialises to correct values (matches reference)
  4. The RLE vector aggregates (sum/min/max) are correct
  5. Columns that don't qualify (high cardinality, nulls) stay DICTIONARY
"""

import sys
import tempfile
from pathlib import Path
from array import array

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx.compiled.rugo.parquet as rp

# Encoding constants
DRAKEN_ENCODING_DENSE = 0
DRAKEN_ENCODING_DICTIONARY = 1
DRAKEN_ENCODING_RLE = 2


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
    meta = rp.read_metadata_from_bytes(raw)
    all_cols = [c["name"] for c in meta["schema_columns"]]
    morsels = rp.read_parquet(raw, all_cols)
    out = {}
    for morsel in morsels:
        for name in col_names:
            if name not in out:
                out[name] = morsel.column(name.encode())
            # First row group wins; multiple groups are concatenated by caller if needed
    return out


def _ref_values(raw: bytes, col_name: str) -> list:
    """Read column via PyArrow as reference (ground truth)."""
    tbl = pq.read_table(pa.BufferReader(raw), columns=[col_name])
    return tbl[col_name].to_pylist()


# ---------------------------------------------------------------------------
# Synthetic parquet builders
# ---------------------------------------------------------------------------

def _build_rle_eligible_int64_parquet() -> bytes:
    """
    Int64 column with 5 distinct values, each repeated 100 times.
    Dictionary: [10, 20, 30, 40, 50]
    Rows: [10]*100 + [20]*100 + [30]*100 + [40]*100 + [50]*100 = 500 rows
    Runs: 5 → 5*4=20 < 500 → RLE path fires.

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
    Int64 column where every value is unique → no runs → DICTIONARY path.
    Non-nullable to match the same definition-level conditions as the RLE test.
    """
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    tbl = pa.table({"id": pa.array(list(range(500)), type=pa.int64())}, schema=schema)
    return _write_parquet(tbl)


def _build_nullable_int64_parquet() -> bytes:
    """
    Int64 column with nulls and high repetition.
    Nullable — definition levels are written, so valid_bits.size() > 0,
    which disqualifies the RLE fast path → stays DICTIONARY.
    """
    values = [10 if i % 2 == 0 else None for i in range(500)]
    tbl = pa.table({"val": pa.array(values, type=pa.int64())})
    return _write_parquet(tbl)


def _build_rle_eligible_int32_parquet() -> bytes:
    """
    Int32 column (decoded as int64 via _make_typed_int64_from_int32_dictionary_vector)
    with 4 distinct values, each repeated 125 times.
    Runs: 4 → 4*4=16 < 500 → RLE path fires.
    Non-nullable to avoid definition-level pages.
    """
    values = []
    for v in [100, 200, 300, 400]:
        values.extend([v] * 125)
    schema = pa.schema([pa.field("code", pa.int32(), nullable=False)])
    tbl = pa.table({"code": pa.array(values, type=pa.int32())}, schema=schema)
    return _write_parquet(tbl)


# ---------------------------------------------------------------------------
# Int64 RLE path tests
# ---------------------------------------------------------------------------

def test_int64_rle_eligible_column_uses_rle_encoding():
    """Repetitive int64 dict column must produce RLE-encoded vector."""
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]
    assert vec.encoding == DRAKEN_ENCODING_RLE, (
        f"expected RLE ({DRAKEN_ENCODING_RLE}), got {vec.encoding} "
        f"(type={type(vec).__name__})"
    )


def test_int64_rle_materializes_to_correct_values():
    """RLE int64 vector must expand to the original values."""
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
    """sum() on RLE-encoded parquet column must equal sum of original values."""
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
    """take() on RLE parquet vector returns dense with correct values."""
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]
    assert vec.encoding == DRAKEN_ENCODING_RLE

    # Take one index from each run
    indices = array("i", [0, 100, 200, 300, 400])
    taken = vec.take(indices)

    assert taken.encoding == DRAKEN_ENCODING_DENSE
    assert taken.to_pylist() == [10, 20, 30, 40, 50]


def test_int64_rle_equals_produces_correct_mask():
    raw = _build_rle_eligible_int64_parquet()
    cols = _read_columns(raw, ["score"])
    vec = cols["score"]

    mask = vec.equals(30)
    result = mask.to_pylist()

    # Rows 200-299 should be True, rest False
    assert all(result[200:300])
    assert not any(result[:200])
    assert not any(result[300:])


# ---------------------------------------------------------------------------
# High-cardinality (no RLE) tests
# ---------------------------------------------------------------------------

def test_high_cardinality_int64_does_not_use_rle():
    """High-cardinality dict column must NOT use RLE encoding."""
    raw = _build_high_cardinality_int64_parquet()
    cols = _read_columns(raw, ["id"])
    vec = cols["id"]
    # Should be DICTIONARY or DENSE, not RLE
    assert vec.encoding != DRAKEN_ENCODING_RLE, (
        f"unexpected RLE encoding for high-cardinality column"
    )


def test_high_cardinality_int64_values_are_correct():
    raw = _build_high_cardinality_int64_parquet()
    cols = _read_columns(raw, ["id"])
    result = cols["id"].to_pylist()
    expected = _ref_values(raw, "id")
    assert result == expected


# ---------------------------------------------------------------------------
# Nullable column (RLE path excluded)
# ---------------------------------------------------------------------------

def test_nullable_int64_does_not_use_rle():
    """Columns with nulls must NOT use RLE fast path."""
    raw = _build_nullable_int64_parquet()
    cols = _read_columns(raw, ["val"])
    vec = cols["val"]
    assert vec.encoding != DRAKEN_ENCODING_RLE


def test_nullable_int64_values_are_correct():
    raw = _build_nullable_int64_parquet()
    cols = _read_columns(raw, ["val"])
    result = cols["val"].to_pylist()
    expected = _ref_values(raw, "val")
    assert result == expected


# ---------------------------------------------------------------------------
# Int32-as-Int64 RLE path tests
# ---------------------------------------------------------------------------

def test_int32_as_int64_rle_eligible_column_uses_rle_encoding():
    """Repetitive int32 dict column (decoded as int64) must produce RLE vector."""
    raw = _build_rle_eligible_int32_parquet()
    cols = _read_columns(raw, ["code"])
    vec = cols["code"]
    assert vec.encoding == DRAKEN_ENCODING_RLE, (
        f"expected RLE ({DRAKEN_ENCODING_RLE}), got {vec.encoding}"
    )


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
    tests = [
        test_int64_rle_eligible_column_uses_rle_encoding,
        test_int64_rle_materializes_to_correct_values,
        test_int64_rle_len_is_correct,
        test_int64_rle_sum_is_correct,
        test_int64_rle_min_is_correct,
        test_int64_rle_max_is_correct,
        test_int64_rle_take_produces_dense_correct,
        test_int64_rle_equals_produces_correct_mask,
        test_high_cardinality_int64_does_not_use_rle,
        test_high_cardinality_int64_values_are_correct,
        test_nullable_int64_does_not_use_rle,
        test_nullable_int64_values_are_correct,
        test_int32_as_int64_rle_eligible_column_uses_rle_encoding,
        test_int32_as_int64_rle_values_are_correct,
        test_int32_as_int64_rle_sum_is_correct,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            import traceback
            print(f"  ❌ {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
