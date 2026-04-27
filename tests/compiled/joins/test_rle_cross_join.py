"""
Tests for RLE-encoded cross join indices.

Verifies that build_cartesian_indices() emits a RLE left index and dense right
index, and that Morsel._take_inplace correctly consumes the RLE index to
produce correct join output.
"""

import sys
from pathlib import Path
from array import array

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from opteryx.compiled.joins import build_cartesian_indices

# Encoding constants — stable contract (test_vector_encoding.py also asserts these)
DRAKEN_ENCODING_DENSE = 0
DRAKEN_ENCODING_DICTIONARY = 1
DRAKEN_ENCODING_RLE = 2
DRAKEN_ENCODING_CONSTANT = 3


# ---------------------------------------------------------------------------
# Index vector encoding
# ---------------------------------------------------------------------------

def test_left_index_is_rle_encoded():
    """build_cartesian_indices must emit an RLE-encoded left index."""
    left, _ = build_cartesian_indices(5, 3)
    assert left.encoding == DRAKEN_ENCODING_RLE, (
        f"expected RLE ({DRAKEN_ENCODING_RLE}), got {left.encoding}"
    )


def test_right_index_is_dense():
    """build_cartesian_indices must emit a dense right index."""
    _, right = build_cartesian_indices(5, 3)
    assert right.encoding == DRAKEN_ENCODING_DENSE, (
        f"expected DENSE ({DRAKEN_ENCODING_DENSE}), got {right.encoding}"
    )


def test_left_index_length_is_product():
    left, _ = build_cartesian_indices(4, 6)
    assert len(left) == 24


def test_right_index_length_is_product():
    _, right = build_cartesian_indices(4, 6)
    assert len(right) == 24


# ---------------------------------------------------------------------------
# Index materialization correctness
# ---------------------------------------------------------------------------

def test_left_index_materializes_correctly_small():
    """RLE left index must expand to repeated row indices."""
    left, _ = build_cartesian_indices(3, 4)
    result = left.to_pylist()
    expected = [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]
    assert result == expected, f"left index: {result}"


def test_right_index_materializes_correctly_small():
    """Dense right index must repeat [0..R-1] L times."""
    _, right = build_cartesian_indices(3, 4)
    result = right.to_pylist()
    expected = [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3]
    assert result == expected, f"right index: {result}"


def test_left_index_materializes_correctly_single_right_row():
    left, _ = build_cartesian_indices(5, 1)
    assert left.to_pylist() == [0, 1, 2, 3, 4]


def test_right_index_materializes_correctly_single_right_row():
    _, right = build_cartesian_indices(5, 1)
    assert right.to_pylist() == [0, 0, 0, 0, 0]


def test_left_index_materializes_correctly_single_left_row():
    left, _ = build_cartesian_indices(1, 5)
    assert left.to_pylist() == [0, 0, 0, 0, 0]


def test_right_index_materializes_correctly_single_left_row():
    _, right = build_cartesian_indices(1, 5)
    assert right.to_pylist() == [0, 1, 2, 3, 4]


def test_empty_cross_join_returns_empty_vectors():
    left, right = build_cartesian_indices(0, 5)
    assert len(left) == 0
    assert len(right) == 0


def test_zero_right_rows_returns_empty_vectors():
    left, right = build_cartesian_indices(5, 0)
    assert len(left) == 0
    assert len(right) == 0


def test_left_index_large_is_correct():
    """Spot-check correctness for a larger join."""
    L, R = 100, 50
    left, _ = build_cartesian_indices(L, R)
    values = left.to_pylist()
    assert len(values) == L * R
    # Each left row i must appear exactly R times, consecutively
    for i in range(L):
        chunk = values[i * R : (i + 1) * R]
        assert all(v == i for v in chunk), (
            f"row {i} chunk incorrect: {chunk[:5]}..."
        )


def test_right_index_large_is_correct():
    """Spot-check correctness for a larger join."""
    L, R = 100, 50
    _, right = build_cartesian_indices(L, R)
    values = right.to_pylist()
    assert len(values) == L * R
    pattern = list(range(R))
    for i in range(L):
        chunk = values[i * R : (i + 1) * R]
        assert chunk == pattern, f"iteration {i} mismatch"


# ---------------------------------------------------------------------------
# take() on RLE index materializes correctly
# ---------------------------------------------------------------------------

def test_rle_vector_take_produces_dense_output():
    """take() on an RLE vector must return a dense vector with correct values."""
    left, _ = build_cartesian_indices(4, 3)
    assert left.encoding == DRAKEN_ENCODING_RLE

    # Take indices [0, 3, 6, 9] → should get rows 0, 1, 2, 3 (one each)
    indices = array("i", [0, 3, 6, 9])
    taken = left.take(indices)

    assert taken.encoding == DRAKEN_ENCODING_DENSE
    assert taken.to_pylist() == [0, 1, 2, 3]


def test_rle_vector_take_repeated_indices():
    """take() with repeated indices on RLE vector must expand correctly."""
    left, _ = build_cartesian_indices(3, 2)
    # left = [0, 0, 1, 1, 2, 2]
    indices = array("i", [0, 0, 4, 4])
    taken = left.take(indices)

    assert taken.encoding == DRAKEN_ENCODING_DENSE
    assert taken.to_pylist() == [0, 0, 2, 2]


# ---------------------------------------------------------------------------
# SQL-level integration
# ---------------------------------------------------------------------------

def test_sql_cross_join_count_is_correct():
    """End-to-end: COUNT(*) of a cross join must equal L × R."""
    import opteryx
    import pyarrow as pa

    session = opteryx.session()
    morsels = list(session.execute_to_morsels(
        "SELECT COUNT(*) AS cnt FROM $planets AS a CROSS JOIN $planets AS b"
    ))
    result = pa.concat_tables([m.to_arrow() for m in morsels])
    assert result["cnt"][0].as_py() == 81  # 9 × 9


def test_sql_cross_join_larger_count_is_correct():
    """End-to-end: larger cross join count must equal L × R."""
    import opteryx
    import pyarrow as pa

    session = opteryx.session()
    morsels = list(session.execute_to_morsels(
        "SELECT COUNT(*) AS cnt FROM testdata.missions CROSS JOIN $planets"
    ))
    result = pa.concat_tables([m.to_arrow() for m in morsels])
    assert result["cnt"][0].as_py() == 4630 * 9


if __name__ == "__main__":
    # Quick self-test without pytest
    tests = [
        test_left_index_is_rle_encoded,
        test_right_index_is_dense,
        test_left_index_length_is_product,
        test_right_index_length_is_product,
        test_left_index_materializes_correctly_small,
        test_right_index_materializes_correctly_small,
        test_left_index_materializes_correctly_single_right_row,
        test_right_index_materializes_correctly_single_right_row,
        test_left_index_materializes_correctly_single_left_row,
        test_right_index_materializes_correctly_single_left_row,
        test_empty_cross_join_returns_empty_vectors,
        test_zero_right_rows_returns_empty_vectors,
        test_left_index_large_is_correct,
        test_right_index_large_is_correct,
        test_rle_vector_take_produces_dense_output,
        test_rle_vector_take_repeated_indices,
        test_sql_cross_join_count_is_correct,
        test_sql_cross_join_larger_count_is_correct,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ❌ {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
