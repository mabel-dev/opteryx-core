"""
Native unit + property tests for DRAKEN_VECTOR_FP16 (D.11).

Design contract (06_value_encoding.md):
  - Physical: uint16_t[data_length * dimension]; dimension via mandatory logical descriptor.
  - from_float_pylist round-trips within half-precision tolerance.
  - dimension is mandatory (missing → hard error); row-length ≠ dimension fails loud.
  - None rows → null; readback gives per-row float lists.
  - Unsupported ops (ordering/arithmetic/similarity) throw.
  - hash: canonical fp16 bits; consistent across identical inputs.
  - take/materialize/compress preserve fp16 encoding.

All tests import draken.draken_native directly; no import opteryx.
"""

import math
import struct

import pytest
from hypothesis import given, settings, HealthCheck, assume
import hypothesis.strategies as st

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FP16_MAX = 65504.0
FP16_MIN_POS = 5.96e-8  # approx 2^-24

def fp16(seq, dim=None):
    """Build a VECTOR_FP16 vector. dim inferred from first non-None row if omitted."""
    if dim is None:
        for row in seq:
            if row is not None:
                dim = len(row)
                break
        if dim is None:
            raise ValueError("cannot infer dimension from all-None sequence")
    return dn.vector_fp16_from_sequence(seq, dim)


def fp16_tolerance(f):
    """Half-precision round-trip tolerance: ~0.1% of |f|, minimum ~1e-7."""
    return max(abs(f) * 0.001, 1e-7)


def row_approx_equal(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if len(a) != len(b):
        return False
    return all(abs(ai - bi) <= fp16_tolerance(ai) for ai, bi in zip(a, b))


# ===========================================================================
# 1. Type tag and descriptor
# ===========================================================================

class TestFp16TypeTag:
    def test_type_is_vector_fp16(self):
        v = fp16([[1.0, 2.0, 3.0]])
        assert v.type == dn.DrakenType.VECTOR_FP16

    def test_logical_type_dimension(self):
        v = fp16([[1.0, 2.0, 3.0, 4.0]])
        assert v.logical_type_dimension == 4

    def test_dimension_1(self):
        v = fp16([[0.5]])
        assert v.logical_type_dimension == 1

    def test_length(self):
        v = fp16([[1.0, 2.0]] * 5)
        assert len(v) == 5

    def test_empty_sequence(self):
        v = dn.vector_fp16_from_sequence([], 3)
        assert len(v) == 0
        assert v.logical_type_dimension == 3


# ===========================================================================
# 2. Construction errors
# ===========================================================================

class TestFp16ConstructionErrors:
    def test_dimension_zero_raises(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_fp16_from_sequence([[1.0]], 0)

    def test_row_length_mismatch_raises(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_fp16_from_sequence([[1.0, 2.0], [1.0]], 2)

    def test_row_too_long_raises(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_fp16_from_sequence([[1.0, 2.0, 3.0]], 2)

    def test_row_too_short_raises(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_fp16_from_sequence([[1.0]], 2)


# ===========================================================================
# 3. Round-trip (within half-precision tolerance)
# ===========================================================================

class TestFp16RoundTrip:
    def test_simple_roundtrip(self):
        rows = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
        v = fp16(rows)
        out = v.to_pylist()
        assert len(out) == 2
        for orig, got in zip(rows, out):
            assert row_approx_equal(orig, got)

    def test_zero_vector(self):
        v = fp16([[0.0, 0.0]])
        assert row_approx_equal(v.to_pylist()[0], [0.0, 0.0])

    def test_negative_values(self):
        rows = [[-1.0, -2.5, -0.5]]
        v = fp16(rows)
        out = v.to_pylist()[0]
        assert row_approx_equal(out, rows[0])

    def test_null_row_readback(self):
        v = fp16([[1.0, 2.0], None, [3.0, 4.0]])
        out = v.to_pylist()
        assert out[0] is not None
        assert out[1] is None
        assert out[2] is not None

    def test_all_null(self):
        v = dn.vector_fp16_from_sequence([None, None, None], 4)
        assert v.to_pylist() == [None, None, None]

    def test_getitem_non_null(self):
        v = fp16([[1.0, 2.0]])
        row = v[0]
        assert len(row) == 2
        assert row_approx_equal(row, [1.0, 2.0])

    def test_getitem_null_row(self):
        v = fp16([None, [1.0, 2.0]])
        assert v[0] is None
        assert v[1] is not None

    def test_large_dimension(self):
        import random
        random.seed(42)
        dim = 128
        row = [random.uniform(-10.0, 10.0) for _ in range(dim)]
        v = fp16([row])
        out = v.to_pylist()[0]
        assert len(out) == dim
        assert row_approx_equal(out, row)

    @given(
        st.lists(
            st.floats(
                min_value=-FP16_MAX, max_value=FP16_MAX,
                allow_nan=False, allow_infinity=False,
            ),
            min_size=2, max_size=2,
        )
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_roundtrip_property_dim2(self, row):
        v = fp16([row])
        out = v.to_pylist()[0]
        assert row_approx_equal(out, row)


# ===========================================================================
# 4. Hash — canonical fp16 bits
# ===========================================================================

class TestFp16Hash:
    def test_hash_length(self):
        v = fp16([[1.0, 2.0]] * 5)
        assert len(v.hash()) == 5

    def test_same_row_same_hash(self):
        v1 = fp16([[1.0, 2.0, 3.0]])
        v2 = fp16([[1.0, 2.0, 3.0]])
        assert v1.hash() == v2.hash()

    def test_different_rows_different_hash(self):
        v1 = fp16([[1.0, 2.0]])
        v2 = fp16([[3.0, 4.0]])
        assert v1.hash()[0] != v2.hash()[0]

    def test_null_row_null_hash_sentinel(self):
        """Null fp16 row must hash identically to null rows of other types."""
        null_type_hash = dn.vector_null_from_length(1).hash()[0]
        fp16_null_hash = dn.vector_fp16_from_sequence([None], 2).hash()[0]
        assert fp16_null_hash == null_type_hash

    def test_hash_empty(self):
        v = dn.vector_fp16_from_sequence([], 3)
        assert v.hash() == []


# ===========================================================================
# 5. Unsupported ops must throw
# ===========================================================================

class TestFp16UnsupportedOps:
    EQ = 0

    def test_compare_scalar_with_value_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.compare_scalar(0, self.EQ)

    def test_compare_scalar_none_raises(self):
        v = fp16([[1.0, 2.0], [3.0, 4.0]])
        with pytest.raises(TypeError):
            v.compare_scalar(None, self.EQ)

    def test_compare_vector_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.compare_vector(v, self.EQ)

    def test_between_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.between(0, 10)

    def test_in_list_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.in_list([[1.0, 2.0]])

    def test_sum_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.sum()

    def test_min_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.min()

    def test_max_raises(self):
        v = fp16([[1.0, 2.0]])
        with pytest.raises((ValueError, Exception)):
            v.max()


# ===========================================================================
# 6. take / materialize / compress
# ===========================================================================

class TestFp16GatherOps:
    def test_take_basic(self):
        v = fp16([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        result = v.take([2, 0])
        assert result.type == dn.DrakenType.VECTOR_FP16
        assert len(result) == 2
        out = result.to_pylist()
        assert row_approx_equal(out[0], [5.0, 6.0])
        assert row_approx_equal(out[1], [1.0, 2.0])

    def test_take_null_rows(self):
        v = fp16([None, [1.0, 2.0], None])
        result = v.take([1, 0, 2])
        out = result.to_pylist()
        assert out[0] is not None
        assert out[1] is None
        assert out[2] is None

    def test_take_empty(self):
        v = fp16([[1.0, 2.0], [3.0, 4.0]])
        result = v.take([])
        assert len(result) == 0

    def test_take_preserves_dimension(self):
        v = fp16([[1.0, 2.0, 3.0]])
        result = v.take([0])
        assert result.logical_type_dimension == 3

    def test_materialize_round_trips(self):
        rows = [[1.0, 2.0], [3.0, 4.0]]
        v = fp16(rows)
        result = v.materialize()
        assert result.type == dn.DrakenType.VECTOR_FP16
        out = result.to_pylist()
        for orig, got in zip(rows, out):
            assert row_approx_equal(orig, got)

    def test_materialize_preserves_nulls(self):
        v = fp16([[1.0, 2.0], None, [3.0, 4.0]])
        result = v.materialize()
        out = result.to_pylist()
        assert out[1] is None

    def test_compress_removes_null_rows(self):
        v = fp16([[1.0, 2.0], None, [3.0, 4.0]])
        result = v.compress()
        assert result.type == dn.DrakenType.VECTOR_FP16
        assert len(result) == 2
        out = result.to_pylist()
        assert all(row is not None for row in out)

    def test_compress_all_null(self):
        v = dn.vector_fp16_from_sequence([None, None], 3)
        result = v.compress()
        assert len(result) == 0

    def test_compress_no_nulls(self):
        rows = [[1.0, 2.0], [3.0, 4.0]]
        v = fp16(rows)
        result = v.compress()
        assert len(result) == 2


# ===========================================================================
# 7. Microbench (smoke — not a timing gate)
# ===========================================================================

class TestFp16Bench:
    def test_bench_ingest_readback_1k(self):
        dim = 64
        n = 1024
        rows = [[float(j) for j in range(dim)] for _ in range(n)]
        v = fp16(rows)
        out = v.to_pylist()
        assert len(out) == n
        assert all(row is not None for row in out)

    def test_bench_hash_1k(self):
        dim = 32
        n = 1024
        rows = [[float(j % 100) for j in range(dim)] for _ in range(n)]
        v = fp16(rows)
        h = v.hash()
        assert len(h) == n
