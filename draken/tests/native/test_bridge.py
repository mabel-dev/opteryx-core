"""
draken/tests/native/test_bridge.py — E.1 bridge surface tests.

Validates:
  1. draken_vector_unwrap: extracts DrakenVector*, calls i64_sum nogil, result correct.
  2. draken_vector_own: wraps VecResult from i64_neg, result matches direct negation.
  3. draken_vector_own_raw: hand-allocate + wrap via _bridge_test_own_raw, read back correct.
  4. Type-check: draken_vector_unwrap raises TypeError on non-Vector input.

Entry points in draken_native (prefixed _bridge_test_*) exercise each bridge function
internally so the round-trip is validated without a separate Cython extension build.
"""

import pytest
from draken import draken_native


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_int64_vec(values):
    return draken_native.vector_from_sequence(values)


# ---------------------------------------------------------------------------
# 1. unwrap + nogil scalar reduction
# ---------------------------------------------------------------------------

class TestBridgeUnwrapSum:
    def test_dense_no_nulls(self):
        v = make_int64_vec([1, 2, 3, 4, 5])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 15

    def test_with_nulls(self):
        v = make_int64_vec([10, None, 30, None, 50])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 90  # 10 + 30 + 50

    def test_all_null(self):
        v = make_int64_vec([None, None, None])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 0

    def test_empty(self):
        v = make_int64_vec([])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 0

    def test_single_element(self):
        v = make_int64_vec([42])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 42

    def test_negative_values(self):
        v = make_int64_vec([-1, -2, -3])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == -6

    def test_constant_encoded(self):
        v = draken_native.vector_from_constant(7, 4)
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 28  # 7 * 4

    def test_dict_encoded(self):
        v = draken_native.vector_from_dict([10, 20], [0, 1, 0, 1])
        result = draken_native._bridge_test_unwrap_sum(v)
        assert result == 60  # 10+20+10+20


# ---------------------------------------------------------------------------
# 2. round-trip: unwrap → nogil op (i64_neg) → draken_vector_own → Vector
# ---------------------------------------------------------------------------

class TestBridgeNegViaOwn:
    def test_dense_no_nulls(self):
        v = make_int64_vec([1, 2, 3])
        result = draken_native._bridge_test_neg_via_own(v)
        assert result.to_pylist() == [-1, -2, -3]

    def test_with_nulls_propagate(self):
        v = make_int64_vec([1, None, 3])
        result = draken_native._bridge_test_neg_via_own(v)
        assert result.to_pylist() == [-1, None, -3]

    def test_all_null(self):
        v = make_int64_vec([None, None])
        result = draken_native._bridge_test_neg_via_own(v)
        assert result.to_pylist() == [None, None]

    def test_single(self):
        v = make_int64_vec([99])
        result = draken_native._bridge_test_neg_via_own(v)
        assert result.to_pylist() == [-99]

    def test_result_is_vector(self):
        v = make_int64_vec([1, 2])
        result = draken_native._bridge_test_neg_via_own(v)
        assert isinstance(result, draken_native.Vector)
        assert result.type == draken_native.DrakenType.INT64

    def test_matches_direct_negation(self):
        vals = [5, -3, 0, 100, -200]
        v = make_int64_vec(vals)
        bridge_result = draken_native._bridge_test_neg_via_own(v)
        expected = [-x for x in vals]
        assert bridge_result.to_pylist() == expected

    def test_dict_encoded_input(self):
        v = draken_native.vector_from_dict([1, 2, 3], [0, 1, 2, 0])
        result = draken_native._bridge_test_neg_via_own(v)
        assert result.to_pylist() == [-1, -2, -3, -1]


# ---------------------------------------------------------------------------
# 3. draken_vector_own_raw: hand-allocate + wrap, read back correct
# ---------------------------------------------------------------------------

class TestBridgeOwnRaw:
    def test_basic_values(self):
        result = draken_native._bridge_test_own_raw([10, 20, 30])
        assert result.to_pylist() == [10, 20, 30]

    def test_with_nulls(self):
        result = draken_native._bridge_test_own_raw([1, None, 3])
        assert result.to_pylist() == [1, None, 3]

    def test_all_null(self):
        result = draken_native._bridge_test_own_raw([None, None])
        assert result.to_pylist() == [None, None]

    def test_empty(self):
        result = draken_native._bridge_test_own_raw([])
        assert result.to_pylist() == []

    def test_result_is_vector(self):
        result = draken_native._bridge_test_own_raw([1, 2])
        assert isinstance(result, draken_native.Vector)
        assert result.type == draken_native.DrakenType.INT64

    def test_negative_values(self):
        result = draken_native._bridge_test_own_raw([-5, -10, 0])
        assert result.to_pylist() == [-5, -10, 0]

    def test_length(self):
        result = draken_native._bridge_test_own_raw([1, 2, 3, 4, 5])
        assert len(result) == 5


# ---------------------------------------------------------------------------
# 4. Type-check: non-Vector input raises TypeError (never segfaults)
# ---------------------------------------------------------------------------

class TestBridgeTypeCheck:
    def test_integer_raises_type_error(self):
        with pytest.raises(TypeError):
            draken_native._bridge_test_unwrap_sum(42)

    def test_string_raises_type_error(self):
        with pytest.raises(TypeError):
            draken_native._bridge_test_unwrap_sum("not a vector")

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError):
            draken_native._bridge_test_unwrap_sum([1, 2, 3])

    def test_none_raises_type_error(self):
        with pytest.raises(TypeError):
            draken_native._bridge_test_unwrap_sum(None)

    def test_type_error_helper_raises(self):
        with pytest.raises(TypeError):
            draken_native._bridge_test_type_error()
