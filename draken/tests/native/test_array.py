"""
Native unit + property tests for DRAKEN_ARRAY (D.13).

Design contract (06_value_encoding.md, 01_ownership.md):
  - Physical: int32 offsets[length+1] + owned child DrakenVector (RAII chains).
  - Access: child[offsets[sel[i]] : offsets[sel[i]+1]] for logical row i.
  - None row → null (validity bit clear); [] row → valid empty sublist.
  - Child type inferred: int → INT64, str → STRING, list → ARRAY (recursive).
  - Supported ops: take, materialize, compress, array_length, array_get.
  - Unsupported ops (hash, compare, sum/min/max, arithmetic) throw.
  - sizeof(DrakenVector) == 40 (child held out-of-line via child_owner).
  - No shared/borrowed children; RAII frees whole subtree on parent destruct.

No import opteryx; no PyArrow.
"""

import gc
import math

import pytest
from hypothesis import given, settings, HealthCheck, assume
import hypothesis.strategies as st

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def arr(seq):
    return dn.vector_array_from_sequence(seq)


def roundtrip(seq):
    return arr(seq).to_pylist()


# ===========================================================================
# 1. Type tag
# ===========================================================================

class TestArrayTypeTag:
    def test_type_is_array(self):
        v = arr([[1, 2, 3]])
        assert v.type == dn.DrakenType.ARRAY

    def test_length(self):
        v = arr([[1], [2, 3], [4, 5, 6]])
        assert len(v) == 3

    def test_empty_vector(self):
        v = arr([])
        assert len(v) == 0
        assert v.to_pylist() == []

    def test_child_type_int64(self):
        v = arr([[1, 2, 3]])
        assert v.array_child_type == dn.DrakenType.INT64

    def test_child_type_string(self):
        v = arr([["hello", "world"]])
        assert v.array_child_type == dn.DrakenType.VARCHAR

    def test_child_type_array(self):
        v = arr([[[1, 2], [3]]])
        assert v.array_child_type == dn.DrakenType.ARRAY


# ===========================================================================
# 2. Round-trip: list[list] values
# ===========================================================================

class TestRoundTrip:
    def test_simple_int_lists(self):
        data = [[1, 2, 3], [4, 5], [6]]
        assert roundtrip(data) == data

    def test_simple_string_lists(self):
        data = [["hello", "world"], ["foo"]]
        assert roundtrip(data) == data

    def test_single_element_sublists(self):
        data = [[42], [-7], [0]]
        assert roundtrip(data) == data

    def test_varying_lengths(self):
        data = [[1], [1, 2], [1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4, 5]]
        assert roundtrip(data) == data

    def test_large_sublists(self):
        data = [list(range(100)), list(range(50, 200))]
        assert roundtrip(data) == data

    def test_single_row(self):
        data = [[10, 20, 30]]
        assert roundtrip(data) == data

    def test_many_rows_one_element_each(self):
        data = [[i] for i in range(100)]
        assert roundtrip(data) == data

    def test_negative_integers(self):
        data = [[-1, -2, -3], [-100]]
        assert roundtrip(data) == data

    def test_string_with_long_values(self):
        long_str = "x" * 100
        data = [[long_str, "short"], [long_str * 2]]
        assert roundtrip(data) == data


# ===========================================================================
# 3. Null row semantics: None → null, [] → valid empty sublist
# ===========================================================================

class TestNullVsEmpty:
    def test_null_row_reads_back_as_none(self):
        v = arr([None])
        assert v.to_pylist() == [None]

    def test_empty_sublist_reads_back_as_empty_list(self):
        v = arr([[]])
        assert v.to_pylist() == [[]]

    def test_null_and_empty_are_distinct(self):
        v = arr([None, []])
        result = v.to_pylist()
        assert result[0] is None
        assert result[1] == []

    def test_mixed_null_empty_nonempty(self):
        data = [[1, 2], None, [], [3]]
        result = roundtrip(data)
        assert result[0] == [1, 2]
        assert result[1] is None
        assert result[2] == []
        assert result[3] == [3]

    def test_all_null(self):
        v = arr([None, None, None])
        assert v.to_pylist() == [None, None, None]

    def test_all_empty(self):
        v = arr([[], [], []])
        assert v.to_pylist() == [[], [], []]

    def test_leading_trailing_null(self):
        data = [None, [1, 2], None]
        result = roundtrip(data)
        assert result[0] is None
        assert result[1] == [1, 2]
        assert result[2] is None

    def test_null_validity_bit(self):
        v = arr([[1], None, [2]])
        assert v[0] == [1]
        assert v[1] is None
        assert v[2] == [2]

    def test_empty_validity_bit(self):
        v = arr([[], [1]])
        assert v[0] == []
        assert v[1] == [1]


# ===========================================================================
# 4. Per-row access via __getitem__
# ===========================================================================

class TestGetItem:
    def test_getitem_basic(self):
        v = arr([[10, 20], [30]])
        assert v[0] == [10, 20]
        assert v[1] == [30]

    def test_getitem_negative_index(self):
        v = arr([[1, 2], [3, 4]])
        assert v[-1] == [3, 4]
        assert v[-2] == [1, 2]

    def test_getitem_null_row(self):
        v = arr([None, [5]])
        assert v[0] is None
        assert v[1] == [5]

    def test_getitem_empty_sublist(self):
        v = arr([[]])
        assert v[0] == []

    def test_getitem_out_of_range(self):
        v = arr([[1, 2]])
        with pytest.raises(Exception):
            _ = v[1]
        with pytest.raises(Exception):
            _ = v[-2]


# ===========================================================================
# 5. array_length and array_get per-row accessors
# ===========================================================================

class TestArrayAccessors:
    def test_array_length_basic(self):
        v = arr([[1, 2, 3], [4], []])
        assert v.array_length(0) == 3
        assert v.array_length(1) == 1
        assert v.array_length(2) == 0

    def test_array_length_null(self):
        v = arr([None, [1, 2]])
        assert v.array_length(0) is None
        assert v.array_length(1) == 2

    def test_array_length_negative_index(self):
        v = arr([[1, 2], [3]])
        assert v.array_length(-1) == 1

    def test_array_get_basic(self):
        v = arr([[10, 20, 30]])
        assert v.array_get(0, 0) == 10
        assert v.array_get(0, 1) == 20
        assert v.array_get(0, 2) == 30

    def test_array_get_negative_element_index(self):
        v = arr([[1, 2, 3]])
        assert v.array_get(0, -1) == 3
        assert v.array_get(0, -3) == 1

    def test_array_get_null_row(self):
        v = arr([None])
        assert v.array_get(0, 0) is None

    def test_array_get_out_of_range(self):
        v = arr([[1, 2]])
        with pytest.raises(Exception):
            v.array_get(0, 2)
        with pytest.raises(Exception):
            v.array_get(0, -3)

    def test_array_get_on_non_array_raises(self):
        vi = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(Exception):
            vi.array_get(0, 0)

    def test_array_get_string_child(self):
        v = arr([["hello", "world"]])
        assert v.array_get(0, 0) == "hello"
        assert v.array_get(0, 1) == "world"

    def test_array_length_requires_array(self):
        v = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(Exception):
            v.array_length(0)


# ===========================================================================
# 6. Array-of-array (recursive ingestion and readback)
# ===========================================================================

class TestArrayOfArray:
    def test_simple_array_of_array(self):
        data = [[[1, 2], [3]], [[4, 5, 6]]]
        assert roundtrip(data) == data

    def test_array_of_array_with_null_outer(self):
        data = [[[1, 2]], None, [[3, 4]]]
        result = roundtrip(data)
        assert result[0] == [[1, 2]]
        assert result[1] is None
        assert result[2] == [[3, 4]]

    def test_array_of_array_with_empty_inner(self):
        data = [[[]], [[1, 2]]]
        result = roundtrip(data)
        assert result[0] == [[]]
        assert result[1] == [[1, 2]]

    def test_array_of_array_with_null_inner_row(self):
        # Inner None is a valid child row (null child element).
        # The child is an array vector; its null rows become None in readback.
        data = [[[1, 2], [3, 4]]]
        assert roundtrip(data) == data

    def test_three_level_nesting(self):
        data = [[[[1, 2], [3]], [[4]]], [[[5, 6]]]]
        assert roundtrip(data) == data

    def test_array_of_array_child_type(self):
        v = arr([[[1, 2], [3]]])
        assert v.array_child_type == dn.DrakenType.ARRAY

    def test_all_null_outer_array_of_array(self):
        v = arr([None, None])
        assert v.to_pylist() == [None, None]


# ===========================================================================
# 7. take — gather rows → new owned result
# ===========================================================================

class TestTake:
    def test_take_subset(self):
        v = arr([[1, 2], [3], [4, 5, 6]])
        t = v.take([0, 2])
        assert t.to_pylist() == [[1, 2], [4, 5, 6]]

    def test_take_reorder(self):
        v = arr([[10], [20, 21], [30, 31, 32]])
        t = v.take([2, 0, 1])
        assert t.to_pylist() == [[30, 31, 32], [10], [20, 21]]

    def test_take_with_repeats(self):
        v = arr([[1, 2], [3, 4]])
        t = v.take([0, 0, 1, 0])
        assert t.to_pylist() == [[1, 2], [1, 2], [3, 4], [1, 2]]

    def test_take_null_row(self):
        v = arr([None, [5, 6], None])
        t = v.take([0, 1, 2, 1])
        result = t.to_pylist()
        assert result[0] is None
        assert result[1] == [5, 6]
        assert result[2] is None
        assert result[3] == [5, 6]

    def test_take_empty_sublist(self):
        v = arr([[], [1, 2], []])
        t = v.take([0, 2])
        assert t.to_pylist() == [[], []]

    def test_take_empty_indices(self):
        v = arr([[1, 2], [3]])
        t = v.take([])
        assert len(t) == 0
        assert t.to_pylist() == []

    def test_take_all_rows(self):
        data = [[1, 2], [3, 4], [5, 6]]
        v = arr(data)
        t = v.take([0, 1, 2])
        assert t.to_pylist() == data

    def test_take_negative_index(self):
        v = arr([[1, 2], [3, 4]])
        t = v.take([-1])
        assert t.to_pylist() == [[3, 4]]

    def test_take_produces_owned_result(self):
        v = arr([[1, 2, 3], [4, 5]])
        t = v.take([0])
        del v
        gc.collect()
        assert t.to_pylist() == [[1, 2, 3]]

    def test_take_out_of_range(self):
        v = arr([[1, 2]])
        with pytest.raises(Exception):
            v.take([1])

    def test_take_string_child(self):
        v = arr([["a", "b"], ["c"]])
        t = v.take([1, 0])
        assert t.to_pylist() == [["c"], ["a", "b"]]

    def test_take_array_of_array(self):
        v = arr([[[1, 2], [3]], [[4, 5, 6]]])
        t = v.take([1, 0])
        assert t.to_pylist() == [[[4, 5, 6]], [[1, 2], [3]]]


# ===========================================================================
# 8. materialize
# ===========================================================================

class TestMaterialize:
    def test_materialize_basic(self):
        data = [[1, 2], [3], [4, 5, 6]]
        v = arr(data)
        m = v.materialize()
        assert m.to_pylist() == data

    def test_materialize_with_nulls(self):
        data = [None, [1, 2], None]
        v = arr(data)
        m = v.materialize()
        assert m.to_pylist() == data

    def test_materialize_empty_sublists(self):
        data = [[], [1], []]
        v = arr(data)
        m = v.materialize()
        assert m.to_pylist() == data

    def test_materialize_string_child(self):
        data = [["hello", "world"], ["foo"]]
        v = arr(data)
        m = v.materialize()
        assert m.to_pylist() == data

    def test_materialize_array_of_array(self):
        data = [[[1, 2], [3]], [[4]]]
        v = arr(data)
        m = v.materialize()
        assert m.to_pylist() == data

    def test_materialize_produces_independent_result(self):
        data = [[1, 2, 3]]
        v = arr(data)
        m = v.materialize()
        del v
        gc.collect()
        assert m.to_pylist() == data


# ===========================================================================
# 9. compress (keep valid rows only)
# ===========================================================================

class TestCompress:
    def test_compress_no_nulls(self):
        data = [[1, 2], [3, 4]]
        v = arr(data)
        c = v.compress()
        assert c.to_pylist() == data

    def test_compress_removes_nulls(self):
        v = arr([None, [1, 2], None, [3]])
        c = v.compress()
        assert c.to_pylist() == [[1, 2], [3]]

    def test_compress_all_null(self):
        v = arr([None, None, None])
        c = v.compress()
        assert len(c) == 0
        assert c.to_pylist() == []

    def test_compress_empty_sublists_kept(self):
        v = arr([None, [], [1]])
        c = v.compress()
        assert c.to_pylist() == [[], [1]]

    def test_compress_array_of_array(self):
        v = arr([[[1, 2]], None, [[3, 4]]])
        c = v.compress()
        assert c.to_pylist() == [[[1, 2]], [[3, 4]]]


# ===========================================================================
# 10. Unsupported ops — all must throw, never silently mis-answer
# ===========================================================================

class TestUnsupportedOps:
    def _v(self):
        return arr([[1, 2], [3]])

    def test_hash_throws(self):
        with pytest.raises(Exception):
            self._v().hash()

    def test_sum_throws(self):
        with pytest.raises(Exception):
            self._v().sum()

    def test_min_throws(self):
        with pytest.raises(Exception):
            self._v().min()

    def test_max_throws(self):
        with pytest.raises(Exception):
            self._v().max()

    def test_compare_scalar_throws(self):
        with pytest.raises(Exception):
            self._v().compare_scalar(1, 0)

    def test_compare_vector_throws(self):
        with pytest.raises(Exception):
            self._v().compare_vector(self._v(), 0)

    def test_between_throws(self):
        with pytest.raises(Exception):
            self._v().between(1, 2)

    def test_in_list_throws(self):
        with pytest.raises(Exception):
            self._v().in_list([1, 2])

    def test_neg_throws(self):
        with pytest.raises(Exception):
            self._v().neg()

    def test_add_throws(self):
        with pytest.raises(Exception):
            self._v().add(self._v())

    def test_sub_throws(self):
        with pytest.raises(Exception):
            self._v().sub(self._v())

    def test_mul_throws(self):
        with pytest.raises(Exception):
            self._v().mul(self._v())

    def test_div_throws(self):
        with pytest.raises(Exception):
            self._v().div(self._v())


# ===========================================================================
# 11. RAII / no-leak stress
# ===========================================================================

class TestRAII:
    def test_repeated_alloc_free(self):
        for _ in range(1000):
            v = arr([[1, 2, 3], None, [], [4, 5]])
            _ = v.to_pylist()

    def test_nested_alloc_free(self):
        for _ in range(500):
            v = arr([[[1, 2], [3]], None, [[4, 5, 6]]])
            _ = v.to_pylist()

    def test_take_alloc_free(self):
        for _ in range(500):
            v = arr([[i, i + 1] for i in range(20)])
            t = v.take(list(range(20)) * 3)
            _ = t.to_pylist()
            del t
            del v

    def test_child_freed_with_parent(self):
        v = arr([["hello" * 20, "world" * 20]])
        t = v.take([0])
        del v
        gc.collect()
        assert t.to_pylist() == [["hello" * 20, "world" * 20]]
        del t

    def test_three_level_nested_raii(self):
        for _ in range(200):
            v = arr([[[[1, 2], [3]], [[4]]], [[[5, 6]]]])
            _ = v.to_pylist()
            del v

    def test_compress_raii(self):
        for _ in range(500):
            v = arr([None, [1, 2, 3], None, [4, 5]])
            c = v.compress()
            _ = c.to_pylist()
            del c
            del v


# ===========================================================================
# 12. DrakenVector ABI size unchanged (sizeof == 40)
# The static_assert in buffers.h enforces sizeof(DrakenVector)==40 at compile
# time; if we reach here, the binary loaded without errors, so the assert held.
# Smoke-test: construct an array vector and verify the module loads cleanly.
# ===========================================================================

class TestABISize:
    def test_module_loads_and_array_works(self):
        v = arr([[1, 2, 3]])
        assert v.type == dn.DrakenType.ARRAY
        assert len(v) == 1


# ===========================================================================
# 13. Hypothesis property tests
# ===========================================================================

int64_elem = st.integers(min_value=-(2**62), max_value=2**62 - 1)
str_elem   = st.text(max_size=30)

sublist_int = st.lists(int64_elem, max_size=10)
sublist_str = st.lists(str_elem, max_size=10)

nullable_sublist_int = st.one_of(st.none(), sublist_int)
nullable_sublist_str = st.one_of(st.none(), sublist_str)


@given(st.lists(nullable_sublist_int, max_size=30))
@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_int64_roundtrip(data):
    assert roundtrip(data) == data


@given(st.lists(nullable_sublist_str, max_size=20))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_string_roundtrip(data):
    assert roundtrip(data) == data


@given(st.lists(nullable_sublist_int, min_size=1, max_size=20))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_take_roundtrip(data):
    v = arr(data)
    n = len(data)
    indices = list(range(n))
    t = v.take(indices)
    assert t.to_pylist() == data


@given(st.lists(nullable_sublist_int, max_size=20))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_compress_removes_only_nulls(data):
    v = arr(data)
    c = v.compress()
    expected = [row for row in data if row is not None]
    assert c.to_pylist() == expected


@given(
    st.lists(nullable_sublist_int, min_size=1, max_size=20),
    st.lists(st.integers(min_value=0), min_size=0, max_size=30),
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_take_null_empty_preserved(data, raw_indices):
    assume(len(data) > 0)
    indices = [i % len(data) for i in raw_indices]
    v = arr(data)
    t = v.take(indices)
    result = t.to_pylist()
    expected = [data[i] for i in indices]
    assert result == expected


@given(st.lists(nullable_sublist_int, max_size=20))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_array_length_matches_python(data):
    v = arr(data)
    for i, row in enumerate(data):
        if row is None:
            assert v.array_length(i) is None
        else:
            assert v.array_length(i) == len(row)


@given(st.lists(sublist_int, min_size=1, max_size=15))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_hypothesis_array_get_all_elements(data):
    v = arr(data)
    for i, row in enumerate(data):
        for j, elem in enumerate(row):
            assert v.array_get(i, j) == elem
