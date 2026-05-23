"""
Milestone B.1 acceptance tests for draken.draken_native.

Run with:  python -m pytest draken/tests/test_draken_native_b1.py -v
"""

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_vec(lst):
    return dn.vector_from_sequence(lst)


# ---------------------------------------------------------------------------
# Round-trip: values and nulls
# ---------------------------------------------------------------------------


def test_round_trip_no_nulls():
    values = [5, 3, 99, -1, 0, 2**62]
    vec = make_vec(values)
    assert vec.to_pylist() == values


def test_round_trip_with_nulls():
    values = [5, 3, None, 7]
    vec = make_vec(values)
    assert vec.to_pylist() == values


def test_null_positions():
    values = [None, 1, None, 3, None]
    vec = make_vec(values)
    result = vec.to_pylist()
    for i, v in enumerate(values):
        if v is None:
            assert result[i] is None, f"expected None at index {i}"
        else:
            assert result[i] == v, f"expected {v} at index {i}"


def test_all_nulls():
    values = [None, None, None]
    vec = make_vec(values)
    assert vec.to_pylist() == [None, None, None]


def test_all_valid_validity_is_null():
    # When no nulls, validity must stay NULL (normalization invariant).
    # We can't inspect the raw pointer from Python, but we can prove behaviour:
    vec = make_vec([1, 2, 3])
    assert all(v is not None for v in vec.to_pylist())


def test_empty_vector():
    vec = make_vec([])
    assert len(vec) == 0
    assert vec.to_pylist() == []


def test_single_element():
    assert make_vec([42]).to_pylist() == [42]
    assert make_vec([None]).to_pylist() == [None]


# ---------------------------------------------------------------------------
# __len__ and .length
# ---------------------------------------------------------------------------


def test_len():
    assert len(make_vec([1, 2, 3])) == 3
    assert len(make_vec([])) == 0


def test_length_prop():
    vec = make_vec([10, 20, 30, 40])
    assert vec.length == 4


# ---------------------------------------------------------------------------
# .type
# ---------------------------------------------------------------------------


def test_type_is_int64():
    vec = make_vec([1, 2, 3])
    assert vec.type == dn.DrakenType.INT64
    assert vec.type.value == 4  # frozen ABI value


# ---------------------------------------------------------------------------
# __getitem__
# ---------------------------------------------------------------------------


def test_getitem_values():
    vec = make_vec([5, 3, None, 7])
    assert vec[0] == 5
    assert vec[1] == 3
    assert vec[2] is None
    assert vec[3] == 7


def test_getitem_negative_index():
    vec = make_vec([10, 20, 30])
    assert vec[-1] == 30
    assert vec[-3] == 10


def test_getitem_out_of_range():
    vec = make_vec([1, 2, 3])
    try:
        _ = vec[10]
        assert False, "expected IndexError"
    except IndexError:
        pass


# ---------------------------------------------------------------------------
# Dense shape invariants (selection is the shared identity global)
# ---------------------------------------------------------------------------


def test_dense_shape_data_length_equals_length():
    # Verified indirectly: all values round-trip without corruption.
    values = list(range(200))
    vec = make_vec(values)
    assert vec.to_pylist() == values


def test_large_vector_with_nulls():
    n = 10_000
    values = [i if i % 7 != 0 else None for i in range(n)]
    vec = make_vec(values)
    result = vec.to_pylist()
    assert result == values


# ---------------------------------------------------------------------------
# Morsel
# ---------------------------------------------------------------------------


def test_morsel_construct_and_access():
    v0 = make_vec([1, 2, 3])
    v1 = make_vec([None, 5, None])
    m = dn.Morsel()
    m.append(v0)
    m.append(v1)
    assert len(m) == 2
    assert m[0].to_pylist() == [1, 2, 3]
    assert m[1].to_pylist() == [None, 5, None]


def test_morsel_negative_index():
    m = dn.Morsel()
    m.append(make_vec([99]))
    assert m[-1].to_pylist() == [99]


def test_morsel_index_error():
    m = dn.Morsel()
    m.append(make_vec([1]))
    try:
        _ = m[5]
        assert False, "expected IndexError"
    except IndexError:
        pass


def test_morsel_empty():
    m = dn.Morsel()
    assert len(m) == 0


# ---------------------------------------------------------------------------
# RAII stress: construct/destroy in a loop — no leak or crash
# ---------------------------------------------------------------------------


def test_construct_destroy_stress():
    for _ in range(5_000):
        vec = make_vec([1, None, 3, None, 5])
        assert vec.to_pylist() == [1, None, 3, None, 5]
        # vec goes out of scope → VectorOwner destructor → draken_free


def test_morsel_construct_destroy_stress():
    for _ in range(2_000):
        m = dn.Morsel()
        for j in range(4):
            m.append(make_vec([j, None, j * 2]))
        assert len(m) == 4
        # m goes out of scope → Morsel destructor releases refs → VectorOwner destructors
