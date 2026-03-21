import os
import sys
from array import array as pyarray

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.morsels.morsel import Morsel

morsel_sort = pytest.importorskip(
    "opteryx.compiled.morsel_ops.sort",
    reason="morsel_ops.sort not built",
).morsel_sort


# ── Helpers ───────────────────────────────────────────────────────────────────


def _morsel(data: dict) -> Morsel:
    """Build a Morsel from a dict of column_name -> list."""
    return Morsel.from_arrow(pa.table({k: pa.array(v) for k, v in data.items()}))


def _apply(morsel: Morsel, perm) -> dict:
    """Apply a uint32 permutation to morsel and return column values as lists."""
    int32_perm = pyarray("i", perm)
    morsel.take(int32_perm)
    return {
        name.decode() if isinstance(name, bytes) else name: morsel.column(
            name if isinstance(name, bytes) else name.encode()
        ).to_pylist()
        for name in morsel.column_names
    }


def _sort_col(data: list, col: str, ascending: bool = True) -> list:
    """Reference sort using Python's sorted() — None sorts first for ASC."""
    def key(x):
        if x is None:
            return (0, None)
        return (1, x)
    reverse = not ascending
    return [row[col] for row in sorted(
        [{col: v} for v in data],
        key=lambda r: key(r[col]),
        reverse=reverse,
    )]


def _col(morsel: Morsel, name: str) -> list:
    return morsel.column(name.encode()).to_pylist()


# ── Int64 ─────────────────────────────────────────────────────────────────────


def test_int64_ascending():
    m = _morsel({"x": [3, 1, 4, 1, 5, 9, 2, 6]})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    assert result["x"] == sorted([3, 1, 4, 1, 5, 9, 2, 6])


def test_int64_descending():
    m = _morsel({"x": [3, 1, 4, 1, 5, 9, 2, 6]})
    perm = morsel_sort(m, [b"x"], [False])
    result = _apply(m, perm)
    assert result["x"] == sorted([3, 1, 4, 1, 5, 9, 2, 6], reverse=True)


def test_int64_negative_values():
    values = [-5, 0, -100, 3, -1, 7]
    m = _morsel({"x": values})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    assert result["x"] == sorted(values)


def test_int64_all_equal():
    m = _morsel({"x": [7, 7, 7, 7], "seq": [3, 1, 2, 0]})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    # All keys equal — permutation is some ordering of [0,1,2,3]
    assert sorted(result["x"]) == [7, 7, 7, 7]
    assert sorted(perm) == [0, 1, 2, 3]


# ── Float64 ───────────────────────────────────────────────────────────────────


def test_float64_ascending():
    values = [3.14, 1.0, -2.5, 0.0, 100.0]
    m = _morsel({"x": pa.array(values, type=pa.float64())})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    assert result["x"] == pytest.approx(sorted(values))


def test_float64_descending():
    values = [3.14, 1.0, -2.5, 0.0, 100.0]
    m = _morsel({"x": pa.array(values, type=pa.float64())})
    perm = morsel_sort(m, [b"x"], [False])
    result = _apply(m, perm)
    assert result["x"] == pytest.approx(sorted(values, reverse=True))


# ── Short strings (≤ 7 bytes — prefix key is exact) ──────────────────────────


def test_string_short_ascending():
    values = [b"fig", b"apple", b"cherry", b"date", b"banana"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


def test_string_short_descending():
    values = [b"fig", b"apple", b"cherry", b"date", b"banana"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [False])
    result = _apply(m, perm)
    assert result["s"] == sorted(values, reverse=True)


def test_string_empty_string():
    values = [b"b", b"", b"a", b"", b"c"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


# ── Long strings (> 7 bytes — tiebreak path exercised) ───────────────────────


def test_string_long_ascending():
    # All share the same 7-byte prefix "abcdefg"
    values = [b"abcdefgZZZ", b"abcdefgAAA", b"abcdefgMMM", b"abcdefgBBB"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


def test_string_long_descending():
    values = [b"abcdefgZZZ", b"abcdefgAAA", b"abcdefgMMM", b"abcdefgBBB"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [False])
    result = _apply(m, perm)
    assert result["s"] == sorted(values, reverse=True)


def test_string_mixed_short_and_long():
    # Some share prefix, some don't; mix of short and long
    values = [b"z", b"abcdefgZZZ", b"abcdefgAAA", b"a", b"abcdefgMMM"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


def test_string_prefix_differs_at_boundary():
    # "abcdefg" (7 bytes, exact prefix) vs "abcdefgX" (same prefix + 1 byte)
    values = [b"abcdefgX", b"abcdefg", b"abcdefgA"]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


# ── Dictionary-encoded columns ────────────────────────────────────────────────


def test_dict_int_ascending():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([2, 0, 1, 2, 0], type=pa.int8()),
        pa.array([30, 10, 20], type=pa.int32()),
    )
    m = Morsel.from_arrow(pa.table({"x": arr}))
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    # Same code → same group; expect grouped together (codes 0=10, 1=20, 2=30)
    assert result["x"] == [10, 10, 20, 30, 30]


def test_dict_int_descending():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([2, 0, 1, 2, 0], type=pa.int8()),
        pa.array([30, 10, 20], type=pa.int32()),
    )
    m = Morsel.from_arrow(pa.table({"x": arr}))
    perm = morsel_sort(m, [b"x"], [False])
    result = _apply(m, perm)
    assert result["x"] == [30, 30, 20, 10, 10]


def test_dict_string_groups_identical_values():
    # Dictionary-encoded strings: sort on codes groups equal strings together
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 2, 1, 0, 2], type=pa.int8()),
        pa.array(["apple", "cherry", "banana"], type=pa.string()),
    )
    m = Morsel.from_arrow(pa.table({"s": arr}))
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    # All rows with the same code must be adjacent
    s = result["s"]
    # Verify grouping: no value appears in two separate non-adjacent spans
    seen = {}
    for i, v in enumerate(s):
        if v not in seen:
            seen[v] = i
        else:
            assert i == seen[v] + 1 or s[i - 1] == v, (
                f"Value {v!r} is not grouped: {s}"
            )
            seen[v] = i


# ── Multi-column sort ─────────────────────────────────────────────────────────


def test_multi_column_int_int_asc_asc():
    # (primary, secondary): expect sorted by primary first, then secondary
    primary =   [2, 1, 2, 1, 3]
    secondary = [9, 5, 1, 7, 3]
    m = _morsel({"p": primary, "s": secondary})
    perm = morsel_sort(m, [b"p", b"s"], [True, True])
    result = _apply(m, perm)
    expected = sorted(zip(primary, secondary))
    assert list(zip(result["p"], result["s"])) == expected


def test_multi_column_int_int_asc_desc():
    primary =   [2, 1, 2, 1, 3]
    secondary = [9, 5, 1, 7, 3]
    m = _morsel({"p": primary, "s": secondary})
    perm = morsel_sort(m, [b"p", b"s"], [True, False])
    result = _apply(m, perm)
    expected = sorted(zip(primary, secondary), key=lambda t: (t[0], -t[1]))
    assert list(zip(result["p"], result["s"])) == expected


def test_multi_column_string_int():
    keys   = [b"banana", b"apple", b"banana", b"apple", b"cherry"]
    values = [2,        1,       1,        3,       1      ]
    m = _morsel({"k": pa.array(keys, type=pa.binary()), "v": values})
    perm = morsel_sort(m, [b"k", b"v"], [True, True])
    result = _apply(m, perm)
    pairs = sorted(zip(keys, values))
    assert list(zip(result["k"], result["v"])) == pairs


def test_multi_column_stability_equal_primary():
    # When primary keys are equal, secondary order must be respected
    primary   = [1, 1, 1, 1]
    secondary = [4, 2, 3, 1]
    m = _morsel({"p": primary, "s": secondary})
    perm = morsel_sort(m, [b"p", b"s"], [True, True])
    result = _apply(m, perm)
    assert result["s"] == [1, 2, 3, 4]


# ── Null handling ─────────────────────────────────────────────────────────────


def test_null_int64_nulls_first_ascending():
    values = pa.array([3, None, 1, None, 2], type=pa.int64())
    m = _morsel({"x": values})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    # Nulls first, then ascending non-null values
    assert result["x"][:2] == [None, None]
    assert result["x"][2:] == [1, 2, 3]


def test_null_int64_nulls_last_descending():
    values = pa.array([3, None, 1, None, 2], type=pa.int64())
    m = _morsel({"x": values})
    perm = morsel_sort(m, [b"x"], [False])
    result = _apply(m, perm)
    # Descending non-null values, nulls last
    assert result["x"][:3] == [3, 2, 1]
    assert result["x"][3:] == [None, None]


def test_null_string_ascending():
    values = pa.array([b"b", None, b"a", None], type=pa.binary())
    m = _morsel({"s": values})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"][:2] == [None, None]
    assert result["s"][2:] == [b"a", b"b"]


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_empty_morsel():
    m = _morsel({"x": pa.array([], type=pa.int64())})
    perm = morsel_sort(m, [b"x"], [True])
    assert list(perm) == []


def test_single_row():
    m = _morsel({"x": [42]})
    perm = morsel_sort(m, [b"x"], [True])
    assert list(perm) == [0]


def test_two_rows_ascending():
    m = _morsel({"x": [9, 1]})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    assert result["x"] == [1, 9]


def test_two_rows_already_sorted():
    m = _morsel({"x": [1, 9]})
    perm = morsel_sort(m, [b"x"], [True])
    assert list(perm) == [0, 1]


def test_permutation_is_a_valid_bijection():
    """The permutation must contain each row index exactly once."""
    n = 100
    import random
    random.seed(42)
    values = [random.randint(-50, 50) for _ in range(n)]
    m = _morsel({"x": pa.array(values, type=pa.int64())})
    perm = morsel_sort(m, [b"x"], [True])
    assert len(perm) == n
    assert sorted(perm) == list(range(n))


def test_mismatched_column_and_ascending_lengths_raises():
    m = _morsel({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        morsel_sort(m, [b"x", b"x"], [True])


def test_empty_column_list_raises():
    m = _morsel({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        morsel_sort(m, [], [])


# ── Correctness vs Python reference ──────────────────────────────────────────


def test_int64_matches_python_sort_random():
    import random
    random.seed(0)
    values = [random.randint(-1000, 1000) for _ in range(500)]
    m = _morsel({"x": pa.array(values, type=pa.int64())})
    perm = morsel_sort(m, [b"x"], [True])
    result = _apply(m, perm)
    assert result["x"] == sorted(values)


def test_string_matches_python_sort_random():
    import random
    import string
    random.seed(1)

    def rand_str(n):
        return "".join(random.choices(string.ascii_lowercase, k=n)).encode()

    values = [rand_str(random.randint(1, 20)) for _ in range(300)]
    m = _morsel({"s": pa.array(values, type=pa.binary())})
    perm = morsel_sort(m, [b"s"], [True])
    result = _apply(m, perm)
    assert result["s"] == sorted(values)


def test_multi_column_matches_python_sort_random():
    import random
    random.seed(2)
    n = 200
    a = [random.randint(0, 5) for _ in range(n)]   # low cardinality primary
    b = [random.randint(0, 100) for _ in range(n)]  # higher cardinality secondary
    m = _morsel({"a": pa.array(a, type=pa.int64()), "b": pa.array(b, type=pa.int64())})
    perm = morsel_sort(m, [b"a", b"b"], [True, True])
    result = _apply(m, perm)
    expected = sorted(zip(a, b))
    assert list(zip(result["a"], result["b"])) == expected
