"""Tests for draken.morsels.sort — the ONE sort implementation (vergesort prepass
+ comparison-sort fallback over AoS/SortKeyCmp keys, draken/morsels/sort.hpp).

Rewritten against the real API (sort_morsels / morsel_sort) after the previous
version of this file was found to be dead: it imported a module path
(opteryx.compiled.morsel_ops.sort) that no longer exists — `setup.py` has a
stale comment saying it moved to draken.morsels.sort — so pytest.importorskip
silently skipped the whole file regardless of environment. It separately called
`Morsel.from_arrow(...)`, which was never a real constructor on the compiled
`Morsel` class (only `from_vectors`/`from_cxx`/`from_cxx_vectors` exist). Both
bugs meant this suite never actually ran and never caught anything.
"""

import os
import sys
from array import array as pyarray

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
import pytest
from draken.morsels.morsel import Morsel
from draken.morsels.sort import morsel_sort, sort_morsels

# ── Helpers ───────────────────────────────────────────────────────────────────


def _vector_for(values):
    """Pick the right dn.vector_*_from_sequence constructor for a Python list."""
    sample = next((v for v in values if v is not None), None)
    if isinstance(sample, float):
        return dn.vector_float64_from_sequence(values)
    if isinstance(sample, bytes):
        return dn.vector_from_bytes_sequence(values)
    if isinstance(sample, str):
        return dn.vector_from_string_sequence(values)
    if isinstance(sample, int) or sample is None:
        return dn.vector_from_sequence(values)
    raise TypeError(f"no vector constructor wired up for sample {sample!r}")


def _morsel(data: dict) -> Morsel:
    """Build a Morsel from a dict of column_name -> list[value]."""
    names = [k.encode() if isinstance(k, str) else k for k in data]
    vectors = [_vector_for(v) for v in data.values()]
    return Morsel.from_vectors(names, vectors)


def _col(morsels, name: bytes) -> list:
    """Materialize and concatenate one column's values across a list of output Morsels."""
    out = []
    for m in morsels:
        m.materialize()
        out.extend(m.column(name).to_pylist())
    return out


def _cols(morsels, names: list) -> dict:
    return {name: _col(morsels, name) for name in names}


def _row_count(morsels) -> int:
    return sum(m.num_rows for m in morsels)


# ── Int64 ─────────────────────────────────────────────────────────────────────


def test_int64_ascending():
    m = _morsel({"x": [3, 1, 4, 1, 5, 9, 2, 6]})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == sorted([3, 1, 4, 1, 5, 9, 2, 6])


def test_int64_descending():
    m = _morsel({"x": [3, 1, 4, 1, 5, 9, 2, 6]})
    out = sort_morsels([m], [b"x"], [False])
    assert _col(out, b"x") == sorted([3, 1, 4, 1, 5, 9, 2, 6], reverse=True)


def test_int64_negative_values():
    values = [-5, 0, -100, 3, -1, 7]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == sorted(values)


def test_int64_all_equal():
    m = _morsel({"x": [7, 7, 7, 7], "seq": [3, 1, 2, 0]})
    out = sort_morsels([m], [b"x"], [True])
    result = _cols(out, [b"x", b"seq"])
    assert result[b"x"] == [7, 7, 7, 7]
    assert sorted(result[b"seq"]) == [0, 1, 2, 3]


# ── Float64 — the bug found and fixed this session ───────────────────────────
#
# The old morsel_sort's numeric keys went through Vector.compress(), which
# masked away the bit that keeps negative floats ordered below positive ones:
# -2.5 sorted AFTER 1.0. sort_morsels/morsel_sort now build keys via
# sort_num_key (draken/morsels/sort.hpp), which handles the IEEE-754 sign bit
# correctly. These two tests are the direct regression check for that bug.


def test_float64_ascending():
    values = [3.14, 1.0, -2.5, 0.0, 100.0]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == pytest.approx(sorted(values))


def test_float64_descending():
    values = [3.14, 1.0, -2.5, 0.0, 100.0]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [False])
    assert _col(out, b"x") == pytest.approx(sorted(values, reverse=True))


def test_float64_negative_sorts_before_positive_regression():
    """Exact case that was broken: a small-magnitude negative float must sort
    before ANY positive float, not land between two positive values."""
    values = [-2.5, 0.0, 1.0, 3.14, 100.0]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == values  # already in correct ascending order


# ── Short strings ─────────────────────────────────────────────────────────────


def test_string_short_ascending():
    values = [b"fig", b"apple", b"cherry", b"date", b"banana"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


def test_string_short_descending():
    values = [b"fig", b"apple", b"cherry", b"date", b"banana"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [False])
    assert _col(out, b"s") == sorted(values, reverse=True)


def test_string_empty_string():
    values = [b"b", b"", b"a", b"", b"c"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


# ── Long strings (prefix ties exercise the tiebreak path) ────────────────────


def test_string_long_ascending():
    values = [b"abcdefgZZZ", b"abcdefgAAA", b"abcdefgMMM", b"abcdefgBBB"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


def test_string_long_descending():
    values = [b"abcdefgZZZ", b"abcdefgAAA", b"abcdefgMMM", b"abcdefgBBB"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [False])
    assert _col(out, b"s") == sorted(values, reverse=True)


def test_string_mixed_short_and_long():
    values = [b"z", b"abcdefgZZZ", b"abcdefgAAA", b"a", b"abcdefgMMM"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


def test_string_prefix_differs_at_boundary():
    values = [b"abcdefgX", b"abcdefg", b"abcdefgA"]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


# ── Dictionary-encoded columns ────────────────────────────────────────────────


def test_dict_int_ascending():
    # codes index directly into values: row_i = values[codes[i]] (same convention
    # as pyarrow's DictionaryArray.from_arrays(indices, dictionary)). codes=[2,0,1,2,0]
    # over values=[30,10,20] materializes to [20,30,10,20,30] — verified directly
    # against the vector's own to_pylist() before trusting this as an expectation.
    v = dn.vector_from_dict(values=[30, 10, 20], codes=[2, 0, 1, 2, 0])
    m = Morsel.from_vectors([b"x"], [v])
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == [10, 20, 20, 30, 30]


def test_dict_int_descending():
    v = dn.vector_from_dict(values=[30, 10, 20], codes=[2, 0, 1, 2, 0])
    m = Morsel.from_vectors([b"x"], [v])
    out = sort_morsels([m], [b"x"], [False])
    assert _col(out, b"x") == [30, 30, 20, 20, 10]


def test_dict_string_groups_identical_values():
    v = dn.vector_from_string_dict_sequence(
        [b"apple", b"cherry", b"banana", b"apple", b"cherry"]
    )
    m = Morsel.from_vectors([b"s"], [v])
    out = sort_morsels([m], [b"s"], [True])
    s = _col(out, b"s")
    seen = {}
    for i, val in enumerate(s):
        if val not in seen:
            seen[val] = i
        else:
            assert i == seen[val] + 1 or s[i - 1] == val, f"{val!r} not grouped: {s}"
            seen[val] = i


# ── Multi-column sort ─────────────────────────────────────────────────────────


def test_multi_column_int_int_asc_asc():
    primary = [2, 1, 2, 1, 3]
    secondary = [9, 5, 1, 7, 3]
    m = _morsel({"p": primary, "s": secondary})
    out = sort_morsels([m], [b"p", b"s"], [True, True])
    result = _cols(out, [b"p", b"s"])
    expected = sorted(zip(primary, secondary))
    assert list(zip(result[b"p"], result[b"s"])) == expected


def test_multi_column_int_int_asc_desc():
    primary = [2, 1, 2, 1, 3]
    secondary = [9, 5, 1, 7, 3]
    m = _morsel({"p": primary, "s": secondary})
    out = sort_morsels([m], [b"p", b"s"], [True, False])
    result = _cols(out, [b"p", b"s"])
    expected = sorted(zip(primary, secondary), key=lambda t: (t[0], -t[1]))
    assert list(zip(result[b"p"], result[b"s"])) == expected


def test_multi_column_string_int():
    keys = [b"banana", b"apple", b"banana", b"apple", b"cherry"]
    values = [2, 1, 1, 3, 1]
    m = _morsel({"k": keys, "v": values})
    out = sort_morsels([m], [b"k", b"v"], [True, True])
    result = _cols(out, [b"k", b"v"])
    assert list(zip(result[b"k"], result[b"v"])) == sorted(zip(keys, values))


def test_multi_column_stability_equal_primary():
    primary = [1, 1, 1, 1]
    secondary = [4, 2, 3, 1]
    m = _morsel({"p": primary, "s": secondary})
    out = sort_morsels([m], [b"p", b"s"], [True, True])
    assert _col(out, b"s") == [1, 2, 3, 4]


# ── NULL handling ─────────────────────────────────────────────────────────────


def test_null_int64_nulls_first_ascending():
    m = _morsel({"x": [3, None, 1, None, 2]})
    out = sort_morsels([m], [b"x"], [True])
    result = _col(out, b"x")
    assert result[:2] == [None, None]
    assert result[2:] == [1, 2, 3]


def test_null_int64_nulls_last_descending():
    m = _morsel({"x": [3, None, 1, None, 2]})
    out = sort_morsels([m], [b"x"], [False])
    result = _col(out, b"x")
    assert result[:3] == [3, 2, 1]
    assert result[3:] == [None, None]


def test_null_string_ascending():
    m = _morsel({"s": [b"b", None, b"a", None]})
    out = sort_morsels([m], [b"s"], [True])
    result = _col(out, b"s")
    assert result[:2] == [None, None]
    assert result[2:] == [b"a", b"b"]


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_empty_morsel():
    m = _morsel({"x": []})
    out = sort_morsels([m], [b"x"], [True])
    assert _row_count(out) == 0


def test_single_row():
    m = _morsel({"x": [42]})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == [42]


def test_two_rows_ascending():
    m = _morsel({"x": [9, 1]})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == [1, 9]


def test_no_morsels_returns_empty_list():
    assert sort_morsels([], [b"x"], [True]) == []


def test_mismatched_column_and_ascending_lengths_raises():
    m = _morsel({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        sort_morsels([m], [b"x", b"x"], [True])


def test_empty_column_list_raises():
    m = _morsel({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        sort_morsels([m], [], [])


def test_unknown_column_name_raises():
    m = _morsel({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        sort_morsels([m], [b"does_not_exist"], [True])


# ── Multi-morsel sort — the capability this phase actually added ────────────
# (the old single-morsel morsel_sort had no way to sort rows spanning more
# than one Morsel; SortSink needs exactly that, and this proves it works.)


def test_multi_morsel_sort_merges_across_morsels():
    m1 = _morsel({"x": [5, 3, 1]})
    m2 = _morsel({"x": [4, 2, 0]})
    out = sort_morsels([m1, m2], [b"x"], [True])
    assert _col(out, b"x") == [0, 1, 2, 3, 4, 5]


def test_multi_morsel_sort_with_limit():
    m1 = _morsel({"x": [5, 3, 1]})
    m2 = _morsel({"x": [4, 2, 0]})
    out = sort_morsels([m1, m2], [b"x"], [True], limit=2)
    assert _col(out, b"x") == [0, 1]
    assert _row_count(out) == 2


# ── The single-morsel permutation API (morsel_sort) ──────────────────────────


def test_morsel_sort_permutation_is_a_valid_bijection():
    import random

    random.seed(42)
    n = 100
    values = [random.randint(-50, 50) for _ in range(n)]
    m = _morsel({"x": values})
    perm = morsel_sort(m, [b"x"], [True])
    assert isinstance(perm, pyarray)
    assert len(perm) == n
    assert sorted(perm) == list(range(n))
    reordered = [values[i] for i in perm]
    assert reordered == sorted(values)


def test_morsel_sort_single_row():
    m = _morsel({"x": [42]})
    assert list(morsel_sort(m, [b"x"], [True])) == [0]


# ── Correctness vs Python reference (random, larger inputs) ──────────────────


def test_int64_matches_python_sort_random():
    import random

    random.seed(0)
    values = [random.randint(-1000, 1000) for _ in range(500)]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == sorted(values)


def test_string_matches_python_sort_random():
    import random
    import string

    random.seed(1)

    def rand_str(n):
        return "".join(random.choices(string.ascii_lowercase, k=n)).encode()

    values = [rand_str(random.randint(1, 20)) for _ in range(300)]
    m = _morsel({"s": values})
    out = sort_morsels([m], [b"s"], [True])
    assert _col(out, b"s") == sorted(values)


def test_multi_column_matches_python_sort_random():
    import random

    random.seed(2)
    n = 200
    a = [random.randint(0, 5) for _ in range(n)]
    b = [random.randint(0, 100) for _ in range(n)]
    m = _morsel({"a": a, "b": b})
    out = sort_morsels([m], [b"a", b"b"], [True, True])
    result = _cols(out, [b"a", b"b"])
    assert list(zip(result[b"a"], result[b"b"])) == sorted(zip(a, b))


def test_float64_matches_python_sort_random_with_negatives():
    """Larger randomized float regression covering the sign-order bug at scale,
    not just the 5-value hand-picked case above."""
    import random

    random.seed(3)
    values = [random.uniform(-1000.0, 1000.0) for _ in range(500)]
    m = _morsel({"x": values})
    out = sort_morsels([m], [b"x"], [True])
    assert _col(out, b"x") == sorted(values)
