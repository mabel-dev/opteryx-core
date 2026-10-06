"""
Native correctness tests for vector_contains_any / vector_contains_all
(opteryx/compiled/nanobind/vector_array_membership.cpp).

  vector_contains_any (array membership):
    basic True/False; empty items → all False; null rows → False; short rows.

  vector_contains_all (array membership):
    basic True/False; empty items → True for non-null rows; null rows → False.
"""


import draken.draken_native as dn

from opteryx.compiled.nanobind import vectors as vss


def arr(rows):
    """DRAKEN_ARRAY Vector from list of list[...] | None."""
    return dn.vector_array_from_sequence(rows)


def pylist(v):
    return v.to_pylist()


# ===========================================================================
# CONTAINS_ANY — array membership
# ===========================================================================

class TestContainsAny:
    def test_basic_match(self):
        v = arr([[1, 2, 3], [4, 5], [6]])
        r = pylist(vss.vector_contains_any(v, {2}))
        assert r == [True, False, False]

    def test_multiple_items(self):
        v = arr([[1, 2, 3], [4, 5], [6]])
        r = pylist(vss.vector_contains_any(v, {2, 4}))
        assert r == [True, True, False]

    def test_no_match(self):
        v = arr([[1, 2], [3, 4]])
        assert pylist(vss.vector_contains_any(v, {99})) == [False, False]

    def test_empty_items_all_false(self):
        v = arr([[1, 2], [3]])
        assert pylist(vss.vector_contains_any(v, set())) == [False, False]

    def test_null_row_gives_false(self):
        v = arr([[1, 2], None, [3]])
        r = pylist(vss.vector_contains_any(v, {2}))
        assert r == [True, False, False]

    def test_empty_array(self):
        v = arr([])
        assert pylist(vss.vector_contains_any(v, {1})) == []

    def test_string_items(self):
        v = arr([["a", "b"], ["c"], ["a"]])
        r = pylist(vss.vector_contains_any(v, {"a"}))
        assert r == [True, False, True]


# ===========================================================================
# CONTAINS_ALL — array membership
# ===========================================================================

class TestContainsAll:
    def test_all_present(self):
        v = arr([[1, 2, 3], [1, 3], [2]])
        r = pylist(vss.vector_contains_all(v, {1, 2}))
        assert r == [True, False, False]

    def test_single_item(self):
        v = arr([[1, 2, 3], [4, 5]])
        assert pylist(vss.vector_contains_all(v, {2})) == [True, False]

    def test_empty_items_vacuously_true(self):
        # Empty needle set → True for all non-null rows (vacuous truth).
        v = arr([[1, 2], [3]])
        assert pylist(vss.vector_contains_all(v, set())) == [True, True]

    def test_null_row_gives_false(self):
        v = arr([[1, 2, 3], None, [1, 2]])
        r = pylist(vss.vector_contains_all(v, {1, 2}))
        assert r == [True, False, True]

    def test_empty_array(self):
        v = arr([])
        assert pylist(vss.vector_contains_all(v, {1})) == []

    def test_not_all_present(self):
        v = arr([[1, 2], [3, 4]])
        assert pylist(vss.vector_contains_all(v, {1, 3})) == [False, False]

    def test_string_items(self):
        v = arr([["a", "b", "c"], ["a", "b"], ["c"]])
        r = pylist(vss.vector_contains_all(v, {"a", "b"}))
        assert r == [True, True, False]
