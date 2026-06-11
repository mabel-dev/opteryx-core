"""
align_tables outer-join unmatched path (WP-06).

right_view[i] < 0 marks an unmatched (outer-join) row whose right columns must
be NULL. The path is now a native take_with_null gather (no to_pylist round-trip,
no per-type reconstruction), which also supports ARRAY / INTERVAL / VARBINARY —
types the old reconstruction path rejected.
"""

import os
import sys
from array import array

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel, align_tables


def _left(n):
    return Morsel.from_vectors([b"lid"], [dn.vector_from_sequence(list(range(n)))])


def test_unmatched_rows_are_null_string_float():
    left = _left(3)
    right = Morsel.from_vectors(
        [b"rs", b"rf"],
        [dn.vector_from_string_sequence(["a", "b", "c"]),
         dn.vector_float64_from_sequence([1.0, 2.0, 3.0])],
    )
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [2, -1, 0]))
    assert out.column(b"rs").to_pylist() == ["c", None, "a"]
    assert out.column(b"rf").to_pylist() == [3.0, None, 1.0]
    assert out.column(b"lid").to_pylist() == [0, 1, 2]


def test_all_unmatched():
    left = _left(3)
    right = Morsel.from_vectors([b"r"], [dn.vector_from_sequence([7, 8, 9])])
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [-1, -1, -1]))
    assert out.column(b"r").to_pylist() == [None, None, None]


def test_no_unmatched_fast_path():
    left = _left(3)
    right = Morsel.from_vectors([b"r"], [dn.vector_from_sequence([7, 8, 9])])
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [2, 0, 1]))
    assert out.column(b"r").to_pylist() == [9, 7, 8]


def test_source_null_and_unmatched_interaction():
    # A matched row pointing at a source-null row stays null; an unmatched row
    # is null regardless. Both must read null.
    left = _left(3)
    right = Morsel.from_vectors([b"x"], [dn.vector_from_sequence([5, None, 7])])
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [1, 2, -1]))
    assert out.column(b"x").to_pylist() == [None, 7, None]


def test_array_unmatched_new_capability():
    left = _left(3)
    right = Morsel.from_vectors([b"arr"], [dn.vector_array_from_sequence([["x", "y"], ["z"], ["w"]])])
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [0, -1, 2]))
    assert out.column(b"arr").to_pylist() == [["x", "y"], None, ["w"]]


def test_bool_unmatched():
    left = _left(3)
    right = Morsel.from_vectors([b"b"], [dn.vector_from_bool_sequence([True, False, True])])
    out = align_tables(left, right, array("i", [0, 1, 2]), array("i", [2, -1, 0]))
    assert out.column(b"b").to_pylist() == [True, None, True]


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
