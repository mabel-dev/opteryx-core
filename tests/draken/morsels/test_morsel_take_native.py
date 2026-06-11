"""
Morsel.take native path (WP-05).

Vector.take / Morsel.take / _take_inplace / align_tables now gather through the
native draken_vector_take_buffer bridge (typed int32 buffer, zero per-row
PyObject boxing) instead of an nb::list. These pin correctness across types,
including physical-type preservation (TIMESTAMP/DATE stay themselves after the
typed kernel hardcodes its tag) and the empty-take edge.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel


def test_take_int_string_float():
    m = Morsel.from_vectors(
        [b"i", b"s", b"f"],
        [dn.vector_from_sequence(list(range(10))),
         dn.vector_from_string_sequence([f"v{i}" for i in range(10)]),
         dn.vector_float64_from_sequence([float(i) for i in range(10)])],
    )
    t = m.take([9, 0, 5, 5, 2])
    assert t.column(b"i").to_pylist() == [9, 0, 5, 5, 2]
    assert t.column(b"s").to_pylist() == ["v9", "v0", "v5", "v5", "v2"]
    assert t.column(b"f").to_pylist() == [9.0, 0.0, 5.0, 5.0, 2.0]


def test_take_empty():
    m = Morsel.from_vectors([b"i"], [dn.vector_from_sequence([1, 2, 3])])
    e = m.take([])
    assert e.num_rows == 0
    assert e.num_columns == 1


def test_take_bool_preserves_type():
    m = Morsel.from_vectors([b"b"], [dn.vector_from_bool_sequence([True, False, True, True])])
    t = m.take([3, 0, 1])
    assert t.column(b"b").to_pylist() == [True, True, False]
    assert t.column(b"b").type == dn.BOOL


def test_take_timestamp_preserves_type():
    ts = [datetime.datetime(2020, 1, 1 + i) for i in range(5)]
    v = dn.vector_timestamp_from_sequence(ts)
    full = v.to_pylist()  # readback form (tz-aware), so we compare like-for-like
    m = Morsel.from_vectors([b"t"], [v])
    t = m.take([4, 2, 0])
    assert t.column(b"t").type == dn.TIMESTAMP64  # NOT downgraded to INT64
    assert t.column(b"t").to_pylist() == [full[4], full[2], full[0]]


def test_take_with_nulls():
    v = dn.vector_from_sequence([1, None, 3, None, 5])
    m = Morsel.from_vectors([b"i"], [v])
    t = m.take([4, 1, 3, 0])
    assert t.column(b"i").to_pylist() == [5, None, None, 1]


def test_take_repeated_and_reordered():
    m = Morsel.from_vectors([b"i"], [dn.vector_from_sequence([10, 20, 30])])
    t = m.take([2, 2, 2, 0])
    assert t.column(b"i").to_pylist() == [30, 30, 30, 10]


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
