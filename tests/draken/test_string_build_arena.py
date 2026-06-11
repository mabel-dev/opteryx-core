"""
String build arena correctness (WP-09).

Inline strings (len <= STR_INLINE_MAX = 12) live in the slot and never reference
the arena; only long strings (len > 12) occupy arena bytes. The builders size the
arena from long strings only and skip zeroing the arena region. These pin the
12/13-byte inline boundary, nulls, empty strings, and that gather/take after the
build still reproduces every value.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn


CASES = {
    "all-short": ["abc", "defgh", "ij"],
    "boundary-12-inline": ["123456789012"],          # exactly 12 → inline
    "boundary-13-arena": ["1234567890123"],          # exactly 13 → arena
    "boundary-mixed": ["123456789012", "1234567890123", "x"],
    "all-long": ["x" * 20, "y" * 30],
    "mixed": ["a", "b" * 40, "cc", "d" * 13],
    "with-nulls": ["short", None, "a" * 50, None],
    "empty-strings": ["", "", "abc"],
    "long-then-short": ["z" * 64, "q"],
}


def test_roundtrip_all_cases():
    for name, vals in CASES.items():
        v = dn.vector_from_string_sequence(vals)
        assert v.to_pylist() == vals, (name, v.to_pylist())


def test_gather_after_build():
    # take/slice copy the arena; ensure dead-byte trimming didn't drop payload.
    for name, vals in CASES.items():
        v = dn.vector_from_string_sequence(vals)
        order = list(range(len(vals)))[::-1]
        t = v.take(order)
        assert t.to_pylist() == [vals[i] for i in order], (name, t.to_pylist())


def test_long_string_distinct_from_prefix_collision():
    # Two long strings sharing the first 12 bytes must remain distinct after
    # build (the arena holds the full bytes; equality verifies them).
    a = "abcdefghijkl" + "MMMMMM"
    b = "abcdefghijkl" + "NNNNNN"
    v = dn.vector_from_string_sequence([a, b, a])
    assert v.to_pylist() == [a, b, a]


if __name__ == "__main__":  # pragma: no cover
    test_roundtrip_all_cases()
    test_gather_after_build()
    test_long_string_distinct_from_prefix_collision()
    print("✅ okay")
