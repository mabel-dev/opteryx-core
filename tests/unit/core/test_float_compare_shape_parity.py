"""
Float compare/between shape parity (WP-14).

float_compare_scalar and float_between gained constant/dict/identity fast paths.
They must produce byte-identical results to the uniform per-row path, including
draken float total order (NaN sorts highest, -0.0 == 0.0). The dict path uses the
identical predicate as the dense loop, so special-value handling matches exactly.
"""

import math
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn

NAN = float("nan")
INF = float("inf")


def _canon(x):
    return 0.0 if x == 0.0 else x


def _gt(a, b):
    an, bn = math.isnan(a), math.isnan(b)
    if an and bn:
        return False
    if an:
        return True
    if bn:
        return False
    return _canon(a) > _canon(b)


def _eq(a, b):
    an, bn = math.isnan(a), math.isnan(b)
    if an and bn:
        return True
    if an or bn:
        return False
    return _canon(a) == _canon(b)


def _uniform(xs, s, op):
    out = []
    for x in xs:
        if op == 0:
            out.append(_eq(x, s))
        elif op == 1:
            out.append(not _eq(x, s))
        elif op == 2:
            out.append(_gt(x, s))
        elif op == 3:
            out.append(_gt(x, s) or _eq(x, s))
        elif op == 4:
            out.append(not _gt(x, s) and not _eq(x, s))
        else:
            out.append(not _gt(x, s))
    return out


VALS = [1.5, NAN, -0.0, 0.0, INF, -INF, 2.5]
PERM = [0, 1, 2, 3, 4, 5, 6, 0, 1, 2]
LOGICAL = [VALS[c] for c in PERM]


def test_dict_compare_all_ops():
    v = dn.vector_float64_from_dict(VALS, PERM)
    for op in range(6):
        assert v.compare_scalar(1.5, op).to_pylist() == _uniform(LOGICAL, 1.5, op), op


def test_constant_compare_all_ops():
    v = dn.vector_float64_from_constant(2.5, 5)
    for op in range(6):
        assert v.compare_scalar(1.5, op).to_pylist() == _uniform([2.5] * 5, 1.5, op), op


def test_dense_compare_matches_uniform():
    v = dn.vector_float64_from_sequence(LOGICAL)
    for op in range(6):
        assert v.compare_scalar(1.5, op).to_pylist() == _uniform(LOGICAL, 1.5, op), op


def test_between_dict_constant_dense():
    expected = [(not math.isnan(x)) and 1.0 <= _canon(x) <= 3.0 for x in LOGICAL]
    assert dn.vector_float64_from_dict(VALS, PERM).between(1.0, 3.0, True, True).to_pylist() == expected
    assert dn.vector_float64_from_sequence(LOGICAL).between(1.0, 3.0, True, True).to_pylist() == expected
    # constant within / outside range
    assert dn.vector_float64_from_constant(2.0, 4).between(1.0, 3.0, True, True).to_pylist() == [True] * 4
    assert dn.vector_float64_from_constant(9.0, 4).between(1.0, 3.0, True, True).to_pylist() == [False] * 4


if __name__ == "__main__":  # pragma: no cover
    test_dict_compare_all_ops()
    test_constant_compare_all_ops()
    test_dense_compare_matches_uniform()
    test_between_dict_constant_dense()
    print("✅ okay")
