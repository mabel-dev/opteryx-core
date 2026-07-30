"""
Correctness for the native ``Vector.null_count()`` kernel (draken_native.cpp),
which replaces the ``int(sum(vec.is_null()))`` pattern (``is_null()`` boxes the
whole column via ``to_pylist()`` — see ``_vector_shim.pyx``) with a single
validity-bitmap popcount pass.

Explicit byte-boundary cases (n=0,1,7,8,9,15,16,17,63,64,65) are the point of
this file: the bitmap is allocated in whole bytes, so a length not a multiple
of 8 leaves padding bits in the final byte whose value is not a documented
contract — the kernel must mask them out explicitly rather than assume 0.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from draken.interop.vector_sequence import vector_from_sequence


@pytest.mark.parametrize("n", [0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 200])
def test_no_nulls_returns_zero_at_every_byte_boundary(n):
    vec = vector_from_sequence(list(range(n)), dtype="INT64")
    assert vec.null_count() == 0


@pytest.mark.parametrize("n", [1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 200])
def test_all_null_returns_length_at_every_byte_boundary(n):
    vec = vector_from_sequence([None] * n, dtype="INT64")
    assert vec.null_count() == n


@pytest.mark.parametrize("n", [7, 8, 9, 15, 16, 17, 63, 64, 65])
def test_single_null_at_the_tail_boundary(n):
    # Null lands in the padding-bit region of the final byte for non-multiple-
    # of-8 lengths -- exactly the case the tail-byte mask exists to handle.
    values = list(range(n - 1)) + [None]
    vec = vector_from_sequence(values, dtype="INT64")
    assert vec.null_count() == 1


def test_empty_column_returns_zero():
    vec = vector_from_sequence([], dtype="INT64")
    assert vec.null_count() == 0


def test_mixed_null_count_matches_python_reference():
    import random

    rng = random.Random(1234)
    for _ in range(20):
        n = rng.randint(1, 300)
        values = [None if rng.random() < 0.3 else rng.randint(-1000, 1000) for _ in range(n)]
        vec = vector_from_sequence(values, dtype="INT64")
        expected = sum(1 for v in values if v is None)
        assert vec.null_count() == expected


def test_string_column_null_count():
    vec = vector_from_sequence(["a", None, "bb", None, None, "ccc"], dtype="VARCHAR")
    assert vec.null_count() == 3


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
