"""Parity tests for the compiled hash kernels against hashlib.

The kernels live in the `opteryx.compiled.nanobind.vectors` extension (built
from `opteryx/compiled/nanobind/vector_hash_codec.cpp`) — NOT in
`opteryx.compiled.vector_ops`, which is an auto-generated consolidation of the
regex/DFA/case-helper pyx modules only.

Contract: VARCHAR Vector in → VARCHAR Vector out, one lowercase hex digest per
row, null rows propagating as null. There is no implicit str() coercion of
non-string input — `MD5(123)` is a deliberate typed error at the SQL boundary
that directs the caller to `123::VARCHAR`.
"""

import hashlib
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken
from opteryx.compiled.nanobind.vectors import (
    vector_md5,
    vector_sha1,
    vector_sha224,
    vector_sha256,
    vector_sha384,
    vector_sha512,
)

ALGORITHMS = [
    ("md5", vector_md5),
    ("sha1", vector_sha1),
    ("sha224", vector_sha224),
    ("sha256", vector_sha256),
    ("sha384", vector_sha384),
    ("sha512", vector_sha512),
]


def _varchar(values):
    return draken.vector_from_sequence(values, "VARCHAR")


@pytest.mark.parametrize("algo,kernel", ALGORITHMS)
def test_hash_matches_hashlib(algo, kernel):
    inputs = ["", "a", "abc", "hello world", "x" * 100, "héllo"]
    expected = [getattr(hashlib, algo)(v.encode("utf-8")).hexdigest() for v in inputs]
    assert list(kernel(_varchar(inputs))) == expected


@pytest.mark.parametrize("algo,kernel", ALGORITHMS)
def test_null_rows_propagate_as_null(algo, kernel):
    out = list(kernel(_varchar(["hello", None, "abc"])))
    assert out[1] is None
    assert out[0] == getattr(hashlib, algo)(b"hello").hexdigest()
    assert out[2] == getattr(hashlib, algo)(b"abc").hexdigest()


@pytest.mark.parametrize("algo,kernel", ALGORITHMS)
def test_empty_input_is_empty_output(algo, kernel):
    assert list(kernel(_varchar([]))) == []


@pytest.mark.parametrize("algo,kernel", ALGORITHMS)
def test_digest_width_is_fixed(algo, kernel):
    """Every digest is the algorithm's full hex width, zero-padding included."""
    width = len(getattr(hashlib, algo)(b"").hexdigest())
    out = list(kernel(_varchar(["", "a", "abc"])))
    assert {len(d) for d in out} == {width}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
