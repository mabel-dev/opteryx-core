"""
S0 (C++-first CxxMorsel) — seam round-trip proof.

Verifies that a Python Vector → CxxMorsel (shared_ptr<VectorOwner>) → Python Vector
round-trip is byte-identical across types, nulls, long (arena) strings, and multiple
columns. This proves the shared_ptr seam (draken_native._cxx_morsel_roundtrip) keeps
the underlying buffers alive and does not copy or corrupt data.

See docs/M4_CPP_MORSEL_DESIGN.md (S0).
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence

DT = dn.DrakenType


def _roundtrip_one(seq, dtype):
    v = vector_from_sequence(seq, dtype=dtype)
    before = v.to_pylist()
    out = dn._cxx_morsel_roundtrip([v])
    assert len(out) == 1
    assert out[0].type == v.type
    assert out[0].to_pylist() == before, (dtype, before, out[0].to_pylist())


def test_roundtrip_int64():
    _roundtrip_one([1, 2, 3, None, 5], DT.INT64)


def test_roundtrip_float64():
    _roundtrip_one([1.5, None, 3.25, -7.0], DT.FLOAT64)


def test_roundtrip_bool():
    _roundtrip_one([True, False, None, True], DT.BOOL)


def test_roundtrip_varchar_with_nulls_and_arena():
    # Mix of short (inline) and long (>12B, arena) strings plus a null.
    _roundtrip_one([b"a", b"hello world long arena string", None, b"x"], DT.VARCHAR)


def test_roundtrip_multi_column():
    a = vector_from_sequence([1, 2, 3], dtype=DT.INT64)
    b = vector_from_sequence([b"p", b"q", b"r"], dtype=DT.VARCHAR)
    a_before, b_before = a.to_pylist(), b.to_pylist()
    out = dn._cxx_morsel_roundtrip([a, b])
    assert len(out) == 2
    assert out[0].type == DT.INT64 and out[1].type == DT.VARCHAR
    assert out[0].to_pylist() == a_before
    assert out[1].to_pylist() == b_before


def test_roundtrip_empty():
    out = dn._cxx_morsel_roundtrip([])
    assert list(out) == []


if __name__ == "__main__":
    test_roundtrip_int64()
    test_roundtrip_float64()
    test_roundtrip_bool()
    test_roundtrip_varchar_with_nulls_and_arena()
    test_roundtrip_multi_column()
    test_roundtrip_empty()
    print("✅ S0 CxxMorsel seam round-trip — all byte-identical")
