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


# --- cxx_hash: the first nogil kernel-backed CxxMorsel op ---
from draken.morsels.morsel import Morsel

_U64 = (1 << 64) - 1


def _check_hash(vectors):
    """cxx_hash (nogil, no PyObject) must equal Morsel.hash (the c_hash dense-mix path)."""
    names = [str(i).encode() for i in range(len(vectors))]
    ref = list(Morsel.from_vectors(names, vectors).hash())  # uint64
    cxx_vec = dn._cxx_hash(vectors, list(range(len(vectors))))
    assert cxx_vec.type == DT.INT64
    got = [(x & _U64) for x in cxx_vec.to_pylist()]  # INT64 bits → uint64
    assert ref == got, (ref, got)


def test_cxx_hash_single_int64():
    _check_hash([vector_from_sequence([1, 2, 3, 4, None], dtype=DT.INT64)])


def test_cxx_hash_single_varchar():
    _check_hash([vector_from_sequence([b"a", b"bb", b"long arena string here", None], dtype=DT.VARCHAR)])


def test_cxx_hash_multi_int():
    _check_hash([
        vector_from_sequence([1, 2, 3], dtype=DT.INT64),
        vector_from_sequence([10, 20, 30], dtype=DT.INT64),
    ])


def test_cxx_hash_multi_mixed():
    _check_hash([
        vector_from_sequence([1, 2], dtype=DT.INT64),
        vector_from_sequence([True, False], dtype=DT.BOOL),
        vector_from_sequence([b"x", b"y"], dtype=DT.VARCHAR),
    ])


# --- cxx_take: nogil row-gather, must match Morsel.take ---
def _check_take(vectors, idx):
    names = [str(i).encode() for i in range(len(vectors))]
    ref = Morsel.from_vectors(names, vectors).take(idx)
    ref_cols = [ref[i].to_pylist() for i in range(len(vectors))]
    out = dn._cxx_take(vectors, list(idx))
    got_cols = [out[i].to_pylist() for i in range(len(vectors))]
    assert ref_cols == got_cols, (ref_cols, got_cols)
    for i in range(len(vectors)):
        assert out[i].type == vectors[i].type


def test_cxx_take_int64_reorder():
    _check_take([vector_from_sequence([10, 20, 30, 40, None], dtype=DT.INT64)], [4, 0, 2, 2, 1])


def test_cxx_take_varchar_dup():
    _check_take([vector_from_sequence([b"a", b"bb", b"long arena string xyz", None], dtype=DT.VARCHAR)], [3, 2, 0, 2])


def test_cxx_take_multi_col():
    _check_take([
        vector_from_sequence([1, 2, 3], dtype=DT.INT64),
        vector_from_sequence([b"p", b"q", b"r"], dtype=DT.VARCHAR),
    ], [2, 0, 1, 1])


def test_cxx_take_empty():
    _check_take([vector_from_sequence([1, 2, 3], dtype=DT.INT64)], [])


# --- cxx_mask / cxx_slice / cxx_combine, vs the Morsel equivalents ---
def _cols(m, ncols):
    return [m[i].to_pylist() for i in range(ncols)]


def test_cxx_mask():
    vs = [
        vector_from_sequence([10, 20, 30, 40, None], dtype=DT.INT64),
        vector_from_sequence([b"a", b"b", b"long arena str xyz", b"d", b"e"], dtype=DT.VARCHAR),
    ]
    maskv = vector_from_sequence([True, False, True, None, True], dtype=DT.BOOL)
    ref = Morsel.from_vectors([b"0", b"1"], vs).filter_mask(maskv)
    got = dn._cxx_mask(vs, maskv)
    assert _cols(ref, 2) == [got[i].to_pylist() for i in range(2)]


def test_cxx_slice():
    vs = [
        vector_from_sequence([10, 20, 30, 40, 50], dtype=DT.INT64),
        vector_from_sequence([b"a", b"b", b"c", b"d", b"e"], dtype=DT.VARCHAR),
    ]
    ref = Morsel.from_vectors([b"0", b"1"], vs).slice(1, 3)
    got = dn._cxx_slice(vs, 1, 3)
    assert _cols(ref, 2) == [got[i].to_pylist() for i in range(2)]


def test_cxx_combine():
    m1 = [vector_from_sequence([1, 2], dtype=DT.INT64), vector_from_sequence([b"a", b"bb"], dtype=DT.VARCHAR)]
    m2 = [vector_from_sequence([3], dtype=DT.INT64), vector_from_sequence([b"long arena string here"], dtype=DT.VARCHAR)]
    m3 = [vector_from_sequence([4, 5, None], dtype=DT.INT64), vector_from_sequence([b"x", None, b"z"], dtype=DT.VARCHAR)]
    ref = Morsel.combine([Morsel.from_vectors([b"0", b"1"], x) for x in (m1, m2, m3)])
    got = dn._cxx_combine([m1, m2, m3])
    assert _cols(ref, 2) == [got[i].to_pylist() for i in range(2)]


if __name__ == "__main__":
    test_roundtrip_int64()
    test_roundtrip_float64()
    test_roundtrip_bool()
    test_roundtrip_varchar_with_nulls_and_arena()
    test_roundtrip_multi_column()
    test_roundtrip_empty()
    test_cxx_hash_single_int64()
    test_cxx_hash_single_varchar()
    test_cxx_hash_multi_int()
    test_cxx_hash_multi_mixed()
    test_cxx_take_int64_reorder()
    test_cxx_take_varchar_dup()
    test_cxx_take_multi_col()
    test_cxx_take_empty()
    test_cxx_mask()
    test_cxx_slice()
    test_cxx_combine()
    print("✅ S0 CxxMorsel seam + hash/take/mask/slice/combine — all byte-identical")
