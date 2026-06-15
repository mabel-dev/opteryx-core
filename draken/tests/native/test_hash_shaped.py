"""
Native correctness tests for draken_hash_shaped (the shape-preserving keying
hash used by group-by / join / distinct).

The invariant under test: hashing preserves shape, and the shaped hash read
through its uniform access pattern (data[selection[i]]) is byte-identical to
the dense per-row hash for EVERY shape — dense, dict, constant, and their
nullable variants. Null keys collide on a baked NULL_HASH slot; the hash
vector itself is always fully valid (validity is never set on a hash vector).
"""

import draken.draken_native as dn

_MASK = (1 << 64) - 1


def _u(x):
    return x & _MASK


def _parity(v):
    """Materialized shaped hash == dense per-row hash, compared as uint64 bits."""
    dense = [_u(x) for x in v.hash()]
    hv = v.hash_shaped()
    mat = [_u(x) for x in hv.materialize().to_pylist()]
    return dense, mat, hv


class TestShapedHashParity:
    def test_dict_string(self):
        v = dn.vector_from_string_dict_sequence([b"a", b"b", b"a", b"c", b"b"] * 3)
        dense, mat, hv = _parity(v)
        assert hv.is_dict and hv.data_length == 3
        assert mat == dense

    def test_dense_string(self):
        v = dn.vector_from_string_sequence([("x" + str(i)).encode("utf-8") for i in range(20)])
        dense, mat, hv = _parity(v)
        assert hv.is_dense and hv.data_length == 20
        assert mat == dense

    def test_constant_string(self):
        v = dn.vector_from_string_dict_sequence([b"hi"] * 16)
        dense, mat, hv = _parity(v)
        assert hv.is_constant
        assert mat == dense

    def test_dense_int64(self):
        v = dn.vector_from_sequence(list(range(20)))
        dense, mat, hv = _parity(v)
        assert hv.is_dense
        assert mat == dense

    def test_nullable_dict_string(self):
        v = dn.vector_from_string_dict_sequence([b"a", b"b", None, b"a", None, b"c"] * 3)
        dense, mat, hv = _parity(v)
        assert mat == dense

    def test_nullable_dense_int64(self):
        v = dn.vector_from_sequence([1, None, 2, None, 3, 4])
        dense, mat, hv = _parity(v)
        assert mat == dense

    def test_all_null_dict(self):
        v = dn.vector_from_string_dict_sequence([None] * 8)
        dense, mat, hv = _parity(v)
        assert mat == dense
        # All null rows hash identically (collide on the baked null slot).
        assert len(set(mat)) == 1

    def test_constant_with_nulls(self):
        v = dn.vector_from_string_dict_sequence([b"z", None, b"z", None] * 2)
        dense, mat, hv = _parity(v)
        assert mat == dense

    def test_empty(self):
        v = dn.vector_from_sequence([])
        dense, mat, hv = _parity(v)
        assert mat == dense == []

    def test_hash_vector_is_never_null(self):
        # A hash vector's slots always hold a real value; nulls collide on a
        # baked slot, so the vector is fully valid regardless of key nullity.
        v = dn.vector_from_string_dict_sequence([b"a", None, b"b", None])
        hv = v.hash_shaped()
        # to_pylist returns no None entries — every hash is present.
        assert all(x is not None for x in hv.materialize().to_pylist())
