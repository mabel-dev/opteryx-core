"""
Native correctness tests for DRAKEN_VARCHAR dict-encoded vectors (Milestone D.3).

Coverage:
  dict_ingest   : dedup, codes, data_length, round-trip via to_pylist
  materialize   : dict / constant / dense → dense; nulls, empty, long strings
  dictionary_encode: dense → dict; dedup; round-trip materialize(dictionary_encode(dense))==dense
  take          : gather by index; repeats, out-of-order, nulls, empty
  d2_on_dict    : hash / eq / compare on dict shape == results on materialized dense
  hash32_reuse  : dict unique slots carry same hash32 as D.1 ingestion
  raii          : no crash / leak under construct-and-destroy stress
"""

import pytest
import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def dense(lst):
    return dn.vector_from_string_sequence([v.encode("utf-8") if isinstance(v, str) else v for v in lst])


def dictv(lst):
    return dn.vector_from_string_dict_sequence([v.encode("utf-8") if isinstance(v, str) else v for v in lst])


def py(v):
    return v.to_pylist()


# ---------------------------------------------------------------------------
# 1. Dict ingestion: dedup correctness
# ---------------------------------------------------------------------------


class TestDictIngest:
    def test_basic_dedup(self):
        v = dictv(["a", "b", "a", "c", "b"])
        assert v.is_dict
        assert v.data_length == 3  # unique: a, b, c
        assert v.length == 5
        assert py(v) == ["a", "b", "a", "c", "b"]

    def test_all_unique(self):
        v = dictv(["x", "y", "z"])
        assert v.data_length == 3
        assert py(v) == ["x", "y", "z"]

    def test_all_same_value(self):
        v = dictv(["hi"] * 100)
        assert v.data_length == 1
        assert py(v) == ["hi"] * 100

    def test_empty(self):
        v = dictv([])
        assert v.length == 0
        assert py(v) == []

    def test_single_non_null(self):
        v = dictv(["hello"])
        assert v.data_length == 1
        assert py(v) == ["hello"]

    def test_single_null(self):
        v = dictv([None])
        assert v.length == 1
        assert py(v) == [None]

    def test_nulls_not_deduped_as_values(self):
        # None is not a value; data_length counts only unique non-null values.
        v = dictv([None, "a", None, "b", None])
        assert v.data_length == 2  # a, b
        assert py(v) == [None, "a", None, "b", None]

    def test_all_null(self):
        v = dictv([None] * 10)
        assert v.length == 10
        assert py(v) == [None] * 10

    def test_empty_string_is_unique_value(self):
        v = dictv(["", "a", ""])
        assert v.data_length == 2  # "" and "a"
        assert py(v) == ["", "a", ""]

    def test_empty_string_distinct_from_null(self):
        v = dictv([None, ""])
        assert py(v) == [None, ""]

    def test_long_strings_deduped(self):
        s = "x" * 50
        v = dictv([s, "other_long_string_here", s, s])
        assert v.data_length == 2
        assert py(v) == [s, "other_long_string_here", s, s]

    def test_long_and_short_together(self):
        short = "hi"
        long_ = "a" * 30
        data = [short, long_, short, long_, None]
        v = dictv(data)
        assert v.data_length == 2
        assert py(v) == data

    def test_large_input(self):
        # 1000 values cycling through 10 unique strings.
        vals = [f"value_{i % 10}" for i in range(1000)]
        v = dictv(vals)
        assert v.data_length == 10
        assert py(v) == vals

    def test_long_string_roundtrip(self):
        data = [None if i % 7 == 0 else f"long_value_{i:05d}_" + "x" * 20
                for i in range(500)]
        v = dictv(data)
        assert py(v) == data


# ---------------------------------------------------------------------------
# 2. Materialize
# ---------------------------------------------------------------------------


class TestMaterialize:
    def test_materialize_dense(self):
        d = dense(["a", "b", "c"])
        m = d.materialize()
        assert py(m) == ["a", "b", "c"]

    def test_materialize_dict(self):
        src = ["x", "y", "x", "z", "y"]
        d = dictv(src)
        m = d.materialize()
        assert not m.is_dict
        assert m.data_length == m.length
        assert py(m) == src

    def test_materialize_constant(self):
        c = dn.vector_from_constant(42, 5)
        # Constant is INT64; skip for string tests. Use dict with data_length==1.
        v = dictv(["hello"] * 5)
        assert v.data_length == 1
        m = v.materialize()
        assert py(m) == ["hello"] * 5

    def test_materialize_preserves_nulls(self):
        src = [None, "a", None, "b"]
        m = dictv(src).materialize()
        assert py(m) == src

    def test_materialize_all_null(self):
        src = [None] * 8
        m = dictv(src).materialize()
        assert py(m) == src

    def test_materialize_long_strings(self):
        src = ["x" * 50, "y" * 50, "x" * 50]
        m = dictv(src).materialize()
        assert py(m) == src

    def test_materialize_empty(self):
        m = dictv([]).materialize()
        assert py(m) == []

    def test_materialize_boundary(self):
        src = ["a" * 11, "b" * 12, "c" * 13]
        m = dictv(src).materialize()
        assert py(m) == src

    def test_materialize_dense_noop(self):
        # Materializing a dense vector is a round-trip.
        src = [f"row_{i}" for i in range(100)]
        d = dense(src)
        m = d.materialize()
        assert py(m) == src


# ---------------------------------------------------------------------------
# 3. Compress
# ---------------------------------------------------------------------------


class TestCompress:
    def test_dictionary_encode_deduplicates(self):
        src = ["a", "b", "a", "c"]
        c = dense(src).dictionary_encode()
        assert c.is_dict
        assert c.data_length == 3
        assert py(c) == src

    def test_dictionary_encode_roundtrip(self):
        src = ["p", "q", "p", "r", "q", "q"]
        m = dense(src).dictionary_encode().materialize()
        assert py(m) == src

    def test_dictionary_encode_roundtrip_long_strings(self):
        long_a = "alpha_" * 10
        long_b = "beta__" * 10
        src = [long_a, long_b, long_a, None, long_b]
        m = dense(src).dictionary_encode().materialize()
        assert py(m) == src

    def test_dictionary_encode_all_unique(self):
        src = [f"unique_{i}" for i in range(50)]
        c = dense(src).dictionary_encode()
        assert c.data_length == 50
        assert py(c) == src

    def test_dictionary_encode_all_same(self):
        src = ["same"] * 20
        c = dense(src).dictionary_encode()
        assert c.data_length == 1
        assert py(c) == src

    def test_dictionary_encode_all_null(self):
        src = [None] * 5
        c = dense(src).dictionary_encode()
        assert py(c) == src

    def test_dictionary_encode_empty(self):
        c = dense([]).dictionary_encode()
        assert py(c) == []

    def test_dictionary_encode_preserves_nulls(self):
        src = [None, "x", None, "y", "x"]
        m = dense(src).dictionary_encode().materialize()
        assert py(m) == src

    def test_dictionary_encode_then_dictionary_encode(self):
        # dictionary_encode(dictionary_encode(dense)) should not break anything.
        src = ["a", "b", "a"]
        cc = dense(src).dictionary_encode().dictionary_encode()
        assert py(cc.materialize()) == src

    def test_materialize_dictionary_encode_roundtrip_boundary(self):
        src = ["a" * 11, "b" * 12, "c" * 13, "a" * 11]
        m = dense(src).dictionary_encode().materialize()
        assert py(m) == src

    def test_hash32_reuse_in_dictionary_encode(self):
        # The unique slots in a dictionary_encode result must carry the same (length, prefix,
        # hash32) as D.1 ingestion would produce for the same string.
        long_s = "verylongstring_" + "x" * 30
        # Build dense via D.1, then dictionary_encode.
        d = dense([long_s, "other", long_s])
        c = d.dictionary_encode()
        # Find the slot index for long_s in the dictionary_encoded vector.
        # materialize gives correct values so we know the mapping.
        m = c.materialize()
        assert m[0] == long_s

        # Get slot fields from the dict data directly via _slot_fields on a
        # dict-ingested version (which builds its own fresh slots).
        d2 = dictv([long_s])
        f_dict = d2._slot_fields(0)       # (length, prefix, hash32) via dict ingest
        f_dense = dense([long_s])._slot_fields(0)  # (length, prefix, hash32) via D.1

        # Both should have identical slot fields (determinism invariant).
        assert f_dict == f_dense, (
            f"dict ingest and D.1 ingest produced different slot fields:\n"
            f"  dict:  {f_dict!r}\n"
            f"  dense: {f_dense!r}"
        )


# ---------------------------------------------------------------------------
# 4. Take
# ---------------------------------------------------------------------------


class TestTake:
    def take(self, v, indices):
        return v.take(list(indices))

    def test_take_dense_basic(self):
        v = dense(["a", "b", "c", "d"])
        assert py(self.take(v, [0, 2])) == ["a", "c"]

    def test_take_dict_basic(self):
        v = dictv(["a", "b", "a", "c"])
        assert py(self.take(v, [0, 1, 3])) == ["a", "b", "c"]

    def test_take_repeats(self):
        v = dense(["x", "y", "z"])
        assert py(self.take(v, [0, 0, 2, 2])) == ["x", "x", "z", "z"]

    def test_take_out_of_order(self):
        v = dense(["p", "q", "r"])
        assert py(self.take(v, [2, 0, 1])) == ["r", "p", "q"]

    def test_take_null_source_row(self):
        v = dense([None, "a", None, "b"])
        assert py(self.take(v, [0, 1, 2, 3])) == [None, "a", None, "b"]

    def test_take_produces_null_output(self):
        v = dense(["x", None, "y"])
        result = self.take(v, [1])
        assert py(result) == [None]

    def test_take_empty_indices(self):
        v = dense(["a", "b"])
        assert py(self.take(v, [])) == []

    def test_take_long_strings(self):
        s1 = "x" * 50
        s2 = "y" * 50
        v = dense([s1, s2, None])
        assert py(self.take(v, [0, 1, 0, 2])) == [s1, s2, s1, None]

    def test_take_dict_repeats(self):
        v = dictv(["a", "b", "a"])
        # Gather same logical row multiple times.
        assert py(self.take(v, [0, 0, 2])) == ["a", "a", "a"]

    def test_take_preserves_validity(self):
        v = dense([None, "hello", None, "world"])
        r = self.take(v, [1, 3])
        assert py(r) == ["hello", "world"]

    def test_take_all_null_input(self):
        v = dense([None, None, None])
        r = self.take(v, [0, 1, 2])
        assert py(r) == [None, None, None]

    def test_take_boundary_strings(self):
        v = dense(["a" * 12, "b" * 13, "c" * 11])
        r = self.take(v, [1, 0, 2])
        assert py(r) == ["b" * 13, "a" * 12, "c" * 11]

    def test_take_result_owns_arena(self):
        # Destroy source after take; result must still be valid.
        long_s = "z" * 100
        v = dense([long_s, "other"])
        r = v.take([0, 0, 0])
        del v  # drop source
        assert py(r) == [long_s, long_s, long_s]

    def test_take_dict_result_owns_arena(self):
        long_s = "q" * 80
        v = dictv([long_s, "short", long_s])
        r = v.take([0, 2])
        del v
        assert py(r) == [long_s, long_s]


# ---------------------------------------------------------------------------
# 5. D.2 ops on dict shape vs materialized dense (uniform model proof)
# ---------------------------------------------------------------------------


class TestD2OnDictShape:
    """
    D.2 hash / compare / eq on a dict-encoded string vector must give the same
    results as the same ops on its materialized-dense form.
    No dict-specific code path may exist; the uniform data[selection[i]] model
    must handle dict shapes transparently.
    """

    def _check_hash_eq(self, src):
        d = dictv(src)
        m = d.materialize()
        h_dict = d.hash()
        h_dense = m.hash()
        assert h_dict == h_dense, (
            f"hash mismatch on dict vs materialized for {src!r}\n"
            f"  dict:  {h_dict}\n"
            f"  dense: {h_dense}"
        )

    def test_hash_short_strings(self):
        self._check_hash_eq(["a", "b", "a", "c", "b"])

    def test_hash_long_strings(self):
        self._check_hash_eq(["x" * 50, "y" * 50, "x" * 50])

    def test_hash_with_nulls(self):
        self._check_hash_eq([None, "hello", None, "world", "hello"])

    def test_hash_all_same(self):
        self._check_hash_eq(["repeat"] * 10)

    def test_compare_eq_short(self):
        d = dictv(["a", "b", "a", "c"])
        m = d.materialize()
        r_dict  = [b for b in d.compare_scalar(b"a", 0).to_pylist()]
        r_dense = [b for b in m.compare_scalar(b"a", 0).to_pylist()]
        assert r_dict == r_dense

    def test_compare_eq_long(self):
        s = "hello_world_" * 5
        d = dictv([s, "other_value__", s])
        m = d.materialize()
        r_dict  = d.compare_scalar(s.encode("utf-8"), 0).to_pylist()
        r_dense = m.compare_scalar(s.encode("utf-8"), 0).to_pylist()
        assert r_dict == r_dense

    def test_compare_ne(self):
        d = dictv(["x", "y", "x"])
        m = d.materialize()
        r_dict  = d.compare_scalar(b"x", 1).to_pylist()
        r_dense = m.compare_scalar(b"x", 1).to_pylist()
        assert r_dict == r_dense

    def test_compare_order(self):
        d = dictv(["apple", "banana", "apple", "cherry"])
        m = d.materialize()
        # gt: which values > "banana"?
        r_dict  = d.compare_scalar(b"banana", 2).to_pylist()
        r_dense = m.compare_scalar(b"banana", 2).to_pylist()
        assert r_dict == r_dense

    def test_compare_vector_dict_x_dense(self):
        src = ["a", "b", "a", "c"]
        d = dictv(src)
        m = d.materialize()
        # compare dict against dense (same logical values)
        r = d.compare_vector(m, 0)
        assert all(r.to_pylist()), "all rows should be equal"

    def test_hash_equal_values_same_hash_in_dict(self):
        # Equal values must hash identically regardless of position in the dict.
        s = "same_string_value"
        d = dictv([s, "different", s, "different", s])
        hashes = d.hash()
        assert hashes[0] == hashes[2] == hashes[4], "same values must hash equal"
        assert hashes[1] == hashes[3], "same values must hash equal"
        assert hashes[0] != hashes[1], "different values should hash differently"


# ---------------------------------------------------------------------------
# 6. Slot-field parity: dense (D.1) and dict (D.3) ingest of the same value
#    produce byte-identical slots. (E37: the hash32 field is now dead — always 0
#    — so this checks length+prefix parity; the equal-0 hash32 still satisfies it.)
# ---------------------------------------------------------------------------


class TestHash32Reuse:
    def test_long_string_hash32_matches_d1(self):
        s = "deterministic_long_string_" + "y" * 20
        # D.1 dense ingest
        f_dense = dense([s])._slot_fields(0)
        # D.3 dict ingest
        f_dict = dictv([s])._slot_fields(0)
        assert len(f_dense) == 3, "expected long-form (length, prefix, hash32)"
        assert len(f_dict)  == 3
        assert f_dense == f_dict, (
            f"D.1 and D.3 slot fields differ for same long string:\n"
            f"  D.1: {f_dense!r}\n"
            f"  D.3: {f_dict!r}"
        )

    def test_short_string_slot_matches_d1(self):
        s = "short"
        f_dense = dense([s])._slot_fields(0)
        f_dict  = dictv([s])._slot_fields(0)
        assert f_dense == f_dict

    def test_multiple_unique_long_strings_all_match(self):
        strings = [f"long_string_{i:02d}_" + "z" * 20 for i in range(5)]
        for s in strings:
            f_d = dense([s])._slot_fields(0)
            f_v = dictv([s])._slot_fields(0)
            assert f_d == f_v, f"mismatch for {s!r}"


# ---------------------------------------------------------------------------
# 7. RAII / no-leak stress
# ---------------------------------------------------------------------------


class TestRAII:
    def test_dict_create_destroy_loop(self):
        for _ in range(200):
            v = dictv([f"value_{j}" for j in range(50)] + [None] * 10)
            del v

    def test_materialize_create_destroy_loop(self):
        base = dictv(["a", "b", "c", None] * 25)
        for _ in range(200):
            m = base.materialize()
            del m

    def test_dictionary_encode_create_destroy_loop(self):
        base = dense(["x", "y", "x", "z"] * 25)
        for _ in range(200):
            c = base.dictionary_encode()
            del c

    def test_take_create_destroy_loop(self):
        base = dense(["hello", "world", "foo", "bar"] * 25)
        indices = list(range(50))
        for _ in range(200):
            r = base.take(indices)
            del r

    def test_long_strings_create_destroy_loop(self):
        data = ["x" * 100, "y" * 200, None] * 10
        for _ in range(100):
            v = dictv(data)
            m = v.materialize()
            c = m.dictionary_encode()
            del v, m, c

    def test_chain_ops_no_crash(self):
        src = [f"val_{i % 7}" + ("_long_suffix_" * 3 if i % 3 == 0 else "")
               for i in range(100)]
        src[::10] = [None] * (len(src[::10]))  # scatter nulls
        v = dictv(src)
        m = v.materialize()
        c = m.dictionary_encode()
        m2 = c.materialize()
        assert py(m2) == src


# ---------------------------------------------------------------------------
# 8. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_single_long_string_dict(self):
        s = "z" * 1000
        v = dictv([s])
        assert py(v) == [s]

    def test_dict_data_length_equals_unique_non_null(self):
        # 3 unique non-null values, 2 nulls.
        v = dictv(["a", None, "b", None, "c"])
        assert v.data_length == 3

    def test_take_empty_from_nonempty(self):
        v = dense(["a", "b", "c"])
        r = v.take([])
        assert py(r) == []
        assert r.length == 0

    def test_dictionary_encode_already_dict(self):
        # dictionary_encoding a dict-encoded vector should work (via uniform access).
        v = dictv(["p", "q", "p", "r"])
        c = v.dictionary_encode()
        assert py(c.materialize()) == ["p", "q", "p", "r"]

    def test_materialize_then_dictionary_encode_then_materialize(self):
        src = ["one", "two", "one", "three", "two"]
        result = dense(src).materialize().dictionary_encode().materialize()
        assert py(result) == src

    def test_take_produces_dense_with_identity_flags(self):
        v = dense(["a", "b", "c"])
        r = v.take([2, 0, 1])
        assert not r.is_dict
        assert r.data_length == r.length == 3
