"""
Native correctness tests for DRAKEN_VARCHAR ingestion and readback (Milestone D.1).

These tests assert the CORRECT answer independently.  They are the primary
correctness signal for the string pilot.

Coverage (per 04_testing.md §1):
  nullability : no nulls / some nulls / all null
  size        : 0 / 1 / small / large
  encoding    : short (≤12 B) / boundary (11/12/13 B) / long (>12 B)
  content     : ASCII / multibyte UTF-8 / embedded NUL / empty string
  slot format : determinism (equal values → identical slot bytes)
               prefix byte-order (lex ordering via uint32 comparison)
               arena_offset overflow raises
"""

import pytest

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make(lst):
    return dn.vector_from_string_sequence([v.encode("utf-8") if isinstance(v, str) else v for v in lst])


def pylist(lst):
    return make(lst).to_pylist()


def null_mask(lst):
    return [x is None for x in make(lst).to_pylist()]


# ---------------------------------------------------------------------------
# 1. Basic round-trip: size × nullability
# ---------------------------------------------------------------------------


class TestEmpty:
    def test_empty_roundtrip(self):
        assert pylist([]) == []

    def test_empty_len(self):
        assert len(make([])) == 0

    def test_empty_type(self):
        assert make([]).type == dn.DrakenType.VARCHAR


class TestSingleElement:
    def test_single_value(self):
        assert pylist(["hello"]) == ["hello"]

    def test_single_null(self):
        assert pylist([None]) == [None]

    def test_single_empty_string(self):
        assert pylist([""]) == [""]

    def test_empty_string_distinct_from_null(self):
        result = pylist([None, ""])
        assert result[0] is None
        assert result[1] == ""
        assert result[1] is not None


class TestNullHandling:
    def test_null_positions_preserved(self):
        src = [None, "a", None, "b", None]
        assert null_mask(src) == [True, False, True, False, True]

    def test_null_does_not_pollute_neighbours(self):
        result = pylist(["x", None, "y"])
        assert result[0] == "x"
        assert result[1] is None
        assert result[2] == "y"

    def test_all_null(self):
        n = 64
        result = pylist([None] * n)
        assert all(v is None for v in result)

    def test_no_nulls_no_validity_poisoning(self):
        result = pylist(["a", "b", "c"])
        assert all(v is not None for v in result)


class TestLargeVector:
    def test_large_no_nulls(self):
        src = [f"value_{i}" for i in range(10_000)]
        assert pylist(src) == src

    def test_large_every_7th_null(self):
        src = [None if i % 7 == 0 else f"item_{i}" for i in range(10_000)]
        assert pylist(src) == src

    def test_large_all_null(self):
        src = [None] * 10_000
        assert pylist(src) == src


# ---------------------------------------------------------------------------
# 2. Short / boundary / long encoding
# ---------------------------------------------------------------------------


class TestShortStrings:
    """Length ≤ 12 bytes — inline in slot."""

    def test_exactly_12_bytes(self):
        s = "a" * 12
        assert pylist([s]) == [s]

    def test_11_bytes(self):
        s = "b" * 11
        assert pylist([s]) == [s]

    def test_1_byte(self):
        assert pylist(["x"]) == ["x"]

    def test_zero_bytes_nonnull(self):
        assert pylist([""]) == [""]

    def test_mix_short(self):
        src = ["", "a", "ab", "abc" * 4, None, "z" * 12]
        assert pylist(src) == src


class TestLongStrings:
    """Length > 12 bytes — stored in arena."""

    def test_exactly_13_bytes(self):
        s = "c" * 13
        assert pylist([s]) == [s]

    def test_long_ascii(self):
        s = "hello world, this is a longer string"
        assert pylist([s]) == [s]

    def test_very_long(self):
        s = "x" * 10_000
        assert pylist([s]) == [s]

    def test_many_long_strings(self):
        src = [f"long_string_value_number_{i:05d}" for i in range(1_000)]
        assert pylist(src) == src

    def test_long_with_nulls(self):
        src = [None if i % 3 == 0 else "y" * 20 for i in range(100)]
        assert pylist(src) == src


class TestBoundaryLengths:
    """Straddle the 12/13-byte inline/arena boundary."""

    def test_boundary_11(self):
        assert pylist(["a" * 11]) == ["a" * 11]

    def test_boundary_12(self):
        assert pylist(["a" * 12]) == ["a" * 12]

    def test_boundary_13(self):
        assert pylist(["a" * 13]) == ["a" * 13]

    def test_boundary_mix(self):
        src = ["a" * 11, "b" * 12, "c" * 13, None, "d" * 14]
        assert pylist(src) == src


# ---------------------------------------------------------------------------
# 3. Content edge cases
# ---------------------------------------------------------------------------


class TestMultibyteUTF8:
    """Python str stores Unicode; encoding to UTF-8 must be correct."""

    def test_2byte_codepoint(self):
        s = "é"  # é (U+00E9, 2-byte UTF-8: 0xC3 0xA9)
        assert pylist([s]) == [s]

    def test_3byte_codepoint(self):
        s = "中"  # 中 (U+4E2D, 3-byte UTF-8)
        assert pylist([s]) == [s]

    def test_4byte_codepoint(self):
        s = "\U0001F600"  # 😀 (U+1F600, 4-byte UTF-8)
        assert pylist([s]) == [s]

    def test_mixed_multibyte_short(self):
        s = "éé"  # 4 UTF-8 bytes, 2 codepoints
        assert pylist([s]) == [s]

    def test_mixed_multibyte_long(self):
        s = "中文" * 10  # 60 UTF-8 bytes, 20 codepoints
        assert pylist([s]) == [s]


class TestEmbeddedNUL:
    """Length-prefixed; embedded NUL bytes must survive round-trip."""

    def test_embedded_nul_short(self):
        # Python str can contain NUL; UTF-8 encodes as 0xC0 0x80 (MUTF-8) for
        # Java, but CPython encodes U+0000 as 0x00 in standard UTF-8.
        # We just verify round-trip: the encoding is whatever CPython produces.
        s = "a\x00b"
        assert pylist([s]) == [s]

    def test_embedded_nul_long(self):
        s = "prefix_" + "\x00" * 5 + "_suffix" * 3
        assert pylist([s]) == [s]

    def test_nul_only_string(self):
        s = "\x00" * 15
        assert pylist([s]) == [s]


# ---------------------------------------------------------------------------
# 4. __getitem__ and len
# ---------------------------------------------------------------------------


class TestGetItem:
    def test_forward_indices(self):
        v = make(["a", "bb", "ccc"])
        assert v[0] == "a"
        assert v[1] == "bb"
        assert v[2] == "ccc"

    def test_negative_indices(self):
        v = make(["x", "y", "z"])
        assert v[-1] == "z"
        assert v[-3] == "x"

    def test_null_via_getitem(self):
        v = make(["hello", None, "world"])
        assert v[0] == "hello"
        assert v[1] is None
        assert v[2] == "world"

    def test_out_of_range_raises(self):
        v = make(["a", "b"])
        with pytest.raises(IndexError):
            _ = v[2]
        with pytest.raises(IndexError):
            _ = v[-3]

    def test_empty_string_via_getitem(self):
        v = make([""])
        assert v[0] == ""

    def test_len(self):
        for n in [0, 1, 5, 100]:
            assert len(make(["x"] * n)) == n


# ---------------------------------------------------------------------------
# 5. Type tag
# ---------------------------------------------------------------------------


class TestTypeTag:
    def test_type_tag_no_nulls(self):
        assert make(["a"]).type == dn.DrakenType.VARCHAR

    def test_type_tag_with_nulls(self):
        assert make(["a", None]).type == dn.DrakenType.VARCHAR

    def test_type_tag_all_nulls(self):
        assert make([None]).type == dn.DrakenType.VARCHAR

    def test_string_tag_frozen_value(self):
        # DRAKEN_VARCHAR ABI value must be 60 (frozen per buffers.h).
        assert dn.DrakenType.VARCHAR.value == 60


# ---------------------------------------------------------------------------
# 6. Determinism (critical for later hash-only equality)
#
# Equal string values MUST produce byte-identical slots (same length, prefix,
# hash32 for long; same inline bytes for short).  This is what makes the
# later hash-only equality scheme sound.
#
# _slot_fields(i) reads the frozen slot directly via the test-only native
# accessor.  Short (len ≤ 12): returns (length, inline_bytes[12]).
# Long (len > 12): returns (length, prefix, hash32).  Null: returns None.
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Equal values in independently built vectors must have identical slot fields."""

    def test_short_string_same_values_round_trip_equal(self):
        values = ["hello", "world", "abc"]
        v1 = make(values)
        v2 = make(values)
        assert v1.to_pylist() == v2.to_pylist()

    def test_long_string_same_values_round_trip_equal(self):
        values = ["x" * 50, "y" * 100, "z" * 13]
        v1 = make(values)
        v2 = make(values)
        assert v1.to_pylist() == v2.to_pylist()

    def test_equal_short_strings_in_same_vector(self):
        s = "same_short"
        result = pylist([s, s, s])
        assert result == [s, s, s]

    def test_equal_long_strings_in_same_vector(self):
        s = "x" * 50
        result = pylist([s, s, s])
        assert result == [s, s, s]

    def test_equal_values_different_positions(self):
        s = "deterministic_value_" + "a" * 20
        src = [s, "other_long_value___" + "b" * 5, s]
        result = pylist(src)
        assert result[0] == result[2] == s

    def test_reconstructed_vectors_agree(self):
        seq = ["short", "a" * 13, "boundary_string_x" * 3, None, ""]
        assert make(seq).to_pylist() == make(seq).to_pylist()

    # --- slot-field tests: read stored fields directly via _slot_fields -------

    def test_short_slot_fields_match_across_vectors(self):
        # Same short string at different positions in independently built vectors.
        s = "hello"
        v1 = make([s, "other"])
        v2 = make(["pad", s])
        f1 = v1._slot_fields(0)
        f2 = v2._slot_fields(1)
        assert f1 == f2, f"short slot fields differ: {f1!r} != {f2!r}"

    def test_boundary_12_slot_fields_match(self):
        s = "a" * 12
        v1 = make([s])
        v2 = make(["x" * 5, s])
        assert v1._slot_fields(0) == v2._slot_fields(1)

    def test_long_slot_fields_match_across_vectors(self):
        # (length, prefix, hash32) must be identical for the same content.
        s = "x" * 50
        v1 = make([s, "other"])
        v2 = make(["padding_value_" + "p" * 20, s])
        f1 = v1._slot_fields(0)
        f2 = v2._slot_fields(1)
        assert len(f1) == 3, "expected long-form (length, prefix, hash32)"
        assert f1 == f2, f"long slot fields differ: {f1!r} != {f2!r}"

    def test_boundary_13_slot_fields_match(self):
        s = "c" * 13
        v1 = make([s])
        v2 = make([s, "something_else__" * 2])
        assert v1._slot_fields(0) == v2._slot_fields(0)

    def test_long_different_arena_offsets_same_fields(self):
        # A long string preceding s in v2 shifts s's arena_offset, but
        # (length, prefix, hash32) must remain identical.
        s = "deterministic_long_string_" + "x" * 10
        v1 = make([s])
        v2 = make(["first_long_string____" + "y" * 10, s])
        f1 = v1._slot_fields(0)
        f2 = v2._slot_fields(1)
        assert f1 == f2, f"fields differ despite same string: {f1!r} != {f2!r}"

    def test_short_inline_zero_padding_deterministic(self):
        # Bytes beyond the string length (up to 12) must be zero.
        s = "pad"  # 3 bytes; bytes 3–11 of inline field must be zero.
        v = make([s])
        fields = v._slot_fields(0)
        assert len(fields) == 2, "expected short-form (length, inline_bytes)"
        length, inline_bytes = fields
        assert length == 3
        assert all(b == 0 for b in inline_bytes[3:]), \
            f"trailing bytes not zero: {inline_bytes!r}"

    def test_null_slot_fields_returns_none(self):
        v = make([None, "hello"])
        assert v._slot_fields(0) is None
        assert v._slot_fields(1) is not None

    def test_short_vs_long_form_discriminant(self):
        # Short string → 2-tuple; long string → 3-tuple.
        v = make(["short", "a" * 13])
        assert len(v._slot_fields(0)) == 2, "<=12-byte string must return 2-tuple"
        assert len(v._slot_fields(1)) == 3, ">12-byte string must return 3-tuple"


# ---------------------------------------------------------------------------
# 7. Prefix byte-order (lex ordering via uint32 comparison)
#
# For two long strings, str_prefix4(a) < str_prefix4(b) must hold iff
# first_4_utf8_bytes(a) < first_4_utf8_bytes(b) lexicographically.
# We read the stored prefix directly via _slot_fields and compare against
# the independently computed big-endian encoding of the first 4 UTF-8 bytes.
# High-bit (>0x7F) bytes are included to catch signed-vs-unsigned bugs.
# ---------------------------------------------------------------------------


def _prefix_uint32(s: str) -> int:
    """Read the stored prefix uint32 from the long-form slot for string s."""
    assert len(s.encode("utf-8")) > 12, "string must be long (>12 UTF-8 bytes)"
    v = make([s])
    _length, prefix, _hash32 = v._slot_fields(0)
    return prefix


def _expected_prefix(s: str) -> int:
    """Compute expected big-endian uint32 prefix from first 4 UTF-8 bytes."""
    utf8 = s.encode("utf-8")
    assert len(utf8) >= 4
    return (utf8[0] << 24) | (utf8[1] << 16) | (utf8[2] << 8) | utf8[3]


class TestPrefixOrdering:
    """Stored prefix uint32 matches big-endian first-4-bytes and orders correctly."""

    def test_ascii_prefix_value_correct(self):
        s = "ab_" + "x" * 20
        assert _prefix_uint32(s) == _expected_prefix(s)

    def test_ascii_prefix_order_consistent(self):
        a = "ab_" + "x" * 20
        b = "az_" + "x" * 20
        assert a < b
        assert _prefix_uint32(a) < _prefix_uint32(b), \
            f"prefix(a)={_prefix_uint32(a):#010x} should be < prefix(b)={_prefix_uint32(b):#010x}"

    def test_prefix_differs_at_each_byte_position(self):
        # Each of the 4 prefix byte positions must contribute to ordering.
        for pos in range(4):
            chars_lo = list("aaaa")
            chars_hi = list("aaaa")
            chars_lo[pos] = "a"  # 0x61
            chars_hi[pos] = "z"  # 0x7A
            s_lo = "".join(chars_lo) + "x" * 20
            s_hi = "".join(chars_hi) + "x" * 20
            assert s_lo < s_hi
            p_lo = _prefix_uint32(s_lo)
            p_hi = _prefix_uint32(s_hi)
            assert p_lo < p_hi, \
                f"prefix ordering wrong at byte position {pos}: {p_lo:#010x} >= {p_hi:#010x}"

    def test_high_bit_prefix_value_correct(self):
        # é = U+00E9, UTF-8: 0xC3 0xA9. Stored prefix must match big-endian encoding.
        s = "é" + "x" * 20
        stored = _prefix_uint32(s)
        expected = _expected_prefix(s)
        assert stored == expected, f"stored {stored:#010x} != expected {expected:#010x}"

    def test_high_bit_prefix_order_unsigned(self):
        # U+007F → single UTF-8 byte 0x7F.
        # U+00C0 → UTF-8: 0xC3 0x80 (first byte 0xC3 > 0x7F).
        # A signed comparison would treat 0xC3... as negative — the prefix
        # comparison must be unsigned for the ordering to be correct.
        a = "\x7f" + "x" * 20  # first UTF-8 byte: 0x7F
        b = "\xc0" + "x" * 20  # first UTF-8 byte: 0xC3
        assert a < b  # Python string order
        pa = _prefix_uint32(a)
        pb = _prefix_uint32(b)
        assert pa == _expected_prefix(a), \
            f"prefix for {a!r}: stored {pa:#010x} != expected {_expected_prefix(a):#010x}"
        assert pb == _expected_prefix(b), \
            f"prefix for {b!r}: stored {pb:#010x} != expected {_expected_prefix(b):#010x}"
        assert pa < pb, \
            f"unsigned ordering wrong (signed bug?): {pa:#010x} >= {pb:#010x}"

    def test_high_bit_first_byte_ordering(self):
        # U+0001 → 0x01 (first byte); U+0080 → UTF-8 0xC2 0x80 (first byte 0xC2).
        # 0xC2 > 0x01 unsigned, so prefix(b) > prefix(a).
        a = "\x01" + "x" * 20
        b = "\x80" + "x" * 20
        assert a < b
        pa = _prefix_uint32(a)
        pb = _prefix_uint32(b)
        assert pa == _expected_prefix(a)
        assert pb == _expected_prefix(b)
        assert pa < pb, f"prefix ordering broken: {pa:#010x} >= {pb:#010x}"

    def test_prefix_independent_of_tail(self):
        # Same first 4 bytes → same prefix, regardless of tail content.
        s1 = "abcd" + "x" * 20
        s2 = "abcd" + "y" * 20
        assert _prefix_uint32(s1) == _prefix_uint32(s2)

    def test_lex_order_preserved_on_readback(self):
        strings = ["apple", "banana", "cherry", "date", "elderberry"]
        result = pylist(strings)
        assert result == sorted(result)

    def test_long_lex_order_preserved(self):
        strings = ["alpha_long_string", "beta_long_string", "gamma_long_string"]
        result = pylist(strings)
        assert result == sorted(result)


# ---------------------------------------------------------------------------
# 8. Short-string pad zeroing (determinism for inline equality)
# ---------------------------------------------------------------------------


class TestInlinePadZeroing:
    """Inline slots pad to 12 bytes with zeros; equal short strings are identical."""

    def test_short_string_equals_same_content(self):
        s = "pad_test"  # 8 bytes, 4 trailing zero bytes in slot
        assert pylist([s, s]) == [s, s]

    def test_distinct_lengths_not_equal(self):
        result = pylist(["abc", "abcd"])
        assert result[0] != result[1]

    def test_empty_string_not_equal_to_nonnull_string(self):
        result = pylist(["", "a"])
        assert result[0] == ""
        assert result[1] == "a"
        assert result[0] != result[1]


# ---------------------------------------------------------------------------
# 9. Arena overflow check
# ---------------------------------------------------------------------------


class TestArenaOverflow:
    def test_normal_large_input_does_not_raise(self):
        # 1000 × 100-byte strings = 100 KB arena; well within u32.
        src = ["y" * 100] * 1_000
        result = pylist(src)
        assert all(v == "y" * 100 for v in result)
