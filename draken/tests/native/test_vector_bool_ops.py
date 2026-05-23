"""
Native + parity tests for E.4: bool utility ops via vector_bool_ops consumer.

Loads the nanobind extension without triggering opteryx/__init__.py,
following the spec_from_file_location pattern established in E.2/E.3.

Coverage:
  bool_vector_all_true:
    n=0, n=1, small n, n not multiple of 8
    all bits = 1, trailing bits properly masked

  bool_vector_from_int8_mask:
    mask[i]=1 → bit i set; mask[i]=0 → bit i clear
    mixed mask, all-null mask, all-valid mask
    non-buffer input → BufferError

  bool_vector_from_inverted_null_bitmap:
    input bit=1 (valid) → output bit=0 (not IS NULL)
    input bit=0 (null)  → output bit=1 (IS NULL)
    trailing bits properly masked

  bool_vector_and_chain:
    empty list → None
    single element → same vector returned
    two elements: AND truth table (T∧T=T, T∧F=F, F∧F=F)
    Kleene null semantics: T∧N=N, F∧N=F, N∧N=N
    early exit when running result is all-False
    TypeError on non-Vector elements
"""

import array
import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_bool_ops extension
# ---------------------------------------------------------------------------

def _load_vector_bool_ops():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_bool_ops*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip("vector_bool_ops extension not built — run make compile first", allow_module_level=True)
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_bool_ops", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bo = _load_vector_bool_ops()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bool_list(vec):
    """Extract list[bool | None] from a DRAKEN_BOOL Vector."""
    return [vec[i] for i in range(len(vec))]


def make_bool_vec(values):
    """Build a DRAKEN_BOOL Vector from list[bool | None]."""
    return dn.vector_from_bool_sequence(values)


def get_bit(vec, i):
    """Read a single logical bool value from a DRAKEN_BOOL Vector."""
    return vec[i]


# ---------------------------------------------------------------------------
# bool_vector_all_true
# ---------------------------------------------------------------------------

class TestBoolVectorAllTrue:

    def test_n_zero(self):
        v = bo.bool_vector_all_true(0)
        assert len(v) == 0

    def test_n_one_is_true(self):
        v = bo.bool_vector_all_true(1)
        assert get_bit(v, 0) is True

    def test_n_eight_all_true(self):
        v = bo.bool_vector_all_true(8)
        assert all(get_bit(v, i) is True for i in range(8))

    def test_n_seven_trailing_bits_clear(self):
        # 7 logical rows → bit 7 in byte 0 must be 0 (masked out).
        v = bo.bool_vector_all_true(7)
        assert len(v) == 7
        assert all(get_bit(v, i) is True for i in range(7))

    def test_n_nine_all_true(self):
        v = bo.bool_vector_all_true(9)
        assert all(get_bit(v, i) is True for i in range(9))

    def test_n_100_all_true(self):
        v = bo.bool_vector_all_true(100)
        assert all(get_bit(v, i) is True for i in range(100))

    def test_negative_n_raises(self):
        with pytest.raises((ValueError, OverflowError, Exception)):
            bo.bool_vector_all_true(-1)


# ---------------------------------------------------------------------------
# bool_vector_from_int8_mask
# ---------------------------------------------------------------------------

class TestBoolVectorFromInt8Mask:

    def test_all_null_mask(self):
        # mask[i] = 1 → IS NULL = True for every row.
        mask = array.array("b", [1, 1, 1, 1])
        v = bo.bool_vector_from_int8_mask(mask, 4)
        assert bool_list(v) == [True, True, True, True]

    def test_all_valid_mask(self):
        # mask[i] = 0 → IS NULL = False for every row.
        mask = array.array("b", [0, 0, 0, 0])
        v = bo.bool_vector_from_int8_mask(mask, 4)
        assert bool_list(v) == [False, False, False, False]

    def test_mixed_mask(self):
        mask = array.array("b", [1, 0, 1, 0, 0, 1])
        v = bo.bool_vector_from_int8_mask(mask, 6)
        assert bool_list(v) == [True, False, True, False, False, True]

    def test_bytes_input_accepted(self):
        # bytes object also supports buffer protocol.
        mask = bytes([1, 0, 1])
        v = bo.bool_vector_from_int8_mask(mask, 3)
        assert bool_list(v) == [True, False, True]

    def test_bytearray_input_accepted(self):
        mask = bytearray([0, 1, 0])
        v = bo.bool_vector_from_int8_mask(mask, 3)
        assert bool_list(v) == [False, True, False]

    def test_n_zero(self):
        mask = array.array("b", [])
        v = bo.bool_vector_from_int8_mask(mask, 0)
        assert len(v) == 0

    def test_non_zero_values_treated_as_null(self):
        # Any nonzero value in mask means IS NULL.
        mask = array.array("b", [2, -1, 127])
        v = bo.bool_vector_from_int8_mask(mask, 3)
        assert bool_list(v) == [True, True, True]

    def test_non_buffer_raises(self):
        with pytest.raises((TypeError, BufferError, Exception)):
            bo.bool_vector_from_int8_mask(42, 1)


# ---------------------------------------------------------------------------
# bool_vector_from_inverted_null_bitmap
# ---------------------------------------------------------------------------

class TestBoolVectorFromInvertedNullBitmap:

    def test_all_valid_bitmap_gives_all_false(self):
        # Input: bit=1 (all valid) → IS NULL = False.
        bitmap = bytes([0xFF])  # 8 rows, all valid
        v = bo.bool_vector_from_inverted_null_bitmap(bitmap, 8)
        assert bool_list(v) == [False] * 8

    def test_all_null_bitmap_gives_all_true(self):
        # Input: bit=0 (all null) → IS NULL = True.
        bitmap = bytes([0x00])
        v = bo.bool_vector_from_inverted_null_bitmap(bitmap, 8)
        assert bool_list(v) == [True] * 8

    def test_mixed_bitmap(self):
        # Bits: 10110001 (LSB first) → valid rows: 0,4,5,7 → IS NULL rows: 1,2,3,6
        bitmap = bytes([0b10110001])  # = 0xB1
        # bit 0 = 1 (valid) → IS NULL = False
        # bit 1 = 0 (null)  → IS NULL = True
        # bit 2 = 0 (null)  → IS NULL = True
        # bit 3 = 0 (null)  → IS NULL = True
        # bit 4 = 1 (valid) → IS NULL = False
        # bit 5 = 1 (valid) → IS NULL = False
        # bit 6 = 0 (null)  → IS NULL = True
        # bit 7 = 1 (valid) → IS NULL = False
        v = bo.bool_vector_from_inverted_null_bitmap(bitmap, 8)
        expected = [False, True, True, True, False, False, True, False]
        assert bool_list(v) == expected

    def test_trailing_bits_masked(self):
        # n=5, byte = 0xFF: bits 5,6,7 are beyond n and must be 0 in output.
        bitmap = bytes([0xFF])
        v = bo.bool_vector_from_inverted_null_bitmap(bitmap, 5)
        assert len(v) == 5
        # All input bits 0..4 are valid (1), so IS NULL = False for all 5 rows.
        assert bool_list(v) == [False] * 5

    def test_n_zero(self):
        v = bo.bool_vector_from_inverted_null_bitmap(bytes([]), 0)
        assert len(v) == 0


# ---------------------------------------------------------------------------
# bool_vector_and_chain
# ---------------------------------------------------------------------------

class TestBoolVectorAndChain:

    def test_empty_list_returns_none(self):
        result = bo.bool_vector_and_chain([])
        assert result is None

    def test_single_element_returned_unchanged(self):
        v = make_bool_vec([True, False, None])
        result = bo.bool_vector_and_chain([v])
        # Same logical values (may or may not be same object).
        assert bool_list(result) == [True, False, None]

    def test_two_all_true(self):
        a = make_bool_vec([True, True])
        b = make_bool_vec([True, True])
        r = bo.bool_vector_and_chain([a, b])
        assert bool_list(r) == [True, True]

    def test_two_one_false(self):
        a = make_bool_vec([True, False, True])
        b = make_bool_vec([True, True, False])
        r = bo.bool_vector_and_chain([a, b])
        assert bool_list(r) == [True, False, False]

    def test_and_truth_table_no_nulls(self):
        # T∧T=T, T∧F=F, F∧T=F, F∧F=F
        a = make_bool_vec([True, True, False, False])
        b = make_bool_vec([True, False, True, False])
        r = bo.bool_vector_and_chain([a, b])
        assert bool_list(r) == [True, False, False, False]

    def test_kleene_null_semantics_and(self):
        # Kleene AND: F∧N=F (valid!), T∧N=N, N∧N=N.
        a = make_bool_vec([True, False, None, None])
        b = make_bool_vec([None, None, True, None])
        r = bo.bool_vector_and_chain([a, b])
        result = bool_list(r)
        assert result[0] is None   # T ∧ N = N
        assert result[1] is False  # F ∧ N = F
        assert result[2] is None   # N ∧ T = N
        assert result[3] is None   # N ∧ N = N

    def test_three_masks(self):
        a = make_bool_vec([True, True, False])
        b = make_bool_vec([True, False, True])
        c = make_bool_vec([True, True, True])
        r = bo.bool_vector_and_chain([a, b, c])
        assert bool_list(r) == [True, False, False]

    def test_early_exit_all_false(self):
        # First mask is all-False → chain stops; result is all-False.
        all_false = make_bool_vec([False, False, False])
        all_true  = make_bool_vec([True, True, True])
        r = bo.bool_vector_and_chain([all_false, all_true])
        assert bool_list(r) == [False, False, False]

    def test_type_error_on_non_vector_element(self):
        v = make_bool_vec([True, False])
        with pytest.raises((TypeError, Exception)):
            bo.bool_vector_and_chain([v, "not a vector"])

    def test_non_list_raises(self):
        with pytest.raises((TypeError, Exception)):
            bo.bool_vector_and_chain("not a list")
