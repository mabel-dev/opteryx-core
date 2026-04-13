# tests/unit/operators/test_outer_join_bloom_fastpath.py
# Unit tests for bloom-filter fast-path used by outer-join operator
# - Verifies Draken-native bloom filter creation from Morsels
# - Verifies Draken-native bloom filter probe returns a bit-packed memoryview
#   which can be unpacked to booleans indicating possible-membership.
#
# These tests exercise the fast-path APIs:
#   - create_bloom_filter_morsel(Morsel, list[str]) -> BloomFilter | None
#   - bloom_filter_check_morsel(BloomFilter, Morsel, list[str]) -> uint8_t[::1] | None
#
# The implementation is tolerant of false positives (bloom property), but for
# small deterministic inputs we expect exact membership for present keys.

import pyarrow as pa
import pytest
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.structures.bloom_filter import (
    bloom_filter_check_morsel,
    create_bloom_filter_morsel,
)


def _unpack_bit_results(bit_mv, n):
    """
    Unpack a bit-packed uint8_t[::1] memoryview (LSB-first) into n booleans.

    bit_mv supports buffer protocol or indexing; we treat it like a sequence of bytes.
    """
    if bit_mv is None:
        return [False] * n
    # Some Cython-returned memoryviews implement len() and indexing
    out = []
    for i in range(n):
        byte = bit_mv[i // 8]
        bit = (byte >> (i % 8)) & 1
        out.append(bool(bit))
    return out


def _morsel_from_pylist(col_name, values):
    """
    Helper: build a pyarrow table from values and convert to a Draken Morsel.
    """
    arr = pa.array(values)
    table = pa.table({col_name: arr})
    m = Morsel.from_arrow(table)
    return m


def test_create_and_check_bloom_morsel_basic():
    # Left: keys present in build side
    left_keys = [1, 2, 3, 1000]
    # Right: mixture of present and absent keys
    right_keys = [2, 3, 4, 1000, 5]

    left_morsel = _morsel_from_pylist("k", left_keys)
    bf = create_bloom_filter_morsel(left_morsel, ["k"])

    # Bloom filter should be created for non-empty morsel
    assert bf is not None

    right_morsel = _morsel_from_pylist("k", right_keys)
    bit_results = bloom_filter_check_morsel(bf, right_morsel, ["k"])
    assert bit_results is not None

    unpacked = _unpack_bit_results(bit_results, right_morsel.num_rows)
    expected = [k in set(left_keys) for k in right_keys]

    # For small deterministic inputs, we expect membership bits to match exactly.
    # Bloom filters can produce false positives, but absent values should usually be false here.
    assert len(unpacked) == len(expected)
    for got, exp in zip(unpacked, expected):
        assert got == exp


def test_create_bloom_morsel_empty_and_probe_empty():
    # Empty left morsel -> create_bloom_filter_morsel should return None
    empty_left = _morsel_from_pylist("k", [])
    bf = create_bloom_filter_morsel(empty_left, ["k"])
    assert bf is None

    # If bloom filter is None, probe should also return None
    right_morsel = _morsel_from_pylist("k", [1, 2, 3])
    bit_results = bloom_filter_check_morsel(bf, right_morsel, ["k"])
    assert bit_results is None

    # If probe morsel is empty, bloom_filter_check_morsel should return None even if BF exists
    left_morsel = _morsel_from_pylist("k", [1, 2, 3])
    bf2 = create_bloom_filter_morsel(left_morsel, ["k"])
    assert bf2 is not None
    empty_probe = _morsel_from_pylist("k", [])
    bit_results2 = bloom_filter_check_morsel(bf2, empty_probe, ["k"])
    assert bit_results2 is None
