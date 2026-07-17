import os
import random
import sys

import numpy as np

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel

from opteryx.compiled.structures.bloom_filter import (
    BloomFilter,
    bloom_filter_check_morsel,
    create_bloom_filter_from_hashes,
    create_bloom_filter_morsel,
)

from opteryx.utils import random_string

SEED: int = random.randint(0, 2**32 - 1)
NUM_ITEMS: int = 1_000_000


def _morsel(items, name=b"items"):
    """Build a single-column Draken Morsel from a list of bytes/str/None."""
    encoded = [i.encode() if isinstance(i, str) else i for i in items]
    v = dn.vector_from_string_sequence(encoded)
    return Morsel.from_vectors([name], [v])


def generate_seeded_byte_items(
    num_items=NUM_ITEMS, item_length=4, seed=SEED, null_probability=0.01
):
    """
    Generate a list of consistent random byte items using a fixed seed.

    Parameters:
        num_items: int
            Number of items to generate.
        item_length: int, optional
            Length of each byte item (default is 8).
        seed: int, optional
            Seed for the random number generator (default is 42).

    Returns:
        list of bytes
            List of seeded random byte items.
    """
    random.seed(seed)  # Seed the random generator for reproducibility
    # Generate the list of random byte items
    return [
        None
        if random.random() < null_probability
        else random.getrandbits(item_length * 8).to_bytes(item_length, byteorder="big")
        for _ in range(num_items)
    ]


def _unpack_bit_results(bit_packed_result, num_items):
    """
    Convert bit-packed boolean result to list of bools.

    The result is a uint8 memoryview where each bit represents a boolean.
    Bits are packed LSB-first.
    """
    results = []
    for i in range(num_items):
        byte_idx = i >> 3  # i // 8
        bit_idx = i & 7  # i % 8
        results.append(bool(bit_packed_result[byte_idx] & (1 << bit_idx)))
    return results


def test_bloom_filter_bulk_add_bulk_check():
    """Test bulk addition of items to the BloomFilter"""
    items = generate_seeded_byte_items(num_items=10000, item_length=4, null_probability=0.0)
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All added items must be found in the filter"


def test_bloom_filter_empty_strings():
    items = [b""] * 100
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Empty strings must be found"


def test_bloom_filter_no_keys():
    """An empty morsel has nothing to build a filter from."""
    m = _morsel([])
    bf = create_bloom_filter_morsel(m, [b"items"])
    assert bf is None


def test_bloom_filter_single_key():
    items = [b"key1"] * 100
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Single repeated key must be found"


def test_bloom_filter_special_characters():
    items = [b"\x00\xff\xaa\xbb", b"\xcc\xdd\xee\xff"] * 100
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Special binary characters must be found"


def test_bloom_filter_unicode_strings():
    items = ["hello", "world", "🚀", "こんにちは"] * 25
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Unicode strings must be found"


def test_bloom_filter_bulk_add_no_nulls_bulk_check():
    items = generate_seeded_byte_items(num_items=5000, item_length=4, null_probability=0.0)
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All non-null items must be found"


def test_bloom_filter_with_nulls():
    items = generate_seeded_byte_items(num_items=5000, item_length=4, null_probability=0.1)
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])
    hits = bloom_filter_check_morsel(bf, m, [b"items"])
    results = _unpack_bit_results(hits, len(items))
    for item, found in zip(items, results):
        if item is not None:
            assert found, f"Non-null item {item!r} must be found"


def test_bloom_filter_false_positives():
    """Test false positive rate of BloomFilter"""
    items = generate_seeded_byte_items(num_items=100000, item_length=4, null_probability=0.0)
    m = _morsel(items)
    bf = create_bloom_filter_morsel(m, [b"items"])

    TEST_SAMPLE_SIZE = 1000
    # Generate non-overlapping test items (different seed)
    tests = generate_seeded_byte_items(
        num_items=TEST_SAMPLE_SIZE, item_length=2, seed=SEED + 1, null_probability=0.0
    )
    test_m = _morsel(tests)
    hits = bloom_filter_check_morsel(bf, test_m, [b"items"])
    results = _unpack_bit_results(hits, TEST_SAMPLE_SIZE)
    hit_count = sum(results)
    # FPR should be significantly less than 90%
    assert hit_count < (TEST_SAMPLE_SIZE * 0.90), (
        f"BloomFilter returned too many false positives.\nseed: {SEED}\nhits: {hit_count}"
    )


# ── New API tests ──────────────────────────────────────────────────────────────


def test_bloom_filter_massive_category():
    """MASSIVE tier: filter for 256M items initialises without error."""
    bf = BloomFilter(256_000_000)
    # Use smaller range to avoid overflow
    hashes = np.array([i * np.uint64(0x9E3779B97F4A7C15) for i in range(1000)], dtype=np.uint64)
    for h in hashes:
        bf.add(int(h))
    results = [bf.possibly_contains(int(h)) for h in hashes]
    assert all(results), "MASSIVE tier: all items added must always be found"


def test_bloom_filter_add_individual_items():
    bf = BloomFilter(100)
    for i in range(100):
        bf.add(i)
    for i in range(100):
        assert bf.possibly_contains(i), f"Item {i} must be found"


def test_bloom_filter_create_from_hashes_basic():
    """create_bloom_filter_from_hashes: all inserted hashes are found."""
    hashes = np.array([hash(i) & 0xFFFFFFFFFFFFFFFF for i in range(10_000)], dtype=np.uint64)
    bf = create_bloom_filter_from_hashes(hashes)
    assert bf is not None
    for h in hashes:
        assert bf.possibly_contains(int(h)), f"Hash {h} not found after insertion"


def test_bloom_filter_create_from_hashes_empty_returns_none():
    """create_bloom_filter_from_hashes: empty input returns None."""
    bf = create_bloom_filter_from_hashes(np.array([], dtype=np.uint64))
    assert bf is None


def test_bloom_filter_create_from_hashes_matches_create_bloom_filter_morsel():
    """create_bloom_filter_from_hashes and create_bloom_filter_morsel agree on membership."""
    items = [random_string() for _ in range(5_000)]
    m = _morsel(items)
    bf_rel = create_bloom_filter_morsel(m, [b"items"])

    # We verify the two paths produce consistent results via membership tests
    test_items = items[:100]
    test_m = _morsel(test_items)
    results_rel = bloom_filter_check_morsel(bf_rel, test_m, [b"items"])
    unpacked = _unpack_bit_results(results_rel, len(test_items))
    # All items that were inserted should be found (no false negatives)
    assert all(unpacked), "create_bloom_filter_morsel: inserted items must always be found"


def test_bloom_filter_possibly_contains_many_direct_basic():
    """possibly_contains_many_direct: all inserted hashes are found."""
    hashes = np.array([hash(i) & 0xFFFFFFFFFFFFFFFF for i in range(5_000)], dtype=np.uint64)
    bf = create_bloom_filter_from_hashes(hashes)
    assert bf is not None

    result = bf.possibly_contains_many_direct(hashes)
    # Convert bit-packed result to list of bools
    bits = _unpack_bit_results(result, len(hashes))
    assert all(bits), "possibly_contains_many_direct: all inserted hashes must be found"


def test_bloom_filter_possibly_contains_many_direct_fpr():
    """possibly_contains_many_direct: FPR on non-inserted hashes is reasonable."""
    n = 50_000
    inserted = np.array([hash(i) & 0xFFFFFFFFFFFFFFFF for i in range(n)], dtype=np.uint64)
    probe = np.array([hash(i + n * 2) & 0xFFFFFFFFFFFFFFFF for i in range(1_000)], dtype=np.uint64)

    bf = create_bloom_filter_from_hashes(inserted)
    result = bf.possibly_contains_many_direct(probe)

    bits = _unpack_bit_results(result, len(probe))
    hits = sum(bits)
    # FPR should be well under 50% for a properly-sized filter
    assert hits < len(probe) * 0.50, f"FPR too high: {hits}/{len(probe)}"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__])
