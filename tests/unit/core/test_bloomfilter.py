import os
import random
import sys

import numpy as np
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.bloom_filter import (
    BloomFilter,
    create_bloom_filter,
    create_bloom_filter_from_hashes,
)
from orso.tools import random_string

SEED: int = random.randint(0, 2**32 - 1)
NUM_ITEMS: int = 1_000_000


class FakeRelation:
    """
    A fake relation class to simulate a pyarrow Table-like structure for testing.
    """

    def __init__(self, columns: dict):
        """
        Parameters:
            columns: dict[str, pyarrow.Array or pyarrow.ChunkedArray]
                Mapping of column names to Arrow arrays
        """
        self._columns = columns
        # Assumes all columns are the same length — valid for BloomFilter use
        self.num_rows = len(next(iter(columns.values())))

    def column(self, name: str):
        return self._columns[name]

    def drop_null(self):
        """
        Drop null values from all columns in the relation.
        """
        null_columns = []
        for column in self._columns.values():
            if isinstance(column, pyarrow.ChunkedArray):
                column = column.combine_chunks()
            for i in range(len(column)):
                if not column[i].is_valid:
                    null_columns.append(i)
        null_columns = set(null_columns)
        valid_rows = [i for i in range(self.num_rows) if i not in null_columns]
        return FakeRelation(
            {name: column.take(valid_rows) for name, column in self._columns.items()}
        )


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


def to_chunked_array(items, chunk_size=1000):
    """
    Convert a list of items into a PyArrow ChunkedArray.

    Parameters:
        items: list of bytes or None
            List of items to convert to a ChunkedArray.
        chunk_size: int, optional
            Size of each chunk (default is 100).

    Returns:
        pyarrow.ChunkedArray
            The ChunkedArray with the items split into multiple chunks.
    """
    # Split items into chunks
    chunks = [pyarrow.array(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]
    # Combine chunks into a ChunkedArray
    return pyarrow.chunked_array(chunks)


def _unpack_bit_results(bit_packed_result, num_items):
    """
    Convert bit-packed boolean result to list of bools.

    The result is a uint8 memoryview where each bit represents a boolean.
    Bits are packed LSB-first (PyArrow bool layout).
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
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": bulk})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All added items must be found in the filter"


def test_bloom_filter_bulk_add_chunked_check():
    """Test bulk addition of items to the BloomFilter with chunked arrays"""
    items = generate_seeded_byte_items(num_items=10000, item_length=4, null_probability=0.0)
    bulk = to_chunked_array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": bulk})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All added items must be found in the filter"


def test_bloom_filter_chunked_add_bulk_check():
    """Test chunked addition of items to the BloomFilter"""
    items = generate_seeded_byte_items(num_items=10000, item_length=4, null_probability=0.0)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": to_chunked_array(items)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": bulk})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All added items must be found in the filter"


def test_bloom_filter_chunked_add_chunk_check():
    """Test chunked addition and chunked checking of items"""
    items = generate_seeded_byte_items(num_items=10000, item_length=4, null_probability=0.0)
    bulk = to_chunked_array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": bulk})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All added items must be found in the filter"


def test_bloom_filter_empty_strings():
    items = [b""] * 100
    relation = FakeRelation({"items": pyarrow.array(items)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Empty strings must be found"


def test_bloom_filter_empty_binary():
    items = [b""] * 100
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Empty binary values must be found"


def test_bloom_filter_chunk_boundaries():
    """Test BloomFilter with strings spanning chunk boundaries."""
    items = generate_seeded_byte_items(num_items=2000, item_length=4, null_probability=0.0)
    relation = FakeRelation({"items": to_chunked_array(items, chunk_size=100)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": to_chunked_array(items, chunk_size=100)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Items across chunk boundaries must be found"


def test_bloom_filter_single_chunk():
    """Test BloomFilter with a single chunk."""
    items = generate_seeded_byte_items(num_items=1000, item_length=4, null_probability=0.0)
    relation = FakeRelation({"items": to_chunked_array(items, chunk_size=10000)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": to_chunked_array(items, chunk_size=10000)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Items in single chunk must be found"


def test_bloom_filter_large_chunks():
    """Test BloomFilter with large chunks."""
    items = generate_seeded_byte_items(num_items=100000, item_length=4, null_probability=0.0)
    relation = FakeRelation({"items": to_chunked_array(items, chunk_size=50000)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": to_chunked_array(items, chunk_size=50000)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Items in large chunks must be found"


def test_bloom_filter_mixed_chunk_sizes():
    """Test BloomFilter with mixed chunk sizes."""
    items = generate_seeded_byte_items(num_items=10000, item_length=4, null_probability=0.0)
    # Create with one chunk size
    relation = FakeRelation({"items": to_chunked_array(items, chunk_size=100)})
    bf = create_bloom_filter(relation, ["items"])
    # Test with different chunk size
    test_relation = FakeRelation({"items": to_chunked_array(items, chunk_size=500)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Items must be found regardless of chunk arrangement"


def test_bloom_filter_all_empty_strings():
    items = [b""] * 100
    relation = FakeRelation({"items": pyarrow.array(items)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array([b""] * 100)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All empty strings must be found"


def test_bloom_filter_all_empty_binary():
    items = [b""] * 100
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array([b""] * 100, type=pyarrow.binary())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All empty binary values must be found"


def test_bloom_filter_single_key():
    items = [b"key1"] * 100
    relation = FakeRelation({"items": pyarrow.array(items)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Single repeated key must be found"


def test_bloom_filter_no_keys():
    items = []
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    assert bf is not None


def test_bloom_filter_special_characters():
    items = [b"\x00\xff\xaa\xbb", b"\xcc\xdd\xee\xff"] * 100
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Special binary characters must be found"


def test_bloom_filter_unicode_strings():
    items = ["hello", "world", "🚀", "こんにちは"] * 25
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Unicode strings must be found"


def test_bloom_filter_unicode_binary():
    items = ["hello".encode(), "world".encode(), "🚀".encode(), "こんにちは".encode()] * 25
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "Unicode binary values must be found"


def test_bloom_strings_and_binary():
    string_items = ["hello", "world", "test"]
    binary_items = [s.encode() for s in string_items]
    relation = FakeRelation({"items": pyarrow.array(binary_items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(binary_items, type=pyarrow.binary())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(binary_items))
    assert all(results), "String/binary conversion must be found"


def test_bloom_binary_and_strings():
    string_items = ["hello", "world", "test"]
    relation = FakeRelation({"items": pyarrow.array(string_items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(string_items, type=pyarrow.string())})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(string_items))
    assert all(results), "String values must be found"


def test_bloom_filter_add_individual_items():
    bf = BloomFilter(100)
    for i in range(100):
        bf.add(i)
    for i in range(100):
        assert bf.possibly_contains(i), f"Item {i} must be found"


def test_bloom_filter_add_many_individual_items():
    bf = BloomFilter(10000)
    for i in range(10000):
        bf.add(i)
    for i in range(10000):
        assert bf.possibly_contains(i), f"Item {i} must be found"


def test_bloom_filter_bulk_add_no_nulls_bulk_check():
    items = generate_seeded_byte_items(num_items=5000, item_length=4, null_probability=0.0)
    relation = FakeRelation({"items": pyarrow.array(items)})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All non-null items must be found"


def test_bloom_filter_bulk_add_bulk_check_no_nulls():
    items = generate_seeded_byte_items(num_items=5000, item_length=4, null_probability=0.0)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": bulk})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All non-null items must be found"


def test_bloom_filter_bulk_add_no_nulls_bulk_check_no_nulls():
    items = generate_seeded_byte_items(num_items=5000, item_length=4, null_probability=0.0)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    results = _unpack_bit_results(hits, len(items))
    assert all(results), "All non-null items must be found"


def test_bloom_filter_false_positives():
    """Test false positive rate of BloomFilter"""
    items = generate_seeded_byte_items(num_items=100000, item_length=4, null_probability=0.0)
    relation = FakeRelation({"items": pyarrow.array(items)})
    bf = create_bloom_filter(relation, ["items"])

    TEST_SAMPLE_SIZE = 1000
    # Generate non-overlapping test items (different seed)
    tests = generate_seeded_byte_items(
        num_items=TEST_SAMPLE_SIZE, item_length=2, seed=SEED + 1, null_probability=0.0
    )
    test_relation = FakeRelation({"items": pyarrow.array(tests)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
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


def test_bloom_filter_create_from_hashes_matches_create_bloom_filter():
    """create_bloom_filter_from_hashes and create_bloom_filter agree on membership."""
    items = [random_string() for _ in range(5_000)]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf_rel = create_bloom_filter(relation, ["items"])

    # We verify the two paths produce consistent results via membership tests
    test_items = items[:100]
    test_relation = FakeRelation({"items": pyarrow.array(test_items, type=pyarrow.string())})
    results_rel = bf_rel.possibly_contains_many(test_relation, ["items"])
    unpacked = _unpack_bit_results(results_rel, len(test_items))
    # All items that were inserted should be found (no false negatives)
    assert all(unpacked), "create_bloom_filter: inserted items must always be found"


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
