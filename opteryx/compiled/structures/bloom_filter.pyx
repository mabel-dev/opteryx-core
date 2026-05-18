# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
This is not a general perpose Bloom Filter, if used outside Opteryx it may not
perform entirely as expected as it is optimized for a specific configuration
and constraints.

We have five size options, all using 2 hashes:
    - a 8k slot bit array for up to 1,000 items (about 4.9% FPR)
    - a 512k slot bit array for up to 62,000 items (about 4.2% FPR)
    - a 8m slot bit array for up to 1m items (about 4.5% FPR)
    - a 128m slot bit array for up to 16m items (about 4.7% FPR)
    - a 2b slot bit array for up to 256m items (about 0.3% FPR at 56m items)

We perform one hash and then use a calculation based on the golden ratio to
determine the second position. This is cheaper than performing two hashes whilst
still providing a good enough split of the two hashes.

The primary use for this structure is to prefilter JOINs and GROUP BY operations.
It is many times faster (about 20x from initial benchmarking) to test for
containment in the bloom filter than to look up the item in the hash table.

Building the filter is fast - for tables up to 1 million records we create the filter
(1m records is roughly a 0.005s build). If the filter isn't effective (less that 5%
eliminations) we discard it which has meant some waste work.

The 16m set takes about 0.08 seconds to build which is acceptable for speculative
use in joins. The MASSIVE (256m) tier is intended for group-by pre-filtering where
the build cost is amortised over the ingest phase.
"""

from libc.stdlib cimport calloc, free
from libc.stdint cimport uint8_t, uint64_t, uint32_t, int64_t
from cpython.array cimport array, clone

from draken.vectors.integer64_vector cimport Integer64Vector
from draken.morsels.morsel cimport Morsel
from opteryx.compiled.morsel_ops.null_filter cimport non_null_row_indices

# Reusable template arrays for zero-copy clone allocations
cdef array _UINT64_TEMPLATE = array('Q', [])
cdef array _UINT8_TEMPLATE = array('B', [])

cdef extern from "<stdint.h>":
    ctypedef unsigned long uintptr_t

# Define sizes for the Bloom filters - now in 64-bit chunks
cdef uint32_t BIT64_ARRAY_SIZE_TINY = 128  # 128 * 64 = 8,192 bits
cdef uint32_t BIT64_ARRAY_SIZE_SMALL = 8 * 1024  # 8K * 64 = 524,288 bits
cdef uint32_t BIT64_ARRAY_SIZE_LARGE = 128 * 1024  # 128K * 64 = 8,388,608 bits
cdef uint32_t BIT64_ARRAY_SIZE_HUGE = 2 * 1024 * 1024  # 2M * 64 = 134,217,728 bits
cdef uint32_t BIT64_ARRAY_SIZE_MASSIVE = 32 * 1024 * 1024  # 32M * 64 = 2,147,483,648 bits

# Golden ratio constant for second hash
cdef uint64_t GOLDEN_RATIO = 0x9E3779B97F4A7C15ULL

cdef class BloomFilter:

    def __cinit__(self, uint32_t expected_records=50000):
        """Initialize Bloom Filter based on expected number of records."""
        if expected_records <= 1_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_TINY
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_TINY * 64
        elif expected_records <= 62_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_SMALL
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_SMALL * 64
        elif expected_records <= 1_000_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_LARGE
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_LARGE * 64
        elif expected_records <= 16_000_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_HUGE
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_HUGE * 64
        elif expected_records <= 256_000_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_MASSIVE
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_MASSIVE * 64
        else:
            raise ValueError("Too many records for this Bloom filter implementation")

        # Precompute mask for faster modulo operations
        self.bit_mask = self.bit_array_size_bits - 1

        # Allocate 64-bit aligned memory
        self.bit_array = <uint64_t*>calloc(self.bit64_array_size, sizeof(uint64_t))
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the Bloom filter.")

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cdef inline void _add(self, const uint64_t item):
        cdef uint64_t h1, h2

        # Use bit mask for fast modulo (works because sizes are powers of 2)
        h1 = item & self.bit_mask
        # Better hash mixing for second position
        h2 = (item * GOLDEN_RATIO) & self.bit_mask

        # Set bits using 64-bit operations
        self.bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        self.bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    cpdef void add(self, const uint64_t item):
        self._add(item)

    cdef inline bint _possibly_contains_fast(self, const uint64_t item) nogil:
        cdef uint64_t h1 = item & self.bit_mask
        cdef uint64_t h2 = (item * GOLDEN_RATIO) & self.bit_mask

        # Load both 64-bit chunks before computing shifts, enabling ILP on superscalar CPUs
        cdef uint64_t chunk1 = self.bit_array[h1 >> 6]
        cdef uint64_t chunk2 = self.bit_array[h2 >> 6]

        cdef uint64_t mask1 = (<uint64_t>1) << (h1 & 0x3F)
        cdef uint64_t mask2 = (<uint64_t>1) << (h2 & 0x3F)

        return (chunk1 & mask1) != 0 and (chunk2 & mask2) != 0

    cpdef bint possibly_contains(self, const uint64_t item):
        return self._possibly_contains_fast(item)

    cpdef uint8_t[::1] possibly_contains_many(self, object relation, list columns):
        """
        Optimized batch checking with better memory access patterns.
        Returns a bit-packed boolean buffer (LSB-first, PyArrow bool_ layout).
        """
        cdef Py_ssize_t num_rows = relation.num_rows
        cdef Py_ssize_t num_bytes = (num_rows + 7) >> 3
        cdef array result_arr = clone(_UINT8_TEMPLATE, num_bytes, True)  # zero-initialised
        cdef uint8_t[::1] result = result_arr
        cdef Integer64Vector valid_row_ids_vec = non_null_row_indices(relation, columns)
        cdef const int64_t* valid_row_ids_ptr = <const int64_t*>valid_row_ids_vec.dense_ptr()
        cdef Py_ssize_t num_valid_rows = len(valid_row_ids_vec)
        cdef array row_hashes_arr = clone(_UINT64_TEMPLATE, num_rows, False)
        cdef uint64_t[::1] row_hashes = row_hashes_arr
        cdef Py_ssize_t i
        cdef int64_t row_id
        cdef uint64_t hash_val, h1, h2, mask1, mask2
        cdef uint64_t bit_mask = self.bit_mask
        cdef uint64_t golden_ratio = GOLDEN_RATIO
        cdef uint64_t* bit_array = self.bit_array
        cdef Morsel _m

        if num_valid_rows > 0:
            # Compute hashes only for non-null rows — prefer Draken Morsel.hash()
            row_hashes = relation.hash(columns)

            for i in range(num_valid_rows):
                row_id = valid_row_ids_ptr[i]
                hash_val = row_hashes[row_id]

                h1 = hash_val & bit_mask
                h2 = (hash_val * golden_ratio) & bit_mask

                mask1 = (<uint64_t>1) << (h1 & 0x3F)
                mask2 = (<uint64_t>1) << (h2 & 0x3F)

                if (bit_array[h1 >> 6] & mask1) != 0 and (bit_array[h2 >> 6] & mask2) != 0:
                    result[row_id >> 3] |= <uint8_t>(1 << (row_id & 7))

        return result

    cpdef uint8_t[::1] possibly_contains_many_direct(self, uint64_t[::1] hashes):
        """
        Batch membership check on pre-computed hash values.

        Use when hashes are already available (e.g. from a join probe or group-by
        ingest pass) to avoid redundant hash computation.  Returns a bit-packed
        boolean buffer (LSB-first, PyArrow bool_ layout).
        """
        cdef Py_ssize_t num_hashes = hashes.shape[0]
        cdef Py_ssize_t num_bytes = (num_hashes + 7) >> 3
        cdef array result_arr = clone(_UINT8_TEMPLATE, num_bytes, True)
        cdef uint8_t[::1] result = result_arr
        cdef Py_ssize_t i
        cdef uint64_t hash_val, h1, h2
        cdef uint64_t chunk1, chunk2, mask1, mask2
        cdef uint64_t bit_mask = self.bit_mask
        cdef uint64_t golden_ratio = GOLDEN_RATIO
        cdef uint64_t* bit_array = self.bit_array

        for i in range(num_hashes):
            hash_val = hashes[i]

            h1 = hash_val & bit_mask
            h2 = (hash_val * golden_ratio) & bit_mask

            chunk1 = bit_array[h1 >> 6]
            chunk2 = bit_array[h2 >> 6]

            mask1 = (<uint64_t>1) << (h1 & 0x3F)
            mask2 = (<uint64_t>1) << (h2 & 0x3F)

            if (chunk1 & mask1) != 0 and (chunk2 & mask2) != 0:
                result[i >> 3] |= <uint8_t>(1 << (i & 7))

        return result

cpdef BloomFilter create_bloom_filter(object relation, list columns):
    """
    Optimized Bloom filter creation with better cache behavior.
    """
    cdef array row_hashes_arr = clone(_UINT64_TEMPLATE, relation.num_rows, False)
    cdef Integer64Vector valid_row_ids_vec = non_null_row_indices(relation, columns)
    cdef const int64_t* valid_row_ids_ptr = <const int64_t*>valid_row_ids_vec.dense_ptr()
    cdef Py_ssize_t num_valid_rows = len(valid_row_ids_vec)
    cdef uint64_t[::1] row_hashes = row_hashes_arr
    cdef Py_ssize_t i
    cdef int64_t row_id
    cdef BloomFilter bf = BloomFilter(<uint32_t>num_valid_rows)
    cdef uint64_t hash_val, h1, h2

    if num_valid_rows == 0:
        return bf

    # Populate row hashes using the selected columns (prefer Morsel.hash())
    row_hashes = relation.hash(columns)

    # Precompute constants for faster access
    cdef uint64_t bit_mask = bf.bit_mask
    cdef uint64_t golden_ratio = GOLDEN_RATIO
    cdef uint64_t* bit_array = bf.bit_array

    # Add to bloom filter
    for i in range(num_valid_rows):
        row_id = valid_row_ids_ptr[i]
        hash_val = row_hashes[row_id]

        h1 = hash_val & bit_mask
        h2 = (hash_val * golden_ratio) & bit_mask

        bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    return bf


cpdef BloomFilter create_bloom_filter_from_hashes(uint64_t[::1] hashes):
    """
    Build a Bloom filter directly from pre-computed hash values.

    Fast path when hashes are already available (e.g. from a join build side or
    group-by ingest).  Avoids the relation/column indirection overhead of
    create_bloom_filter().

    Returns None if hashes is empty or exceeds the maximum supported cardinality
    (256 million items).
    """
    cdef Py_ssize_t num_hashes = hashes.shape[0]
    cdef BloomFilter bf
    cdef Py_ssize_t i
    cdef uint64_t hash_val, h1, h2
    cdef uint64_t bit_mask
    cdef uint64_t golden_ratio = GOLDEN_RATIO
    cdef uint64_t* bit_array

    if num_hashes == 0 or num_hashes > <Py_ssize_t>256_000_000:
        return None

    bf = BloomFilter(<uint32_t>num_hashes)
    bit_mask = bf.bit_mask
    bit_array = bf.bit_array

    for i in range(num_hashes):
        hash_val = hashes[i]
        h1 = hash_val & bit_mask
        h2 = (hash_val * golden_ratio) & bit_mask
        bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    return bf


# Draken-native bloom filter API (no Arrow conversion)

cpdef BloomFilter create_bloom_filter_morsel(Morsel morsel, list columns):
    """
    Create a Bloom filter from a Draken Morsel using native hashing.

    Uses Morsel.hash(columns) to compute hashes natively, avoiding any Arrow conversion.
    This is the fast path for join build phases.

    Args:
        morsel: Draken Morsel containing data
        columns: List of column identities to hash

    Returns:
        BloomFilter instance or None if morsel is empty
    """
    if morsel is None or morsel.num_rows == 0:
        return None

    cdef uint64_t[::1] hashes = morsel.hash(columns)
    return create_bloom_filter_from_hashes(hashes)


cpdef uint8_t[::1] bloom_filter_check_morsel(BloomFilter bloom_filter, Morsel morsel, list columns):
    """
    Check Morsel rows against bloom filter using native hashing.

    Returns a bit-packed uint8_t array (LSB-first, PyArrow bool_ layout).
    Rows where result bit is set were "possibly in set" according to bloom filter.

    Uses Morsel.hash(columns) for native hashing, no Arrow conversion in hot path.

    Args:
        bloom_filter: BloomFilter instance (from create_bloom_filter_morsel)
        morsel: Draken Morsel to check
        columns: List of column identities to hash

    Returns:
        Bit-packed uint8_t array with length (morsel.num_rows + 7) >> 3 bytes
        or None if inputs are empty
    """
    if bloom_filter is None or morsel is None or morsel.num_rows == 0:
        return None

    cdef uint64_t[::1] hashes = morsel.hash(columns)
    return bloom_filter.possibly_contains_many_direct(hashes)
