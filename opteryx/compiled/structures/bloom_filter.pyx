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
Bloom filter — not general-purpose; tuned for Opteryx join/group-by pre-filtering.

Five size tiers (all powers of 2, all using 2 hashes):
    8k bits  — up to 1k items   (~4.9% FPR)
    512k bits — up to 62k items  (~4.2% FPR)
    8M bits  — up to 1M items   (~4.5% FPR)
    128M bits — up to 16M items  (~4.7% FPR)
    2B bits  — up to 256M items  (~0.3% FPR at 56M items)

Second-hash position: HIGH bits of a golden-ratio multiply (Fibonacci
hashing), cheaper than a second full hash. High bits are load-bearing —
the low bits of h*C depend only on the low bits of h, so a masked product
is a bijection of the first position and the FPR triples.

Hot insert and probe paths live in bloom_filter_ops.hpp (NEON / SSE2 / scalar).
"""

from libc.stdlib cimport calloc, free
from libc.stdint cimport uint8_t, uint64_t, uint32_t, int64_t
from cpython.array cimport array, clone

from draken.morsels.morsel cimport Morsel

cdef extern from "bloom_filter_ops.hpp" nogil:
    void bloom_insert_many(
        uint64_t* bit_array,
        const uint64_t* hashes,
        size_t n,
        uint64_t bit_mask
    )
    void bloom_query_packed(
        const uint64_t* bit_array,
        const uint64_t* hashes,
        size_t n,
        uint64_t bit_mask,
        uint8_t* result
    )

# Reusable template for zero-copy clone allocations
cdef array _UINT8_TEMPLATE = array('B', [])

# Bloom filter tier boundaries (in 64-bit chunks)
cdef uint32_t BIT64_ARRAY_SIZE_TINY    =       128   #   8 192 bits
cdef uint32_t BIT64_ARRAY_SIZE_SMALL   =     8_192   # 524 288 bits
cdef uint32_t BIT64_ARRAY_SIZE_LARGE   =   131_072   #   8M bits
cdef uint32_t BIT64_ARRAY_SIZE_HUGE    = 2_097_152   # 128M bits
cdef uint32_t BIT64_ARRAY_SIZE_MASSIVE = 33_554_432  #   2G bits

cdef uint64_t GOLDEN_RATIO = 0x9E3779B97F4A7C15ULL


cdef class BloomFilter:

    def __cinit__(self, uint32_t expected_records=50000):
        if expected_records <= 1_000:
            self.bit64_array_size   = BIT64_ARRAY_SIZE_TINY
        elif expected_records <= 62_000:
            self.bit64_array_size   = BIT64_ARRAY_SIZE_SMALL
        elif expected_records <= 1_000_000:
            self.bit64_array_size   = BIT64_ARRAY_SIZE_LARGE
        elif expected_records <= 16_000_000:
            self.bit64_array_size   = BIT64_ARRAY_SIZE_HUGE
        elif expected_records <= 256_000_000:
            self.bit64_array_size   = BIT64_ARRAY_SIZE_MASSIVE
        else:
            raise ValueError("Too many records for this Bloom filter implementation")

        self.bit_array_size_bits = self.bit64_array_size * 64
        self.bit_mask            = self.bit_array_size_bits - 1
        # Second position = high bits of the golden-ratio multiply (Fibonacci
        # hashing): shift = 64 - log2(bits). Masking the product instead makes
        # the second position a bijection of the first (low product bits depend
        # only on low input bits) — a single-hash filter, ~12% FPR.
        cdef uint64_t mask = self.bit_mask
        cdef uint32_t log2_bits = 0
        while mask:
            log2_bits += 1
            mask >>= 1
        self.bit_shift = 64 - log2_bits

        self.bit_array = <uint64_t*>calloc(self.bit64_array_size, sizeof(uint64_t))
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for Bloom filter")

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cdef inline void _add(self, const uint64_t item) nogil:
        cdef uint64_t h1 = item & self.bit_mask
        cdef uint64_t h2 = (item * GOLDEN_RATIO) >> self.bit_shift
        self.bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        self.bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    cpdef void add(self, const uint64_t item):
        self._add(item)

    cdef inline bint _possibly_contains_fast(self, const uint64_t item) nogil:
        cdef uint64_t h1 = item & self.bit_mask
        cdef uint64_t h2 = (item * GOLDEN_RATIO) >> self.bit_shift
        cdef uint64_t chunk1 = self.bit_array[h1 >> 6]
        cdef uint64_t chunk2 = self.bit_array[h2 >> 6]
        cdef uint64_t mask1  = (<uint64_t>1) << (h1 & 0x3F)
        cdef uint64_t mask2  = (<uint64_t>1) << (h2 & 0x3F)
        return (chunk1 & mask1) != 0 and (chunk2 & mask2) != 0

    cpdef bint possibly_contains(self, const uint64_t item):
        return self._possibly_contains_fast(item)

    cpdef uint8_t[::1] possibly_contains_many_direct(self, uint64_t[::1] hashes):
        """Batch membership check on pre-computed hash values.

        Returns a bit-packed uint8_t array (LSB-first). Set bit = possibly in set.
        """
        cdef Py_ssize_t num_hashes = hashes.shape[0]
        cdef Py_ssize_t num_bytes  = (num_hashes + 7) >> 3
        cdef array result_arr = clone(_UINT8_TEMPLATE, num_bytes, True)  # zero-init
        cdef uint8_t[::1] result = result_arr
        if num_hashes == 0:
            return result
        with nogil:
            bloom_query_packed(
                self.bit_array, &hashes[0],
                <size_t>num_hashes, self.bit_mask, &result[0]
            )
        return result


cpdef BloomFilter create_bloom_filter_from_hashes(uint64_t[::1] hashes):
    """Build a Bloom filter directly from pre-computed hash values."""
    cdef Py_ssize_t num_hashes = hashes.shape[0]
    if num_hashes == 0 or num_hashes > <Py_ssize_t>256_000_000:
        return None

    cdef BloomFilter bf = BloomFilter(<uint32_t>num_hashes)
    with nogil:
        bloom_insert_many(bf.bit_array, &hashes[0], <size_t>num_hashes, bf.bit_mask)
    return bf


cpdef BloomFilter create_bloom_filter_morsel(Morsel morsel, list columns):
    """Create a Bloom filter from a Draken Morsel using native hashing."""
    if morsel is None or morsel.num_rows == 0:
        return None
    cdef uint64_t[::1] hashes = morsel.hash(columns)
    return create_bloom_filter_from_hashes(hashes)


cpdef uint8_t[::1] bloom_filter_check_morsel(BloomFilter bloom_filter, Morsel morsel, list columns):
    """Check Morsel rows against a Bloom filter using native hashing.

    Returns a bit-packed uint8_t array (LSB-first, PyArrow bool_ layout).
    """
    if bloom_filter is None or morsel is None or morsel.num_rows == 0:
        return None
    cdef uint64_t[::1] hashes = morsel.hash(columns)
    return bloom_filter.possibly_contains_many_direct(hashes)
