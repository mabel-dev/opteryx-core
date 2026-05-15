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
opteryx/compiled/structures/perfect_hash_set.pyx

Direct-addressed bit-set for bounded integer keys.

Replaces CarcharSetWrapper on the eligible path when:
  - The key column is a narrow integer (Int8 or Int16), AND
  - The key range (max - min + 1) fits within the configured cap.

Bit-array layout:
  slot  = key - _min_val              (always in [0, _range))
  word  = _words[slot >> 6]           (uint64_t)
  mask  = 1ULL << (slot & 63)
  test  = word & mask
  set   = word |= mask

All hot-path methods are noexcept nogil. Null handling: callers must check
null_bitmap_ptr() and skip null rows before calling batch methods; this class
has no null slot. If a null column is detected at call-site, fall back to the
CarcharSetWrapper path.
"""

from libc.stdlib cimport calloc, free
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t


cdef class PerfectHashSet:
    """Bit-array set for bounded integer keys.

    Constructed with (min_val, max_val); slots cover [min_val, max_val].
    All values outside that range are a caller error — this class does not
    bounds-check in noexcept nogil methods.
    """

    def __cinit__(self, int64_t min_val, int64_t max_val):
        self._min_val = min_val
        self._range   = max_val - min_val + 1
        self._n_words = (self._range + 63) // 64
        self._words   = <uint64_t*>calloc(<size_t>self._n_words, sizeof(uint64_t))
        if self._words == NULL:
            raise MemoryError("PerfectHashSet: calloc failed")

    def __dealloc__(self):
        if self._words != NULL:
            free(self._words)
            self._words = NULL

    # ── Single-value ops ──────────────────────────────────────────────────────

    cdef bint insert_i64(self, int64_t val) noexcept nogil:
        cdef int64_t idx = val - self._min_val
        cdef uint64_t mask = <uint64_t>1 << (idx & 63)
        cdef bint is_new = not (self._words[idx >> 6] & mask)
        self._words[idx >> 6] |= mask
        return is_new

    cdef bint contains_i64(self, int64_t val) noexcept nogil:
        cdef int64_t idx = val - self._min_val
        return bool(self._words[idx >> 6] & (<uint64_t>1 << (idx & 63)))

    # ── int8 batch ────────────────────────────────────────────────────────────

    cdef Py_ssize_t find_new_indices_out_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        cdef uint64_t mask
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            mask = <uint64_t>1 << (idx & 63)
            if not (self._words[idx >> 6] & mask):
                self._words[idx >> 6] |= mask
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_found_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if self._words[idx >> 6] & (<uint64_t>1 << (idx & 63)):
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_not_found_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if not (self._words[idx >> 6] & (<uint64_t>1 << (idx & 63))):
                out[count] = <int32_t>i
                count += 1
        return count

    # ── int16 batch ───────────────────────────────────────────────────────────

    cdef Py_ssize_t find_new_indices_out_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        cdef uint64_t mask
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            mask = <uint64_t>1 << (idx & 63)
            if not (self._words[idx >> 6] & mask):
                self._words[idx >> 6] |= mask
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_found_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if self._words[idx >> 6] & (<uint64_t>1 << (idx & 63)):
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_not_found_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if not (self._words[idx >> 6] & (<uint64_t>1 << (idx & 63))):
                out[count] = <int32_t>i
                count += 1
        return count

    # ── int32 batch (Date32Vector physical storage) ──────────────────────────

    cdef Py_ssize_t find_new_indices_out_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        cdef uint64_t mask
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            mask = <uint64_t>1 << (idx & 63)
            if not (self._words[idx >> 6] & mask):
                self._words[idx >> 6] |= mask
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_found_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if self._words[idx >> 6] & (<uint64_t>1 << (idx & 63)):
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_not_found_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = <int64_t>keys[i] - self._min_val
            if not (self._words[idx >> 6] & (<uint64_t>1 << (idx & 63))):
                out[count] = <int32_t>i
                count += 1
        return count

    # ── int64 batch (for IN-list literals, TimestampVector, TimeVector) ──────

    cdef Py_ssize_t find_new_indices_out_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        cdef uint64_t mask
        for i in range(length):
            idx = keys[i] - self._min_val
            if idx < 0 or idx >= self._range:
                out[count] = <int32_t>i  # out-of-range → not yet seen
                count += 1
                continue
            mask = <uint64_t>1 << (idx & 63)
            if not (self._words[idx >> 6] & mask):
                self._words[idx >> 6] |= mask
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_found_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = keys[i] - self._min_val
            if idx < 0 or idx >= self._range:
                continue  # out-of-range → not in set
            if self._words[idx >> 6] & (<uint64_t>1 << (idx & 63)):
                out[count] = <int32_t>i
                count += 1
        return count

    cdef Py_ssize_t probe_not_found_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
    ) noexcept nogil:
        cdef Py_ssize_t i, count = 0
        cdef int64_t idx
        for i in range(length):
            idx = keys[i] - self._min_val
            if idx < 0 or idx >= self._range:
                out[count] = <int32_t>i  # out-of-range → not in set → NOT IN matches
                count += 1
                continue
            if not (self._words[idx >> 6] & (<uint64_t>1 << (idx & 63))):
                out[count] = <int32_t>i
                count += 1
        return count
