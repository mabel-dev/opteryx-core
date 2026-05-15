# cython: language_level=3

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t


cdef class PerfectHashSet:
    cdef int64_t  _min_val
    cdef int64_t  _range          # max - min + 1
    cdef uint64_t* _words         # ceil(range / 64) uint64_t words, calloc'd
    cdef Py_ssize_t _n_words

    # Single-value operations
    cdef bint insert_i64(self, int64_t val) noexcept nogil
    cdef bint contains_i64(self, int64_t val) noexcept nogil

    # Batch operations for int8 backing (Int8 narrow integer columns)
    cdef Py_ssize_t find_new_indices_out_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil

    # Batch operations for int16 backing (Int16 narrow integer columns)
    cdef Py_ssize_t find_new_indices_out_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil

    # Batch operations for int32 backing (Date32Vector physical storage)
    cdef Py_ssize_t find_new_indices_out_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil

    # Batch operations for generic int64 values (IN-list literals, TimestampVector)
    cdef Py_ssize_t find_new_indices_out_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
    ) noexcept nogil
