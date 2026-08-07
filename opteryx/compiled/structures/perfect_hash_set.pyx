# cython: language_level=3
# cython: cplus=True
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

Thin Cython wrapper around opteryx::perfect_hash::PerfectHashSet (C++).

Provides Python-visible constructor and Cython-side hot-path methods.
All hot-path methods are nogil.
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from opteryx.compiled.structures.perfect_hash_set cimport CppPerfectHashSet


cdef class PerfectHashSet:
    """Bit-array set for bounded integer keys.

    Constructed with (min_val, max_val); slots cover [min_val, max_val].
    All values outside that range are a caller error - this class does not
    bounds-check in the fast path (only int64 batch operations do).
    """

    def __cinit__(self, int64_t min_val, int64_t max_val):
        self._ptr = new CppPerfectHashSet(min_val, max_val)

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __repr__(self):
        return f"PerfectHashSet()"

       # Single-value ops
    cdef bint insert_i64(self, int64_t val) noexcept nogil:
        return self._ptr.insert_i64(val)

    cdef bint contains_i64(self, int64_t val) noexcept nogil:
        return self._ptr.contains_i64(val)

      # int8 batch
    cdef Py_ssize_t find_new_indices_out_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_new_indices_out_32_i8(keys, out, <size_t>length)

    cdef Py_ssize_t probe_found_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_found_32_i8(keys, out, <size_t>length)

    cdef Py_ssize_t probe_not_found_32_i8(
        self,
        const int8_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_not_found_32_i8(keys, out, <size_t>length)

      # int16 batch
    cdef Py_ssize_t find_new_indices_out_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_new_indices_out_32_i16(keys, out, <size_t>length)

    cdef Py_ssize_t probe_found_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_found_32_i16(keys, out, <size_t>length)

    cdef Py_ssize_t probe_not_found_32_i16(
        self,
        const int16_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_not_found_32_i16(keys, out, <size_t>length)

      # int32 batch
    cdef Py_ssize_t find_new_indices_out_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_new_indices_out_32_i32(keys, out, <size_t>length)

    cdef Py_ssize_t probe_found_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_found_32_i32(keys, out, <size_t>length)

    cdef Py_ssize_t probe_not_found_32_i32(
        self,
        const int32_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_not_found_32_i32(keys, out, <size_t>length)

      # int64 batch
    cdef Py_ssize_t find_new_indices_out_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_new_indices_out_32_i64(keys, out, <size_t>length)

    cdef Py_ssize_t probe_found_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_found_32_i64(keys, out, <size_t>length)

    cdef Py_ssize_t probe_not_found_32_i64(
        self,
        const int64_t* keys,
        int32_t* out,
        Py_ssize_t length,
      ) noexcept nogil:
        return <Py_ssize_t>self._ptr.probe_not_found_32_i64(keys, out, <size_t>length)
