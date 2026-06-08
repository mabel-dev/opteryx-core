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
opteryx/compiled/structures/perfect_hash_map.pyx

Thin Cython wrapper around opteryx::perfect_hash::PerfectHashMap (C++).

Provides Python-visible constructor and Cython-side hot-path methods.
All hot-path methods are nogil.
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libc.stddef cimport size_t
from opteryx.compiled.structures.perfect_hash_map cimport CppPerfectHashMap


cdef class PerfectHashMap:
    """Direct-addressed hash map for bounded integer keys.

    Constructed with (min_val, max_val); slots cover [min_val, max_val].
    Stores int64_t payloads for each key.
    """

    def __cinit__(self, int64_t min_val, int64_t max_val):
        self._ptr = new CppPerfectHashMap(min_val, max_val)

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __repr__(self):
        return f"PerfectHashMap()"

    # ── Single-value ops ──────────────────────────────────────────────────────

    cdef bint insert_i64(self, int64_t key, int64_t payload) noexcept nogil:
        return self._ptr.insert_i64(key, payload)

    cdef bint lookup_i64(self, int64_t key, int64_t& payload_out) noexcept nogil:
        return self._ptr.lookup_i64(key, payload_out)

    # ── int8 batch ────────────────────────────────────────────────────────────

    cdef Py_ssize_t find_or_insert_32_i8(
        self,
        const int8_t* keys,
        int64_t* payloads_in,
        int32_t* out_is_new,
        int64_t* out_payloads,
        Py_ssize_t length,
    ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_or_insert_32_i8(
            keys, payloads_in, out_is_new, out_payloads, <size_t>length
        )

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

    # ── int16 batch ───────────────────────────────────────────────────────────

    cdef Py_ssize_t find_or_insert_32_i16(
        self,
        const int16_t* keys,
        int64_t* payloads_in,
        int32_t* out_is_new,
        int64_t* out_payloads,
        Py_ssize_t length,
    ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_or_insert_32_i16(
            keys, payloads_in, out_is_new, out_payloads, <size_t>length
        )

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

    # ── int32 batch ───────────────────────────────────────────────────────────

    cdef Py_ssize_t find_or_insert_32_i32(
        self,
        const int32_t* keys,
        int64_t* payloads_in,
        int32_t* out_is_new,
        int64_t* out_payloads,
        Py_ssize_t length,
    ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_or_insert_32_i32(
            keys, payloads_in, out_is_new, out_payloads, <size_t>length
        )

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

    # ── int64 batch ───────────────────────────────────────────────────────────

    cdef Py_ssize_t find_or_insert_32_i64(
        self,
        const int64_t* keys,
        int64_t* payloads_in,
        int32_t* out_is_new,
        int64_t* out_payloads,
        Py_ssize_t length,
    ) noexcept nogil:
        return <Py_ssize_t>self._ptr.find_or_insert_32_i64(
            keys, payloads_in, out_is_new, out_payloads, <size_t>length
        )

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
