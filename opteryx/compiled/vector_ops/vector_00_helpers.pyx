# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False

# Shared inline helpers for the vector_ops consolidated module.
# This file is alphabetically first so its definitions are available to all
# subsequent includes.

from libc.stdint cimport uint8_t
from libc.stddef cimport size_t
from libc.string cimport memset

from draken.vectors.bool_vector cimport BoolVector, from_decoded

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t size) nogil
    void draken_free(void* ptr) nogil


cdef inline uint8_t _sv_ascii_lower(uint8_t c) noexcept nogil:
    if c >= 65 and c <= 90:
        return c + 32
    return c


cdef BoolVector _all_null_bool(Py_ssize_t n):
    """Return a BoolVector of length n with all rows null."""
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef Py_ssize_t alloc = nbytes if nbytes > 0 else 1
    cdef uint8_t* data = <uint8_t*>draken_malloc(<size_t>alloc)
    if data == NULL:
        raise MemoryError()
    memset(data, 0, <size_t>alloc)
    cdef uint8_t* null_bm = <uint8_t*>draken_malloc(<size_t>alloc)
    if null_bm == NULL:
        draken_free(data)
        raise MemoryError()
    memset(null_bm, 0, <size_t>alloc)
    return from_decoded(<void*>data, null_bm, <size_t>n)
