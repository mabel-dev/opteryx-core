# Minimal Cython declaration file for LRU_K
# Allows other .pyx modules to use typed references via cimport

from libc.stdint cimport int64_t

cdef class LRU_K:
    cdef public int64_t k
    cdef public int64_t max_size
    cdef public int64_t max_memory
    cdef int64_t current_memory
    cdef object slots
    cdef dict access_history
    cdef int64_t _clock
    cdef int64_t hits
    cdef int64_t misses
    cdef int64_t evictions
    cdef int64_t inserts
    cdef public int64_t size
