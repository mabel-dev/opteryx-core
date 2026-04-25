# Cython declaration file for LRU_K
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

    cpdef object get(self, bytes key)
    cpdef tuple set(self, bytes key, bytes value, bint evict)
    cpdef object evict(self, bint details)
    cpdef bint delete(self, bytes key)
    cpdef void clear(self, bint reset_stats)
    cdef void _update_access_history(self, bytes key)
    cdef tuple _evict_if_needed(self)
    cdef bint _should_evict(self)
    cdef tuple _evict_one(self, bint details)
