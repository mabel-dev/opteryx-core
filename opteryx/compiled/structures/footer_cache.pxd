# footer_cache.pxd — typed Cython interface for ParquetFooterBytesCache
from libc.stdint cimport int64_t, uint8_t

from opteryx.compiled.structures.lru_k cimport LRU_K
from opteryx.compiled.structures.memory_pool cimport MemoryPool


cdef class ParquetFooterBytesCache:
    cdef MemoryPool pool
    cdef LRU_K lru
    cdef dict _path_to_ref
    cdef object _lock

    cpdef object get(self, str path)
    cpdef bint put(self, str path, const uint8_t[::1] envelope)
    cpdef void clear(self)
    cpdef dict stats(self)
