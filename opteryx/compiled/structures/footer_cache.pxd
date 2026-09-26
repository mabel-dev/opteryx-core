# footer_cache.pxd — typed Cython interface for ParquetFooterBytesCache / ParquetParsedFooterCache
from libc.stdint cimport int64_t, uint8_t
from libcpp.memory cimport shared_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string

from opteryx.compiled.structures.lru_k cimport LRU_K
from opteryx.compiled.structures.memory_pool cimport MemoryPool
from rugo.parquet_reader cimport FileStats


cdef extern from "<mutex>" namespace "std" nogil:
    cppclass cpp_mutex "std::mutex":
        cpp_mutex()
        void lock()
        void unlock()


cdef class ParquetFooterBytesCache:
    cdef MemoryPool pool
    cdef LRU_K lru
    cdef dict _path_to_ref
    cdef object _lock

    cpdef object get(self, str path)
    cpdef bint put(self, str path, const uint8_t[::1] envelope)
    cpdef void clear(self)
    cpdef dict stats(self)


# The shared footer-map vocabulary (src/cpp/engine/parquet_footer_map.hpp): a parsed
# footer is immutable and held by shared_ptr, so scans pin it instead of copying it.
cdef extern from "engine/parquet_footer_map.hpp" nogil:
    ctypedef shared_ptr[const FileStats] ParquetFooterRef
    ctypedef unordered_map[string, ParquetFooterRef] ParquetFooterMap
    int64_t parquet_footer_bytes(const FileStats& fs)


cdef class ParquetParsedFooterCache:
    cdef unordered_map[string, ParquetFooterRef] _map
    cdef unordered_map[string, int64_t] _entry_bytes
    cdef LRU_K lru
    cdef int64_t _budget_bytes
    cdef int64_t _resident_bytes
    cdef int64_t _over_budget
    cdef cpp_mutex* _mutex

    cdef ParquetFooterRef get(self, str path)
    cdef ParquetFooterRef put(self, str path, FileStats fs)
    cpdef void clear(self)
    cpdef dict stats(self)
