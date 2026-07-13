# footer_cache.pxd — typed Cython interface for ParquetFooterBytesCache / ParquetParsedFooterCache
from libc.stdint cimport int64_t, uint8_t
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


cdef class ParquetParsedFooterCache:
    cdef unordered_map[string, FileStats] _map
    cdef LRU_K lru
    cdef int _max_entries
    cdef cpp_mutex* _mutex

    cdef bint try_get(self, str path, FileStats* out)
    cdef const FileStats* try_get_ptr(self, str path)
    cdef void put_fs(self, str path, const FileStats& fs)
    cpdef void clear(self)
    cpdef dict stats(self)
