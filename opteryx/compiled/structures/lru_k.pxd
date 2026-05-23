# Cython declaration file for LRU_K (backed by C++ LRU2, K=2 fixed)

from libc.stdint cimport int64_t
from libcpp.string cimport string

cdef extern from "lru2.hpp" namespace "opteryx" nogil:
    cdef cppclass CppLRU2 "opteryx::LRU2":
        CppLRU2(int64_t max_size, int64_t max_memory) except +
        void set(const char* key, int64_t key_len,
                 const char* value, int64_t val_len,
                 bint evict)
        bint get_into(const char* key, int64_t key_len,
                      const char** out_data, int64_t* out_len)
        bint evict_one_into(bint need_value,
                            string& key_out, string& val_out)
        bint erase(const char* key, int64_t key_len)
        void clear(bint reset_stats)
        bint contains(const char* key, int64_t key_len)
        int64_t size()
        int64_t current_memory()
        int64_t hits()
        int64_t misses()
        int64_t evictions()
        int64_t inserts()

cdef class LRU_K:
    cdef CppLRU2* _lru
    cdef public int64_t k
    cdef public int64_t max_size
    cdef public int64_t max_memory

    cpdef object get(self, bytes key)
    cpdef tuple set(self, bytes key, bytes value, bint evict)
    cpdef object evict(self, bint details)
    cpdef bint delete(self, bytes key)
    cpdef void clear(self, bint reset_stats)
