# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: lintrule=ignore

from libc.stdint cimport int64_t, uint64_t, int32_t, uint8_t
from libc.stddef cimport size_t
from libcpp.pair cimport pair
from libcpp.vector cimport vector

# Identity Hash Definition - not part of abseil but used by our implementation
# We prehash the values before putting them into the Map & Set, so don't rehash
cdef extern from "identity_hash.h":
    cdef cppclass IdentityHash:
        size_t operator()(uint64_t value) const


cdef extern from "absl/container/flat_hash_map.h" namespace "absl" nogil:
    cdef cppclass flat_hash_map[K, V, HashFunc=*]:
        cppclass iterator:
            pair[K, V]& operator*()
            iterator operator++()
            bint operator!=(iterator)

        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()
        void reserve(size_t value)
        iterator begin()
        iterator end()

cdef class FlatHashMap:
    cdef flat_hash_map[uint64_t, vector[int64_t], IdentityHash] _map

    cpdef insert(self, uint64_t key, int64_t value)
    cpdef size_t size(self)
    cpdef clear(self)
    cpdef vector[int64_t] get(self, uint64_t key)
    cpdef size_t get_count(self, uint64_t key)
    cpdef uint64_t get_many_count(self, uint64_t[::1] keys)

cdef class FlatHashMapByteVector:
    cdef flat_hash_map[uint64_t, vector[uint8_t], IdentityHash] _map

    cdef void store(self, uint64_t key, vector[uint8_t] value) noexcept nogil
    cdef vector[uint8_t] retrieve(self, uint64_t key) noexcept nogil
    cpdef size_t size(self)
    cpdef clear(self)

