# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint64_t, int32_t, uint8_t
from libc.stddef cimport size_t
from libcpp.pair cimport pair


cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V, HashFunc]:
        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()

cdef class FlatHashMap:
    #cdef flat_hash_map[uint64_t, vector[int64_t]] _map

    def __cinit__(self):
        self._map = flat_hash_map[uint64_t, vector[int64_t], IdentityHash]()

    cpdef insert(self, key: uint64_t, value: int64_t):
        self._map[key].push_back(value)

    cpdef size_t size(self):
        return self._map.size()

    cpdef clear(self):
        self._map.clear()

    cpdef vector[int64_t] get(self, uint64_t key):
        return self._map[key]

    cpdef size_t get_count(self, uint64_t key):
        return self._map[key].size()

    cpdef uint64_t get_many_count(self, uint64_t[::1] keys):
        cdef Py_ssize_t i
        cdef uint64_t total = 0
        for i in range(keys.shape[0]):
            total += self._map[keys[i]].size()
        return total


cdef class FlatHashMapByteVector:
    """Abseil flat_hash_map[uint64_t, vector[uint8_t]] for serialized key storage."""
    #cdef flat_hash_map[uint64_t, vector[uint8_t], IdentityHash] _map

    def __cinit__(self):
        self._map = flat_hash_map[uint64_t, vector[uint8_t], IdentityHash]()

    cdef void store(self, uint64_t key, vector[uint8_t] value) noexcept nogil:
        """Store a vector directly without Python conversion."""
        self._map[key] = value

    cdef vector[uint8_t] retrieve(self, uint64_t key) noexcept nogil:
        """Retrieve a vector from the map."""
        return self._map[key]

    cpdef size_t size(self):
        return self._map.size()

    cpdef clear(self):
        self._map.clear()

