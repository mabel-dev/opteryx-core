# cython: language_level=3

from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector
from libcpp.pair cimport pair


cdef extern from "carchar_join_index.hpp" namespace "opteryx::carchar" nogil:
    cdef cppclass CarcharJoinIndex:
        CarcharJoinIndex(size_t initial_capacity, double load_factor) except +
        size_t size() noexcept
        size_t capacity() noexcept
        void reserve(size_t expected_entries) except +
        pair[int64_t, bint] insert_row(uint64_t key, int64_t row_id) except +
        vector[int64_t] rows_for(uint64_t key) except +
        vector[int64_t] get(uint64_t key) except +
        vector[int64_t] rows_from_payload(int64_t payload_ref) noexcept
        vector[pair[uint64_t, int64_t]] items() noexcept


cdef class CarcharJoinIndexWrapper:
    cdef CarcharJoinIndex* _ptr

    cpdef size_t size(self)
    cpdef size_t capacity(self)
    cpdef void reserve(self, size_t capacity)
    cpdef void insert_row(self, uint64_t key, int64_t row_id)

    cpdef vector[int64_t] rows_for(self, uint64_t key)
    cpdef list items_py(self)
