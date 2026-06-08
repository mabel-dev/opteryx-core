# cython: language_level=3

from libc.stdint cimport uint64_t, int32_t, int64_t
from libc.stddef cimport size_t
from libcpp cimport bool as cpp_bool
from libcpp.pair cimport pair
from opteryx.compiled.structures.carchar_set cimport CarcharSet, CarcharSetWrapper

cdef extern from "parvi.hpp" namespace "opteryx::parvi" nogil:
    cdef struct ParviSetResult:
        bint is_new
        bint overflow

    cdef cppclass ParviSet:
        ParviSet() except +
        size_t size() const
        bint full() const
        bint contains(uint64_t key) const
        ParviSetResult insert_or_ignore(uint64_t key)
        void clear()
        void drain_into(CarcharSet& target) const
        # Template hot-path method — callable directly under nogil. Returns
        # (count_new, overflow). Carchar's equivalent is mark_new_indices_32.
        pair[size_t, cpp_bool] mark_new_indices[IndexT](
            const uint64_t* keys, IndexT* out_indices, size_t length
        ) noexcept

cdef class ParviSetWrapper:
    cdef ParviSet* _ptr
    cpdef size_t size(self)
    cpdef bint full(self)
    cpdef bint contains(self, uint64_t key)
    cpdef bint insert(self, uint64_t key)
    cpdef void drain_into_carchar(self, CarcharSetWrapper target)
    cpdef void clear(self)
