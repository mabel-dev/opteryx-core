# cython: language_level=3

from libc.stdint cimport uint64_t, int32_t, int64_t
from libc.stddef cimport size_t

cdef extern from "parvi.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharSet:
        pass

cdef extern from "parvi.hpp" namespace "opteryx::parvi":
    cdef struct ParviSetResult:
        bint is_new

    cdef cppclass ParviSet:
        ParviSet() except +
        size_t size() const
        bint full() const
        bint contains(uint64_t key) const
        ParviSetResult insert_or_ignore(uint64_t key)
        void clear()
        void drain_into(CarcharSet& target) const
        # mark_new_indices is a template, handled via extern from * in .pyx

cdef class ParviSetWrapper:
    cdef ParviSet* _ptr
    cpdef size_t size(self)
    cpdef bint full(self)
    cpdef bint contains(self, uint64_t key)
    cpdef bint insert(self, uint64_t key)
    cpdef tuple mark_new_indices_32_public(
        self,
        uint64_t[::1] keys_view,
        int32_t[::1] indices_view,
        size_t length,
    )
    cpdef void clear(self)
    cdef tuple mark_new_indices_32(
        self,
        uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept
    cdef tuple mark_new_indices_64(
        self,
        uint64_t* keys,
        int64_t* out_indices,
        size_t length,
    ) noexcept
