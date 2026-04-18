# cython: language_level=3

from libc.stdint cimport int32_t, int64_t, uint64_t, uint8_t
from libc.stddef cimport size_t


cdef extern from "carchar_set.hpp" namespace "opteryx::carchar" nogil:
    cdef cppclass CarcharSet:
        CarcharSet(size_t initial_capacity, double load_factor) except +
        size_t size() noexcept
        size_t capacity() noexcept
        void reserve(size_t expected_entries) except +
        bint insert_or_ignore(uint64_t key) except +
        size_t insert_many(const uint64_t* keys, size_t length) except +
        size_t mark_new(const uint64_t* keys, uint8_t* out_is_new, size_t length) noexcept
        size_t mark_new_indices_32(const uint64_t* keys, int32_t* out_indices, size_t length) noexcept
        size_t mark_new_indices_64(const uint64_t* keys, int64_t* out_indices, size_t length) noexcept


cdef class CarcharSetWrapper:
    cdef CarcharSet* _ptr

    cpdef size_t size(self)
    cpdef size_t capacity(self)
    cpdef bint add(self, uint64_t value)
    cpdef bint has(self, uint64_t value)
    cpdef void reserve_py(self, size_t capacity)

    cdef inline bint insert(self, uint64_t value) noexcept nogil
    cdef inline bint contains(self, uint64_t value) noexcept nogil
    cdef inline void reserve(self, size_t capacity) noexcept nogil
    cdef Py_ssize_t find_new_indices_out(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int64_t* out_indices,
    ) noexcept nogil
    cdef Py_ssize_t find_new_indices_out_32(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int32_t* out_indices,
    ) noexcept nogil

    # C-level nogil helpers used by hot-path code; prototypes must be
    # present here so Cython generates correct vtables.
    cdef size_t _insert_many_nogil(self, uint64_t* keys, size_t length) noexcept nogil
    cdef void _tighten_nogil(self) noexcept nogil
