# cython: language_level=3

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t
from libc.stddef cimport size_t


cdef extern from "perfect_hash_set.hpp" namespace "opteryx::perfect_hash" nogil:
    cdef cppclass CppPerfectHashSet "opteryx::perfect_hash::PerfectHashSet":
        CppPerfectHashSet(int64_t min_val, int64_t max_val) except +

          # Single-value operations
        bint insert_i64(int64_t val) noexcept
        bint contains_i64(int64_t val) noexcept

          # int8_t batch operations
        size_t find_new_indices_out_32_i8(
            const int8_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_found_32_i8(
            const int8_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_not_found_32_i8(
            const int8_t* keys, int32_t* out, size_t length
          ) noexcept

          # int16_t batch operations
        size_t find_new_indices_out_32_i16(
            const int16_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_found_32_i16(
            const int16_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_not_found_32_i16(
            const int16_t* keys, int32_t* out, size_t length
          ) noexcept

          # int32_t batch operations
        size_t find_new_indices_out_32_i32(
            const int32_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_found_32_i32(
            const int32_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_not_found_32_i32(
            const int32_t* keys, int32_t* out, size_t length
          ) noexcept

          # int64_t batch operations
        size_t find_new_indices_out_32_i64(
            const int64_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_found_32_i64(
            const int64_t* keys, int32_t* out, size_t length
          ) noexcept
        size_t probe_not_found_32_i64(
            const int64_t* keys, int32_t* out, size_t length
          ) noexcept

          # Getters for _rebuild_carchar_from_phash fallback
        int64_t min_val()
        int64_t range()
        size_t  n_words()
        uint64_t word_at(size_t i)


cdef class PerfectHashSet:
    cdef CppPerfectHashSet* _ptr

      # Single-value operations
    cdef bint insert_i64(self, int64_t val) noexcept nogil
    cdef bint contains_i64(self, int64_t val) noexcept nogil

      # int8_t batch operations
    cdef Py_ssize_t find_new_indices_out_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i8(
        self, const int8_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil

      # int16_t batch operations
    cdef Py_ssize_t find_new_indices_out_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i16(
        self, const int16_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil

      # int32_t batch operations
    cdef Py_ssize_t find_new_indices_out_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i32(
        self, const int32_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil

      # int64_t batch operations
    cdef Py_ssize_t find_new_indices_out_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_found_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
    cdef Py_ssize_t probe_not_found_32_i64(
        self, const int64_t* keys, int32_t* out, Py_ssize_t length
      ) noexcept nogil
