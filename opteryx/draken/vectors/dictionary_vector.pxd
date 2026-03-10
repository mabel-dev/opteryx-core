# cython: language_level=3

from libc.stdint cimport int32_t, uint64_t
from opteryx.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.vector cimport Vector


cdef class DictionaryVector(Vector):
    cdef DrakenDictionaryBuffer* ptr
    cdef bint owns_data
    cdef bint owns_dictionary_values
    cdef object _dict_owner_ref
    cdef BoolVector _equals_numeric(self, object literal, bint invert)
    cdef BoolVector _compare_numeric(self, object literal, int op)
    cdef BoolVector _in_list_numeric(self, object literals)

    cpdef DictionaryVector take(self, int32_t[::1] indices)
    cpdef list to_pylist(self)
    cpdef object null_bitmap(self)
    cpdef BoolVector is_null_boolvector(self)
    cpdef BoolVector equals(self, object literal)
    cpdef BoolVector not_equals(self, object literal)
    cpdef BoolVector less_than(self, object literal)
    cpdef BoolVector greater_than(self, object literal)
    cpdef BoolVector less_than_or_equals(self, object literal)
    cpdef BoolVector greater_than_or_equals(self, object literal)
    cpdef BoolVector in_list(self, object literals)
    cpdef BoolVector like(self, object pattern, bint ignore_case=*)
    cpdef BoolVector rlike(self, object pattern)
    cpdef BoolVector contains(self, object substr, bint ignore_case=*)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *


cdef DictionaryVector from_arrow(object array)
