from libc.stdint cimport int32_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.bool_vector cimport BoolVector

cdef class Date32Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef ConstAccessor _const_accessor
    cdef int32_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null

    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef Date32Vector take(self, int32_t[::1] indices)
    cdef bint _compare_date_values(self, int32_t left, int32_t right, int op) nogil
    cdef BoolVector _compare_scalar(self, int32_t value, int op)

    cpdef BoolVector equals(self, int32_t value)
    cpdef BoolVector not_equals(self, int32_t value)
    cpdef BoolVector greater_than(self, int32_t value)
    cpdef BoolVector greater_than_or_equals(self, int32_t value)
    cpdef BoolVector less_than(self, int32_t value)
    cpdef BoolVector less_than_or_equals(self, int32_t value)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef int32_t min(self)
    cpdef int32_t max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Date32Vector from_arrow(object array)
cdef Date32Vector from_dict(const int32_t[::1] codes, const int32_t[::1] dictionary)
cdef Date32Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int32_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
