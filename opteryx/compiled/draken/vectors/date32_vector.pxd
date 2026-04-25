from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector

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
    cpdef BoolVector equals_vector(self, Date32Vector other)
    cpdef BoolVector not_equals_vector(self, Date32Vector other)
    cpdef BoolVector greater_than_vector(self, Date32Vector other)
    cpdef BoolVector greater_than_or_equals_vector(self, Date32Vector other)
    cpdef BoolVector less_than_vector(self, Date32Vector other)
    cpdef BoolVector less_than_or_equals_vector(self, Date32Vector other)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef int32_t min(self)
    cpdef int32_t max(self)
    cpdef int64_t sum(self)
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    # Temporal arithmetic operations (Phase 5b)
    cpdef Int64Vector subtract_date32_vector(self, Date32Vector other)
    cpdef Int64Vector subtract_timestamp_vector(self, TimestampVector other)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Date32Vector from_arrow(object array)
cdef Date32Vector from_dict(const int32_t[::1] codes, const int32_t[::1] dictionary)
cdef Date32Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int32_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
