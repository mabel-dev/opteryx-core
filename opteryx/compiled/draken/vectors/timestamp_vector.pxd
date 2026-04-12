from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DictAccessor, DrakenFixedBuffer, DrakenVarBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector

cdef class TimestampVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef Py_ssize_t null_bit_offset
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef str timestamp_unit  # 'ns', 'us', 'ms', or 's'
    cdef int _unit_code     # integer alias for timestamp_unit (0=ns,1=us,2=ms,3=s)
    cdef ConstAccessor _const_accessor
    cdef int64_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null
    cdef DictAccessor _dict_accessor
    cdef DrakenVarBuffer* _dict_values
    cdef uint8_t* _dict_codes
    cdef uint8_t _dict_code_width
    cdef uint8_t _dict_ordered

    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef TimestampVector take(self, int32_t[::1] indices)
    cdef bint _compare_timestamp_values(self, int64_t left, int64_t right, int op) nogil
    cdef BoolVector _compare_scalar(self, int64_t value, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector equals_vector(self, TimestampVector other)
    cpdef BoolVector not_equals_vector(self, TimestampVector other)
    cpdef BoolVector greater_than_vector(self, TimestampVector other)
    cpdef BoolVector greater_than_or_equals_vector(self, TimestampVector other)
    cpdef BoolVector less_than_vector(self, TimestampVector other)
    cpdef BoolVector less_than_or_equals_vector(self, TimestampVector other)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef int64_t min(self)
    cpdef int64_t max(self)
    cpdef int64_t sum(self)

    # Temporal arithmetic operations (Phase 5b)
    cpdef Int64Vector subtract_timestamp_vector(self, TimestampVector other)
    cpdef Int64Vector subtract_date32_vector(self, Date32Vector other)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef TimestampVector from_arrow(object array)
cdef TimestampVector from_dict(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    str timestamp_unit,
)
cdef TimestampVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
    str timestamp_unit,
)
