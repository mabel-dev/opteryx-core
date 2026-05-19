from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t, uint32_t

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.date32_vector cimport Date32Vector

cdef class TimestampVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef Py_ssize_t null_bit_offset
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef str timestamp_unit  # 'ns', 'us', 'ms', or 's'
    cdef int _unit_code     # integer alias for timestamp_unit (0=ns,1=us,2=ms,3=s)
    cdef DrakenVarBuffer* _dict_values
    cdef uint8_t _dict_ordered

    cdef uint8_t* null_bitmap_ptr(self) noexcept
    cdef DrakenVector* unified(self) noexcept

    cpdef TimestampVector take(self, int32_t[::1] indices)
    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n)
    cpdef BoolVector _compare_scalar(self, int64_t value, int op)
    cpdef BoolVector _compare_vector(self, TimestampVector other, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=*, bint upper_inclusive=*)
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
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    # Temporal arithmetic operations (Phase 5b)
    cpdef Integer64Vector subtract_timestamp_vector(self, TimestampVector other)
    cpdef Integer64Vector subtract_date32_vector(self, Date32Vector other)

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
cdef TimestampVector timestamp_dict_from_raw(
    int64_t num_rows,
    uint32_t* codes,
    DrakenVarBuffer* dict_values,
    uint8_t ordered,
    uint8_t* row_nulls,
    str timestamp_unit,
)
