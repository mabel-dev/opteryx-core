from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.bool_vector cimport BoolVector

cdef class TimestampVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef Py_ssize_t null_bit_offset
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef str timestamp_unit  # 'ns', 'us', 'ms', or 's'

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
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef int64_t min(self)
    cpdef int64_t max(self)

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
