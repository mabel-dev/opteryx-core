from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport ConstAccessor
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector

cdef class Int64Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef DictAccessor _dict_accessor
    cdef DrakenVarBuffer* _dict_values
    cdef uint8_t* _dict_codes
    cdef uint8_t _dict_code_width
    cdef uint8_t _dict_ordered
    cdef ConstAccessor _const_accessor
    cdef int64_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null

    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef Int64Vector take(self, int32_t[::1] indices)
    cdef bint _compare_int64_values(self, int64_t left, int64_t right, int op) nogil
    cdef BoolVector _compare_scalar(self, int64_t value, int op)
    cdef BoolVector _compare_vector(self, Int64Vector other, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector equals_vector(self, Int64Vector other)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector not_equals_vector(self, Int64Vector other)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_vector(self, Int64Vector other)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector greater_than_or_equals_vector(self, Int64Vector other)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_vector(self, Int64Vector other)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than_or_equals_vector(self, Int64Vector other)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)
    #cpdef int64_t __getitem__(self, Py_ssize_t i)

    cpdef int64_t sum(self)
    cpdef int64_t min(self)
    cpdef int64_t max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Int64Vector from_arrow(object array)
cdef Int64Vector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary)
cdef Int64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
cdef Int64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=*,
    bint ordered=*,
    const uint8_t* dict_entry_null_bitmap=*,
)
cdef Int64Vector from_sequence(const int64_t[::1] data)
