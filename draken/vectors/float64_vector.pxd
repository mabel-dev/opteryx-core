from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from draken.core.buffers cimport ConstAccessor
from draken.core.buffers cimport DictAccessor
from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenRLEBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector

cdef class Float64Vector(Vector):
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
    cdef double _const_value
    cdef bint _has_const
    cdef bint _const_is_null
    cdef DrakenRLEBuffer* _rle_buffer

    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef Float64Vector take(self, int32_t[::1] indices)
    cdef bint _compare_float_values(self, double left, double right, int op) nogil
    cdef BoolVector _compare_scalar(self, double value, int op)
    cdef BoolVector _compare_vector(self, Float64Vector other, int op)

    cpdef BoolVector equals(self, double value)
    cpdef BoolVector equals_vector(self, Float64Vector other)
    cpdef BoolVector not_equals(self, double value)
    cpdef BoolVector not_equals_vector(self, Float64Vector other)
    cpdef BoolVector greater_than(self, double value)
    cpdef BoolVector greater_than_vector(self, Float64Vector other)
    cpdef BoolVector greater_than_or_equals(self, double value)
    cpdef BoolVector greater_than_or_equals_vector(self, Float64Vector other)
    cpdef BoolVector less_than(self, double value)
    cpdef BoolVector less_than_vector(self, Float64Vector other)
    cpdef BoolVector less_than_or_equals(self, double value)
    cpdef BoolVector less_than_or_equals_vector(self, Float64Vector other)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)
    cpdef int8_t[::1] is_null_with_nan(self)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    cpdef list to_pylist(self)
    #cpdef double __getitem__(self, Py_ssize_t i)

    cpdef double sum(self)
    cpdef double min(self)
    cpdef double max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Float64Vector from_arrow(object array)
cdef Float64Vector from_dict(const int32_t[::1] codes, const double[::1] dictionary)
cdef Float64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const double[::1] dictionary,
    const uint8_t[::1] row_validity,
)
cdef Float64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=*,
    bint ordered=*,
    const uint8_t* dict_entry_null_bitmap=*,
)
cdef Float64Vector from_sequence(double[::1] data)
cdef Float64Vector from_rle_builder(
    double* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    uint8_t* null_bitmap=*,
)
cdef Float64Vector _materialize_rle_float64(Float64Vector rle_vec)
cdef Float64Vector _materialize_dict_float64(Float64Vector vec)
cdef Float64Vector make_float64_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
)
