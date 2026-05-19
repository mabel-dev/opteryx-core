from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint32_t, uint64_t, uint8_t

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector

cdef class Float32Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef DrakenVarBuffer* _dict_values
    cdef uint8_t _dict_ordered
    cdef uint8_t* null_bitmap_ptr(self) noexcept
    cdef DrakenVector* unified(self) noexcept
    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n)

    cpdef Float32Vector take(self, int32_t[::1] indices)
    cdef BoolVector _compare_scalar(self, float value, int op)
    cdef BoolVector _compare_vector(self, Float32Vector other, int op)

    cpdef BoolVector equals(self, float value)
    cpdef BoolVector equals_vector(self, Float32Vector other)
    cpdef BoolVector not_equals(self, float value)
    cpdef BoolVector not_equals_vector(self, Float32Vector other)
    cpdef BoolVector greater_than(self, float value)
    cpdef BoolVector greater_than_vector(self, Float32Vector other)
    cpdef BoolVector greater_than_or_equals(self, float value)
    cpdef BoolVector greater_than_or_equals_vector(self, Float32Vector other)
    cpdef BoolVector less_than(self, float value)
    cpdef BoolVector less_than_vector(self, Float32Vector other)
    cpdef BoolVector less_than_or_equals(self, float value)
    cpdef BoolVector less_than_or_equals_vector(self, Float32Vector other)
    cpdef BoolVector between(self, float lower, float upper,
                              bint lower_inclusive=*, bint upper_inclusive=*)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)
    cpdef int8_t[::1] is_null_with_nan(self)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    cpdef list to_pylist(self)

    cpdef float sum(self)
    cpdef float min(self)
    cpdef float max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Float32Vector from_dict(const int32_t[::1] codes, const float[::1] dictionary)
cdef Float32Vector from_dict_nullable(
    const int32_t[::1] codes,
    const float[::1] dictionary,
    const uint8_t[::1] row_validity,
)
cdef Float32Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const float* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=*,
    bint ordered=*,
    const uint8_t* dict_entry_null_bitmap=*,
)
cdef Float32Vector from_sequence(float[::1] data)
cdef Float32Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
)
cdef Float32Vector from_arrow(object array)
cdef Float32Vector _materialize_dict_float32(Float32Vector vec)
cdef Float32Vector make_float32_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const float* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
)
