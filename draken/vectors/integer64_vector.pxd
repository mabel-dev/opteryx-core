from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint32_t, uint64_t, uint8_t

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.float64_vector cimport Float64Vector

cdef class Integer64Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef bint _owns_dict_data
    cdef bint _owns_selection

    cpdef Integer64Vector take(self, int32_t[::1] indices)
    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n)
    cdef DrakenVector* unified(self) noexcept
    # Integer-dispatched kernels. cpdef so the expression evaluator can call
    # them directly from Python and bypass the per-op named wrappers below.
    cpdef BoolVector _compare_scalar(self, int64_t value, int op)
    cpdef BoolVector _compare_vector(self, Integer64Vector other, int op)
    cpdef BoolVector _compare_float64_vector(self, object other, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector equals_vector(self, Integer64Vector other)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector not_equals_vector(self, Integer64Vector other)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_vector(self, Integer64Vector other)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector greater_than_or_equals_vector(self, Integer64Vector other)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_vector(self, Integer64Vector other)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than_or_equals_vector(self, Integer64Vector other)
    cpdef BoolVector equals_float64_vector(self, object other)
    cpdef BoolVector not_equals_float64_vector(self, object other)
    cpdef BoolVector greater_than_float64_vector(self, object other)
    cpdef BoolVector greater_than_or_equals_float64_vector(self, object other)
    cpdef BoolVector less_than_float64_vector(self, object other)
    cpdef BoolVector less_than_or_equals_float64_vector(self, object other)
    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=*, bint upper_inclusive=*)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    cpdef list to_pylist(self)
    cpdef Float64Vector to_float64_vector(self)
    #cpdef int64_t __getitem__(self, Py_ssize_t i)

    cpdef int64_t sum(self)
    cpdef int64_t min(self)
    cpdef int64_t max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Integer64Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
)
cdef Integer64Vector from_arrow(object array)
cdef Integer64Vector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary)
cdef Integer64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
cdef Integer64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=*,
    bint ordered=*,
    const uint8_t* dict_entry_null_bitmap=*,
)
cdef Integer64Vector from_sequence(const int64_t[::1] data)
cdef Integer64Vector _materialize_dict_int64(Integer64Vector vec)
cdef Integer64Vector make_int64_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
)
