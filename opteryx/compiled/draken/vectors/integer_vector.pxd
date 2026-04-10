from libc.stdint cimport int8_t, int32_t, int64_t, uint64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer, DrakenType
from opteryx.compiled.draken.vectors.vector cimport Vector

cdef class IntegerVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef ConstAccessor _const_accessor
    cdef int64_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null

    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef list to_pylist(self)
    cpdef IntegerVector take(self, int32_t[::1] indices)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef IntegerVector from_arrow(object array)
cdef IntegerVector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary)
cdef IntegerVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
