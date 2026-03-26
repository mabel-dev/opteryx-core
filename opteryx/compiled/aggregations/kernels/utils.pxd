# cython: language_level=3
#
# Shared inline helpers for aggregate kernel files.
# These are duplicated here (rather than cimported from group_by_engine.pyx)
# because kernel files are separate compilation units and must be self-contained.

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer


cdef inline bint _bitmap_is_valid(const uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline int64_t _read_integer_value(DrakenFixedBuffer* ptr, Py_ssize_t index) noexcept nogil:
    if ptr.itemsize == 1:
        return (<char*> ptr.data)[index]
    if ptr.itemsize == 2:
        return (<short*> ptr.data)[index]
    if ptr.itemsize == 4:
        return (<int*> ptr.data)[index]
    return (<int64_t*> ptr.data)[index]
