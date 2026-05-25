# Stub .pxd for draken.core.fixed_vector.
# Provides alloc_fixed_buffer / free_fixed_buffer for consumer compatibility.

from libc.stdint cimport uint8_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType


cdef inline DrakenFixedBuffer* alloc_fixed_buffer(
    DrakenType dtype, size_t length, size_t itemsize
):
    cdef DrakenFixedBuffer* buf = <DrakenFixedBuffer*>malloc(sizeof(DrakenFixedBuffer))
    if buf == NULL:
        raise MemoryError()
    buf.data = malloc(length * itemsize)
    if buf.data == NULL:
        free(buf)
        raise MemoryError()
    buf.null_bitmap = NULL
    buf.length = length
    buf.itemsize = itemsize
    buf.type = dtype
    return buf


cdef inline void free_fixed_buffer(DrakenFixedBuffer* buf, bint owns_data=True):
    if buf == NULL:
        return
    if buf.data != NULL:
        free(buf.data)
    if buf.null_bitmap != NULL:
        free(buf.null_bitmap)
    free(buf)
