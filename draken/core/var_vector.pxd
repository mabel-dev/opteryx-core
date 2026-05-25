# Stub .pxd for draken.core.var_vector.
# Re-exports alloc_var_buffer / free_var_buffer for consumer compatibility.
# alloc_var_buffer is implemented inline in draken.core.buffers.

from libc.stddef cimport size_t
from libc.stdlib cimport free

from draken.core.buffers cimport DrakenVarBuffer, DrakenType, alloc_var_buffer


cdef inline void free_var_buffer(DrakenVarBuffer* buf, bint owns_data=True):
    if buf == NULL:
        return
    if buf.data != NULL:
        free(buf.data)
    if buf.offsets != NULL:
        free(buf.offsets)
    if buf.null_bitmap != NULL:
        free(buf.null_bitmap)
    free(buf)
