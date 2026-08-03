# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

cdef extern from "_base85.h":
    void* b85tobin_len(void* dest, const char* src, size_t len)
    char* bintob85(char* dest, const void* src, size_t size)


cpdef bytes encode(bytes data):
    cdef size_t in_len = len(data)
    # Buffer must be sized to what bintob85 WRITES, not to what it returns.
    # The tail block zero-pads its final group to 4 bytes and unconditionally
    # writes all 5 output chars, then advances the cursor by only
    # (remaining + 1). So the write extent is always a whole number of 5-char
    # groups -- ((in_len + 3) // 4) * 5 -- while the returned length is the
    # shorter (in_len * 5 + 3) // 4. Sizing to the returned length overruns the
    # allocation by 1-3 bytes for every in_len not a multiple of 4.
    # No terminator is written, so no +1 is required.
    cdef size_t out_len = ((in_len + 3) // 4) * 5

    if in_len == 0:
        return b""

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    cdef char* end_ptr = bintob85(outbuf, <void*>input_ptr, in_len)

    if end_ptr == NULL:
        free(outbuf)
        raise RuntimeError("Base85 encoding failed")

    cdef size_t encoded_len = end_ptr - outbuf
    cdef bytes result = PyBytes_FromStringAndSize(outbuf, encoded_len)
    free(outbuf)
    return result


cpdef bytes decode(bytes data):
    cdef size_t in_len = len(data)
    cdef size_t out_len = (in_len // 5) * 4 + 4

    if in_len == 0:
        return b""

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* inbuf = PyBytes_AsString(data)
    cdef char* end_ptr = <char*>b85tobin_len(outbuf, inbuf, in_len)

    cdef size_t decoded_len = 0
    if end_ptr != NULL and end_ptr >= outbuf and end_ptr <= outbuf + out_len:
        decoded_len = end_ptr - outbuf

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, decoded_len)
    free(outbuf)
    return result
