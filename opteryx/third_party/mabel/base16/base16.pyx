# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

from opteryx.third_party.mabel.base16.base16 cimport b16tobin_len, bintob16


cpdef bytes encode(bytes data):
    cdef size_t in_len = len(data)
    cdef size_t out_len = in_len * 2

    cdef char* outbuf = <char*>malloc(out_len + 1)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    bintob16(outbuf, <void*>input_ptr, in_len)

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, out_len)
    free(outbuf)
    return result


cpdef bytes decode(bytes data):
    cdef size_t in_len = len(data)
    cdef size_t out_len = in_len // 2

    if in_len == 0:
        return b""

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* inbuf = PyBytes_AsString(data)
    cdef char* end_ptr = <char*>b16tobin_len(outbuf, inbuf, in_len)

    cdef size_t decoded_len = 0
    if end_ptr != NULL and end_ptr >= outbuf and end_ptr <= outbuf + out_len:
        decoded_len = end_ptr - outbuf

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, decoded_len)
    free(outbuf)
    return result
