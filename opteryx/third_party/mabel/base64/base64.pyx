# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

cdef extern from "_base64.h":
    void* b64tobin_len(void* dest, const char* src, size_t len)
    char* bintob64(char* dest, const void* src, size_t size)


cdef inline size_t calc_encoded_size(size_t length):
    return ((length + 2) // 3) * 4


cpdef bytes encode(bytes data):
    cdef size_t in_len = len(data)
    cdef size_t out_len = calc_encoded_size(in_len)

    cdef char* outbuf = <char*>malloc(out_len + 1)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    bintob64(outbuf, <void*>input_ptr, in_len)

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, out_len)
    free(outbuf)
    return result


cpdef bytes decode(bytes data):
    cdef size_t in_len = len(data)
    cdef size_t out_len = (in_len // 4) * 3

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* inbuf = PyBytes_AsString(data)
    cdef char* end_ptr = <char*>b64tobin_len(outbuf, inbuf, in_len)

    cdef size_t decoded_len = 0
    if end_ptr != NULL and end_ptr >= outbuf and end_ptr <= outbuf + out_len:
        decoded_len = end_ptr - outbuf

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, decoded_len)
    free(outbuf)
    return result
