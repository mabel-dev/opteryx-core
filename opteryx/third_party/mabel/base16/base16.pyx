# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString
from cpython.ref cimport Py_DECREF

from opteryx.third_party.mabel.base16.base16 cimport b16tobin_len, bintob16, b16_encoded_size, b16_decoded_size


cdef inline size_t calc_encoded_size(size_t length):
    """Base16-encoded output length."""
    return length * 2


cdef inline size_t calc_decoded_size(size_t length):
    """Worst-case decoded output size."""
    return length // 2


cdef size_t _encode_impl(char* outbuf, const char* inbuf, size_t in_len):
    """Encode implementation - pure C, no Python objects."""
    bintob16(outbuf, <void*>inbuf, in_len)
    return in_len * 2


cdef size_t _decode_impl(char* outbuf, const char* inbuf, size_t in_len):
    """Decode implementation - pure C, no Python objects. Returns decoded length."""
    cdef char* end_ptr = <char*>b16tobin_len(outbuf, inbuf, in_len)
    if end_ptr == NULL or end_ptr < outbuf:
        return 0
    return end_ptr - outbuf


cpdef bytes encode(bytes data):
    """
    Base16-encode a bytes object using bintob16 from C.
    Returns: encoded bytes (upper-case hex).

    Parameters
    ----------
    data : bytes
        Input bytes to encode

    Returns
    -------
    bytes
        Hex-encoded uppercase string
    """
    cdef size_t in_len = len(data)
    cdef size_t out_len = calc_encoded_size(in_len)

    cdef char* outbuf = <char*>malloc(out_len + 1)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    _encode_impl(outbuf, input_ptr, in_len)

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, out_len)
    free(outbuf)
    return result


cpdef bytes decode(bytes data):
    """
    Base16-decode a bytes object. Prefer Python's bytes.fromhex for correctness.
    Falls back to the C implementation on exception.
    """
    try:
        # data may be bytes; bytes.fromhex expects str, so decode as ASCII
        s = data.decode('ascii')
        return bytes.fromhex(s)
    except Exception:
        pass

    # Fallback to C implementation
    cdef size_t in_len = len(data)
    cdef size_t out_len = calc_decoded_size(in_len)

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* inbuf = PyBytes_AsString(data)
    cdef size_t decoded_len = _decode_impl(outbuf, inbuf, in_len)

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, decoded_len)
    free(outbuf)

    return result


cpdef bint has_scalar():
    """
    Check if scalar implementation is available.
    Always returns True for base16.
    """
    return True
