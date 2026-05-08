# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint32_t

from opteryx.third_party.pcg.pcg cimport oneseq_xsh_rs_32_16, static_arbitrary_seed

cdef oneseq_xsh_rs_32_16 _util_rng
_util_rng.seed(static_arbitrary_seed())

# default charset (same as _orso_utils)
cdef bytes DEFAULT_CHARSET = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

def random_string_c(int length, charset=None):
    """Generate a random string using PCG engine. Returns Python str."""
    if length <= 0:
        return ""

    cdef bytes cs
    cdef Py_ssize_t clen
    cdef char* buf
    cdef int i
    cdef uint32_t rv

    if charset is None:
        cs = DEFAULT_CHARSET
    else:
        if isinstance(charset, bytes):
            cs = charset
        else:
            cs = charset.encode('ascii')

    clen = len(cs)
    if clen == 0:
        raise ValueError("charset must not be empty")

    buf = <char*>malloc(length)
    if buf is NULL:
        raise MemoryError("Failed to allocate buffer")

    try:
        for i in range(length):
            rv = _util_rng()
            buf[i] = cs[rv % clen]
        pyb = PyBytes_FromStringAndSize(buf, length)
        return pyb.decode('ascii')
    finally:
        free(buf)
