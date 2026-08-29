# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint32_t

from opteryx.third_party.pcg.pcg cimport nondeterministic_seed, oneseq_xsh_rs_32_16

# Seeded from OS entropy once per process at import. NOT fork-safe: a process
# that forks after import copies this state into every child, so ids that must
# never collide across processes cannot come from this RNG - use
# opteryx.utils.unique_id (structural time/mac/pid uniqueness) for those.
cdef oneseq_xsh_rs_32_16 _util_rng
_util_rng.seed(nondeterministic_seed())

# default charset (same as _sql_utils)
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
