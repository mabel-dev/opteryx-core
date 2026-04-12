# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=False
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint32_t
from libc.math cimport isnan, isinf, isfinite

from opteryx.compiled.draken.vectors.string_vector cimport StringVector, StringVectorBuilder


cdef extern from "ryu.h":
    int d2fixed_buffered_n(double d, uint32_t precision, char* result)

cdef char ZERO = 48  # or ord('0')
cdef char DOT = 46   # or ord('.')

# Define safe limits for double values to be processed by ryu
cdef double MAX_SAFE_DOUBLE = 9.9e+24
cdef double MIN_SAFE_DOUBLE = -9.9e+24

cdef inline int trim_trailing_zeros(char* buf, int length) nogil:
    cdef int i = length - 1
    # Strip trailing zeros
    while i > 0 and buf[i] == ZERO:
        i -= 1
    # Ensure there's at least one digit after the decimal
    if buf[i] == DOT:
        i += 1
        buf[i] = ZERO
    return i + 1  # New length

cdef inline bint is_safe_double(double d) nogil:
    """Check if a double value is safe to pass to ryu"""
    return (isfinite(d) and
            d <= MAX_SAFE_DOUBLE and
            d >= MIN_SAFE_DOUBLE)

cdef inline bytes safe_double_to_bytes(double d, uint32_t precision):
    """Safely convert a double to bytes, handling extreme values"""
    cdef char buf[32]
    cdef int length

    if not is_safe_double(d):
        if isnan(d):
            return b"NaN"
        elif isinf(d):
            if d > 0:
                return b"Infinity"
            else:
                return b"-Infinity"
        else:
            # For extreme finite values, fall back to Python string conversion
            return str(d).encode('ascii')

    length = d2fixed_buffered_n(d, precision, buf)
    length = trim_trailing_zeros(buf, length)
    return <bytes>buf[:length]

cpdef StringVector format_double_array_bytes(const double[::1] arr, uint32_t precision=6):
    """
    Convert an array of float64s to a StringVector of bytes.

    Args:
        arr: C-contiguous memoryview of float64 values
        precision: Decimal precision for formatting

    Returns:
        StringVector containing formatted byte strings
    """
    cdef:
        Py_ssize_t i, n = arr.shape[0]
        StringVectorBuilder builder = StringVectorBuilder(n)
        bytes formatted
        char* c_str
        Py_ssize_t byte_len

    for i in range(n):
        formatted = safe_double_to_bytes(arr[i], precision)
        c_str = formatted
        byte_len = len(formatted)
        builder.append_bytes(c_str, byte_len)

    return builder.finish()

cpdef StringVector format_double_array_ascii(const double[::1] arr, uint32_t precision=6):
    """
    Convert an array of float64s to a StringVector of Python strings (ASCII encoded as bytes internally).

    Args:
        arr: C-contiguous memoryview of float64 values
        precision: Decimal precision for formatting

    Returns:
        StringVector containing formatted ASCII strings
    """
    cdef:
        Py_ssize_t i, n = arr.shape[0]
        StringVectorBuilder builder = StringVectorBuilder(n)
        bytes formatted
        char* c_str
        Py_ssize_t byte_len

    for i in range(n):
        formatted = safe_double_to_bytes(arr[i], precision)
        # For ASCII, the bytes are already the ASCII representation
        c_str = formatted
        byte_len = len(formatted)
        builder.append_bytes(c_str, byte_len)

    return builder.finish()
