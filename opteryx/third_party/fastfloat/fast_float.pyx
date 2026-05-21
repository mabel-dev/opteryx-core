# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# distutils: language = c++
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_AsUTF8String
from libc.stdlib cimport malloc

from draken.vectors.float64_vector cimport Float64Vector, from_decoded as float64_from_decoded


cdef extern from "fast_float.h" namespace "fast_float":
    cdef cppclass from_chars_result:
        const char* ptr

    from_chars_result from_chars(
        const char* first,
        const char* last,
        double& value
    )

cdef inline double c_parse_fast_float(bytes bts):
    cdef const char* s = bts
    cdef double value = 0.0
    cdef Py_ssize_t n = len(bts)

    cdef from_chars_result res = from_chars(s, s + n, value)

    if res.ptr != NULL:
        return value
    else:
        raise ValueError(f"Could not parse float from {bts!r}")

cpdef double parse_fast_float(bytes bts):
    return c_parse_fast_float(bts)

cpdef Float64Vector parse_ascii_array_to_double(object arr):
    """
    Parse an array of Python strings (str) to Float64Vector using fast_float.
    Assumes ASCII input.

    Args:
        arr: Sequence of str or None values

    Returns:
        Float64Vector of parsed double values (NaN for unparseable or None inputs)
    """
    cdef Py_ssize_t i, n = len(arr)
    cdef double* out
    cdef bytes encoded
    cdef const char* c_str
    cdef Py_ssize_t length
    cdef double val = 0.0
    cdef from_chars_result res
    cdef object item

    if n == 0:
        return float64_from_decoded(NULL, NULL, 0)

    out = <double*>malloc(n * sizeof(double))
    if out == NULL:
        raise MemoryError(f"Cannot allocate buffer for {n} doubles")

    for i in range(n):
        item = arr[i]
        if item is None:
            out[i] = float('nan')
            continue

        # Convert str to bytes (UTF-8 encoded, ideally ASCII)
        encoded = PyUnicode_AsUTF8String(item)
        c_str = PyBytes_AS_STRING(encoded)
        length = PyBytes_GET_SIZE(encoded)

        res = from_chars(c_str, c_str + length, val)
        if res.ptr != NULL:
            out[i] = val
        else:
            out[i] = float('nan')

    # Transfer ownership of the malloc'd buffer to the Float64Vector.
    return float64_from_decoded(<void*>out, NULL, n)


cpdef Float64Vector parse_byte_array_to_double(object arr):
    """
    Parse an array of Python bytes (b"123.45") to Float64Vector using fast_float.

    Args:
        arr: Sequence of bytes or None values

    Returns:
        Float64Vector of parsed double values (NaN for unparseable or None inputs)
    """
    cdef Py_ssize_t i, n = len(arr)
    cdef double* out
    cdef const char* c_str
    cdef Py_ssize_t length
    cdef double val = 0.0
    cdef from_chars_result res
    cdef object item

    if n == 0:
        return float64_from_decoded(NULL, NULL, 0)

    out = <double*>malloc(n * sizeof(double))
    if out == NULL:
        raise MemoryError(f"Cannot allocate buffer for {n} doubles")

    for i in range(n):
        item = arr[i]
        if item is None:
            out[i] = float('nan')
            continue

        c_str = PyBytes_AS_STRING(item)
        length = PyBytes_GET_SIZE(item)

        res = from_chars(c_str, c_str + length, val)
        if res.ptr != NULL:
            out[i] = val
        else:
            out[i] = float('nan')

    # Transfer ownership of the malloc'd buffer to the Float64Vector.
    return float64_from_decoded(<void*>out, NULL, n)
