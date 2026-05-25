# cython: language_level=3
# Cython shim for draken.vectors.float32_vector — E.24 vtable bridge.

from cpython.object cimport PyObject
from libc.stdint cimport uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_FLOAT32
from draken.vectors.vector cimport Vector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    PyObject* draken_vector_own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type)

cdef extern from *:
    """static inline void _shim_decref(PyObject* op) { Py_DECREF(op); }"""
    void _shim_decref(PyObject* op)


cdef class Float32Vector(Vector):
    pass


cdef Float32Vector from_decoded(void* data, uint8_t* null_bitmap, size_t length):
    cdef PyObject* raw = draken_vector_own_raw(data, null_bitmap, <uint32_t>length, DRAKEN_FLOAT32)
    if raw == NULL:
        raise MemoryError("draken_vector_own_raw failed for Float32Vector")
    cdef Float32Vector result = Float32Vector.__new__(Float32Vector)
    result._nb = <object>raw
    _shim_decref(raw)
    result._dv = draken_vector_unwrap(raw)
    return result


cdef Float32Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const float* dict_ptr,
    Py_ssize_t dict_size,
    const uint8_t* null_bitmap=NULL,
):
    from draken.draken_native import vector_float32_from_sequence
    cdef list rows = []
    cdef Py_ssize_t i, code
    cdef uint8_t byte_val

    for i in range(row_count):
        if code_width == 1:
            code = codes[i]
        elif code_width == 2:
            code = codes[2 * i] | (codes[2 * i + 1] << 8)
        else:  # code_width == 4
            code = (codes[4 * i] | (codes[4 * i + 1] << 8) |
                    (codes[4 * i + 2] << 16) | (codes[4 * i + 3] << 24))
        rows.append(<float>dict_ptr[code])

    if null_bitmap != NULL:
        for i in range(row_count):
            if not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                rows[i] = None

    return Float32Vector(vector_float32_from_sequence(rows))
