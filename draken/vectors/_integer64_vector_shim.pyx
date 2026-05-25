# cython: language_level=3
# Cython shim for draken.vectors.integer64_vector — E.24 vtable bridge.

from cpython.object cimport PyObject
from libc.stdint cimport int64_t, uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_INT64
from draken.vectors.vector cimport Vector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    PyObject* draken_vector_own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type)

cdef extern from *:
    """static inline void _shim_decref(PyObject* op) { Py_DECREF(op); }"""
    void _shim_decref(PyObject* op)


cdef class Integer64Vector(Vector):
    @classmethod
    def from_constant(cls, value, num_rows, is_null=False):
        from draken.draken_native import vector_from_constant
        return cls(vector_from_constant(None if is_null else int(value), num_rows))

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        return self._dv.validity


cdef Integer64Vector from_decoded(void* data, uint8_t* null_bitmap, size_t length):
    cdef PyObject* raw = draken_vector_own_raw(data, null_bitmap, <uint32_t>length, DRAKEN_INT64)
    if raw == NULL:
        raise MemoryError("draken_vector_own_raw failed for Integer64Vector")
    cdef Integer64Vector result = Integer64Vector.__new__(Integer64Vector)
    result._nb = <object>raw
    _shim_decref(raw)
    result._dv = draken_vector_unwrap(raw)
    return result


cdef Integer64Vector from_sequence(const int64_t[::1] data):
    from draken.draken_native import vector_from_sequence
    cdef Integer64Vector result = Integer64Vector.__new__(Integer64Vector)
    nb_vec = vector_from_sequence(list(data))
    result._nb = nb_vec
    result._dv = draken_vector_unwrap(<PyObject*>nb_vec)
    return result


cdef Integer64Vector make_int64_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    raise NotImplementedError("make_int64_dict_only not implemented in E.24 shim")


cdef Integer64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dict_ptr,
    Py_ssize_t dict_size,
    const uint8_t* null_bitmap=NULL,
):
    from draken.draken_native import vector_from_sequence
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
        rows.append(<object>dict_ptr[code])

    if null_bitmap != NULL:
        for i in range(row_count):
            if not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                rows[i] = None

    return Integer64Vector(vector_from_sequence(rows))


cdef Integer64Vector from_dict(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
):
    raise NotImplementedError("Integer64Vector.from_dict not implemented in E.24 shim")


cdef Integer64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] validity,
):
    raise NotImplementedError("Integer64Vector.from_dict_nullable not implemented in E.24 shim")
