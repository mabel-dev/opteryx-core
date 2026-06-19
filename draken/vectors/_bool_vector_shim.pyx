# cython: language_level=3
# Cython shim for draken.vectors.bool_vector — E.24 vtable bridge.

from cpython.object cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from libc.stdint cimport uint8_t, uint32_t
from libc.stddef cimport size_t
from libc.string cimport memset

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_BOOL
from draken.vectors.vector cimport Vector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    PyObject* draken_vector_own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type)

# C-level Py_DECREF: Cython 3 changed cpython.ref.Py_DECREF to take `object`,
# but we need to decrement a raw PyObject* without Cython's incref side-effect.
cdef extern from *:
    """static inline void _shim_decref(PyObject* op) { Py_DECREF(op); }"""
    void _shim_decref(PyObject* op)


cdef class BoolVector(Vector):
    def __cinit__(self, object nb_vector=None):
        if isinstance(nb_vector, int):
            from draken.draken_native import vector_from_bool_constant
            nb_vec = vector_from_bool_constant(False, nb_vector)
            self._nb = nb_vec
            self._dv = draken_vector_unwrap(<PyObject*>nb_vec)

    @classmethod
    def from_constant(cls, value, num_rows, is_null=False):
        from draken.draken_native import vector_from_bool_constant
        return cls(vector_from_bool_constant(None if is_null else bool(value), num_rows))

    def not_vector(self):
        return BoolVector(self._nb.bool_not())

    def and_vector(self, other):
        cdef object other_nb = (<BoolVector>other)._nb if isinstance(other, BoolVector) else other
        return BoolVector(self._nb.bool_and(other_nb))

    def or_vector(self, other):
        cdef object other_nb = (<BoolVector>other)._nb if isinstance(other, BoolVector) else other
        return BoolVector(self._nb.bool_or(other_nb))

    def xor_vector(self, other):
        # XOR = (A OR B) AND NOT (A AND B)
        cdef object other_nb = (<BoolVector>other)._nb if isinstance(other, BoolVector) else other
        cdef object a_or_b = self._nb.bool_or(other_nb)
        cdef object a_and_b = self._nb.bool_and(other_nb)
        return BoolVector(a_or_b.bool_and(a_and_b.bool_not()))

    def to_byte_array(self):
        """Pack the logical bool values into an LSB-first byte bitmask (one bit
        per logical row; NULL rows clear to 0). Fully native: reads the unified
        DrakenVector bitmap directly (``data[selection[i]]`` AND validity) in a
        nogil loop — no per-row Python objects, no to_pylist."""
        cdef const DrakenVector* dv = draken_vector_unwrap(<PyObject*>self._nb)
        cdef uint32_t n = dv.length
        cdef Py_ssize_t nbytes = (<Py_ssize_t>n + 7) >> 3
        cdef object out = PyBytes_FromStringAndSize(NULL, nbytes)
        if n == 0:
            return out
        cdef uint8_t* buf = <uint8_t*>PyBytes_AS_STRING(out)
        cdef const uint8_t* data = <const uint8_t*>dv.data
        cdef const uint32_t* sel = dv.selection
        cdef const uint8_t* validity = dv.validity
        cdef uint32_t i, code
        cdef uint8_t bit
        with nogil:
            memset(buf, 0, <size_t>nbytes)
            if validity == NULL:
                for i in range(n):
                    code = sel[i]
                    if (data[code >> 3] >> (code & 7)) & 1:
                        buf[i >> 3] |= <uint8_t>(1u << (i & 7))
            else:
                for i in range(n):
                    code = sel[i]
                    bit = <uint8_t>((data[code >> 3] >> (code & 7)) & 1)
                    bit &= <uint8_t>((validity[i >> 3] >> (i & 7)) & 1)
                    if bit:
                        buf[i >> 3] |= <uint8_t>(1u << (i & 7))
        return out

    def any(self):
        return self._nb.bool_any()

    def all(self):
        return self._nb.bool_all()

    def equals(self, value):
        from draken.draken_native import vector_from_bool_constant
        nb_const = vector_from_bool_constant(bool(value), self._nb.length)
        return BoolVector(self._nb.compare_vector(nb_const, 0))

    def not_equals(self, value):
        from draken.draken_native import vector_from_bool_constant
        nb_const = vector_from_bool_constant(bool(value), self._nb.length)
        return BoolVector(self._nb.compare_vector(nb_const, 1))

    def between(self, lower, upper, lower_inclusive=True, upper_inclusive=True):
        return BoolVector(self._nb.between(lower, upper, lower_inclusive, upper_inclusive))


cdef BoolVector from_decoded(void* data, uint8_t* null_bitmap, size_t length):
    cdef PyObject* raw = draken_vector_own_raw(data, null_bitmap, <uint32_t>length, DRAKEN_BOOL)
    if raw == NULL:
        raise MemoryError("draken_vector_own_raw failed for BoolVector")
    cdef BoolVector result = BoolVector.__new__(BoolVector)
    result._nb = <object>raw   # Cython incref → refcount = 2
    _shim_decref(raw)          # balance the NEW ref → refcount = 1
    result._dv = draken_vector_unwrap(raw)
    return result
