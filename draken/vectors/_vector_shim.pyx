# cython: language_level=3
# Cython shim for draken.vectors.vector — provides __pyx_vtable__ for cimport consumers.
# E.24: _nb holds the nanobind handle; _dv is a borrowed pointer into it.

from cpython.object cimport PyObject
from libc.stdint cimport int32_t, uint8_t, uint64_t

from draken.core.buffers cimport DrakenVector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)


cdef class Vector:
    def __cinit__(self, object nb_vector=None):
        if nb_vector is None or isinstance(nb_vector, int):
            # int form: BoolVector(n) compat — subclass __cinit__ sets _nb/_dv
            self._nb = None
            self._dv = NULL
        else:
            self._nb = nb_vector
            self._dv = draken_vector_unwrap(<PyObject*>nb_vector)

    @property
    def length(self):
        return self._nb.length

    @property
    def type(self):
        return self._nb.type

    @property
    def data_length(self):
        return self._nb.data_length

    def __len__(self):
        return self._nb.length

    def __getitem__(self, int idx):
        return self._nb[idx]

    def take(self, indices):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.take(list(indices)))

    def _compare_scalar(self, value, int op):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.compare_scalar(value, op))

    def _compare_vector(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def equals_vector(self, other):
        return self._compare_vector(other, 0)

    def not_equals_vector(self, other):
        return self._compare_vector(other, 1)

    def greater_than_vector(self, other):
        return self._compare_vector(other, 2)

    def greater_than_or_equals_vector(self, other):
        return self._compare_vector(other, 3)

    def less_than_vector(self, other):
        return self._compare_vector(other, 4)

    def less_than_or_equals_vector(self, other):
        return self._compare_vector(other, 5)

    def _compare_float64_vector(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def _compare_vector_op(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def between(self, lower, upper, lower_inclusive=True, upper_inclusive=True):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.between(lower, upper, lower_inclusive, upper_inclusive))

    def in_list(self, values):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.in_list(values))

    def hash(self):
        return self._nb.hash()

    def sum(self):
        return self._nb.sum()

    def min(self):
        return self._nb.min()

    def max(self):
        return self._nb.max()

    def null_bitmap(self):
        if self._dv == NULL:
            return None
        if self._dv.validity == NULL:
            return None
        cdef Py_ssize_t n_bytes = (self._dv.length + 7) // 8
        return bytes((<uint8_t*>self._dv.validity)[:n_bytes])

    def is_null(self):
        cdef list vals = self._nb.to_pylist()
        cdef Py_ssize_t n = len(vals)
        cdef bytearray result = bytearray(n)
        cdef Py_ssize_t i
        for i in range(n):
            if vals[i] is None:
                result[i] = 1
        return result

    def to_pylist(self):
        return self._nb.to_pylist()

    def materialize(self):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.materialize())

    def compress(self):
        # sort.pyx expects int64_t[::1] memoryview — sortable int64 keys.
        # For E.24 shim: convert to int64 sort keys via to_pylist().
        import struct
        from array import array as _array
        vals = self._nb.to_pylist()
        type_name = self._nb.type.name
        keys = []
        if type_name in ("FLOAT32", "FLOAT64"):
            for v in vals:
                if v is None:
                    keys.append(-0x8000000000000000)
                else:
                    # IEEE 754 bit cast to sortable int64
                    bits = struct.unpack('Q', struct.pack('d', float(v)))[0]
                    if bits & 0x8000000000000000:
                        bits ^= 0xFFFFFFFFFFFFFFFF
                    keys.append(bits & 0x7FFFFFFFFFFFFFFF)
        else:
            for v in vals:
                if v is None:
                    keys.append(-0x8000000000000000)
                elif isinstance(v, bool):
                    keys.append(1 if v else 0)
                else:
                    keys.append(int(v))
        return _array('q', keys)

    cdef DrakenVector* unified(self) noexcept:
        return <DrakenVector*>self._dv

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        return self._dv.validity

    cdef bint c_hash_single(self, uint64_t* out, int32_t n) except -1 nogil:
        cdef Py_ssize_t i
        with gil:
            hashes = self._nb.hash()
            for i in range(n):
                out[i] = <uint64_t>hashes[i]
        return 0
