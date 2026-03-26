from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector


cdef class VectorVector(ArrayVector):
    cdef Py_ssize_t _dimensions


cdef VectorVector from_arrow(object array)
