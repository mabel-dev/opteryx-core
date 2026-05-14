# cython: language_level=3

# Shared cdef extern declarations for the C++ CompiledExpression arena. The
# corresponding .pyx implements CompiledExpressionHandle; sibling Cython
# modules (evaluation.pyx) cimport this .pxd to walk the arena directly via
# CompiledExpression* pointers.
#
# PyObject* fields are owned by the C++ arena (held_refs_). Cython readers
# should cast with <object> to obtain a borrowed-then-incref'd Python handle,
# the transient refcount churn is harmless because the arena outlives the
# walker.

from cpython.ref cimport PyObject
from libcpp.vector cimport vector


cdef extern from "expression/compiled_expression.h" namespace "opteryx_expr":
    cdef cppclass CompiledExpression:
        int node_type
        PyObject* value
        PyObject* schema_column
        PyObject* source_node
        CompiledExpression* left
        CompiledExpression* right
        CompiledExpression* centre
        vector[CompiledExpression*] parameters

    cdef cppclass CompiledExpressionArena:
        CompiledExpressionArena() except +
        CompiledExpression* lower(object py_node) except NULL
        object node_type_walk(CompiledExpression* root)
        size_t node_count() const


cdef class CompiledExpressionHandle:
    cdef CompiledExpressionArena* _arena
    cdef CompiledExpression* _root

    cdef CompiledExpression* root(self) noexcept
