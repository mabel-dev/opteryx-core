# cython: language_level=3

"""Cython wrapper for the C++ CompiledExpression arena.

Bind-time lowering pass: walks an opteryx Node tree and produces a flat C++
representation that the (forthcoming) nogil evaluator will consume. See
src/cpp/expression/compiled_expression.h for the on-disk contract.

This module currently exposes only what's needed for the Wedge C round-trip
test (CompiledExpressionHandle.node_type_walk). Wedge D will add the eval
entry point on top of the same arena.
"""

from libcpp.deque cimport deque
from libcpp.vector cimport vector


cdef extern from "expression/compiled_expression.h" namespace "opteryx_expr":
    cdef cppclass CompiledExpression:
        int node_type

    cdef cppclass CompiledExpressionArena:
        CompiledExpressionArena() except +
        CompiledExpression* lower(object py_node) except NULL
        object node_type_walk(CompiledExpression* root)
        size_t node_count() const


cdef class CompiledExpressionHandle:
    """Owns one CompiledExpressionArena and the root pointer into it.

    The arena outlives the handle and is deleted in __dealloc__. Children
    pointed at by the root remain valid for the handle's lifetime.
    """
    cdef CompiledExpressionArena* _arena
    cdef CompiledExpression* _root

    def __cinit__(self):
        self._arena = new CompiledExpressionArena()
        self._root = NULL

    def __dealloc__(self):
        if self._arena != NULL:
            del self._arena
            self._arena = NULL

    @property
    def node_count(self):
        return self._arena.node_count()

    def node_type_walk(self):
        """Return a list of (node_type, num_children) tuples in depth-first order.

        Used by the round-trip test to validate that the lowered tree mirrors
        the source Node tree exactly.
        """
        if self._root == NULL:
            return []
        return self._arena.node_type_walk(self._root)


def lower(node):
    """Lower an opteryx Node tree into a CompiledExpressionHandle.

    The source `node` may continue to be used after this call — the handle
    holds its own refs to the Python objects it cares about.
    """
    cdef CompiledExpressionHandle handle = CompiledExpressionHandle()
    handle._root = handle._arena.lower(node)
    return handle
