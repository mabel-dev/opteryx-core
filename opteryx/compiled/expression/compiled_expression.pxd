# cython: language_level=3

# Shared cdef extern declarations for the C++ CompiledExpression arena, plus
# the typed bytecode container produced by build_bytecode() and consumed by
# execute_bytecode() in the evaluator package.
#
# All bytecode hot-path types are C structs or typed cdef class instances —
# no `object` fields, no Python list iteration, no method dispatch through
# PyObject. See CLAUDE.md §2/§3.

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


# ---------------------------------------------------------------------------
# Bytecode VM data layout
#
# Opcodes are dense small ints so the executor's `if/elif` chain folds to a
# C jump table under optimize.use_switch=True. PyObject* slots hold borrowed
# pointers; the owning strong reference lives in CompiledBytecode._held_refs.
# The executor never touches refcounts — it casts to typed cdef classes
# (Vector, BoolVector, Morsel, bytes, str) and dispatches into typed cpdef
# methods directly.
# ---------------------------------------------------------------------------

# Opcode values — keep in sync with the executor switch in evaluation.pyx.
cdef enum BCOpcode:
    BC_LOAD_COL          = 1
    BC_LOAD_LIT_BOOL     = 2
    BC_LOAD_LIT_SCALAR   = 3
    BC_LOAD_LIT_SET      = 4
    BC_AND               = 5
    BC_OR                = 6
    BC_XOR               = 7
    BC_NOT               = 8
    BC_DNF               = 9
    BC_CNF               = 10
    BC_COMPARE           = 11
    BC_BETWEEN           = 12
    BC_BINARY_OP         = 13
    BC_UNARY_OP          = 14
    BC_FUNCTION          = 15
    BC_EXTRACTION        = 16
    BC_CAST              = 17
    BC_LEGACY            = 99


# Compare-time flag bits (instr.flags).
cdef enum BCCompareFlag:
    BC_CMP_LEFT_TEMPORAL  = 1
    BC_CMP_RIGHT_TEMPORAL = 2


ctypedef struct BytecodeInstr:
    int opcode               # BCOpcode
    int arity                # for BC_DNF / BC_CNF / BC_FUNCTION
    int op_code              # OP_EQ / OP_GT / ... for BC_COMPARE; lower_incl for BC_BETWEEN
    int flags                # bitfield of BCCompareFlag; upper_incl for BC_BETWEEN
    int bool_value           # 0/1 for BC_LOAD_LIT_BOOL
    PyObject* literal_obj    # for BC_LOAD_LIT_SCALAR / BC_LOAD_LIT_SET; lower bound for BC_BETWEEN; key for BC_EXTRACTION
    PyObject* literal_obj2   # upper bound scalar for BC_BETWEEN
    PyObject* compare_op_str # for BC_COMPARE; op string for BC_BINARY_OP / BC_UNARY_OP / BC_EXTRACTION
    PyObject* left_orso_type # for BC_COMPARE / BC_BINARY_OP — OrsoTypes enum or Py_None
    PyObject* right_orso_type
    PyObject* column_identity # for BC_LOAD_COL — bytes
    PyObject* column_name     # for BC_LOAD_COL — bytes
    PyObject* source_node     # for BC_LEGACY — Node
    PyObject* callable_ref   # for BC_FUNCTION — kernel callable


cdef class CompiledBytecode:
    cdef BytecodeInstr* instrs
    cdef Py_ssize_t count
    cdef Py_ssize_t capacity
    cdef Py_ssize_t max_stack_depth
    # _held_refs keeps every PyObject* that any instruction points at alive
    # for the bytecode's lifetime. The executor never touches this list; it
    # exists purely for refcount/lifetime correctness.
    cdef list _held_refs
    # True if every opcode in this bytecode is GIL-free (only bool algebra
    # and column loads — no comparisons, functions, or legacy nodes).
    # Enables the raw-bitmap nogil execution path in evaluate_bitmap().
    cdef bint is_pure_bitmap

    cdef BytecodeInstr* _push_instr(self) except NULL
    cdef inline void _hold(self, object obj)
