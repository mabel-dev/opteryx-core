# cython: language_level=3

# Shared cdef extern declarations for the C++ CompiledExpression arena, plus
# the typed bytecode container produced by build_bytecode() and consumed by
# execute_bytecode() in the evaluator package.
#
# All bytecode hot-path types are C structs or typed cdef class instances —
# no `object` fields, no Python list iteration, no method dispatch through
# PyObject. See CLAUDE.md §2/§3.

from cpython.ref cimport PyObject
from libc.stdint cimport int16_t
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

# ---------------------------------------------------------------------------
# Type codes for left_type_code / right_type_code in BytecodeInstr.
# Only temporal types need to be distinguished at execution time; all other
# types are handled by vector-type dispatch inside the kernels.
# ---------------------------------------------------------------------------
cdef enum BCTypeCode:
    BC_TYPE_NONE      = 0   # no type / not a temporal type
    BC_TYPE_DATE      = 1   # SqlType.DATE
    BC_TYPE_TIMESTAMP = 2   # SqlType.TIMESTAMP


# ---------------------------------------------------------------------------
# Binary operator codes for BytecodeInstr.op_code (BC_BINARY_OP instructions).
# ---------------------------------------------------------------------------
cdef enum BCBinaryOpCode:
    BOP_UNKNOWN       = 0
    BOP_PLUS          = 1
    BOP_MINUS         = 2
    BOP_MULTIPLY      = 3
    BOP_DIVIDE        = 4
    BOP_MODULO        = 5
    BOP_INT_DIVIDE    = 6
    BOP_STRING_CONCAT = 7
    BOP_BITWISE_OR    = 8
    BOP_BITWISE_AND   = 9
    BOP_BITWISE_XOR   = 10
    BOP_SHIFT_LEFT    = 11
    BOP_SHIFT_RIGHT   = 12


# ---------------------------------------------------------------------------
# Unary operator codes for BytecodeInstr.op_code (BC_UNARY_OP instructions).
# ---------------------------------------------------------------------------
cdef enum BCUnaryOpCode:
    UOP_UNKNOWN      = 0
    UOP_IS_NULL      = 1
    UOP_IS_NOT_NULL  = 2
    UOP_IS_EMPTY     = 3
    UOP_IS_NOT_EMPTY = 4
    UOP_BITWISE_NOT  = 5
    UOP_IS_TRUE      = 6
    UOP_IS_NOT_FALSE = 7
    UOP_IS_FALSE     = 8
    UOP_IS_NOT_TRUE  = 9


# ---------------------------------------------------------------------------
# Extraction operator codes for BytecodeInstr.op_code (BC_EXTRACTION instructions).
# ---------------------------------------------------------------------------
cdef enum BCExtractionOpCode:
    BC_EXTR_UNKNOWN     = 0
    BC_EXTR_MAP_STRING  = 1   # vector_map_access_string(vec, key_vec_int64)
    BC_EXTR_MAP_ARRAY   = 2   # vector_array_map_access(vec, key_int64)
    BC_EXTR_JSON_PTR    = 3   # vector_json_extract(vec, key_bytes)  ['->']
    BC_EXTR_JSON_KEY    = 4   # vector_json_extract(vec, key_bytes)  ['->>']


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
    BC_CASE              = 18


# Compare-time flag bits (instr.flags).
cdef enum BCCompareFlag:
    BC_CMP_LEFT_TEMPORAL  = 1
    BC_CMP_RIGHT_TEMPORAL = 2
    # Right operand is a set/list literal folded into literal_obj at bind time.
    # BC_COMPARE pops only ONE item from the stack (the left/column operand);
    # the right operand (set/list/CarcharSet) is read from slot.literal_obj.
    # This eliminates BC_LOAD_LIT_SET as a stack operand — sets can never be
    # DrakenVector* so they must not appear on the execution stack.
    BC_CMP_INLIST_INLINE  = 4


# Instruction-level flag bits (instr.flags for BC_FUNCTION / BC_EXTRACTION / BC_CAST / BC_BINARY_OP).
# Phase 9b: Distinguish C native kernels from legacy Python callables.
cdef enum BCInstrFlag:
    BC_INSTR_C_NATIVE = 0x1000  # kernel_fn is a C function pointer; dispatch to C ABI instead of PyObject_Call


ctypedef struct BytecodeInstr:
    int opcode               # BCOpcode
    int arity                # for BC_DNF / BC_CNF / BC_FUNCTION
    int op_code              # OP_EQ / OP_GT / ... for BC_COMPARE; lower_incl for BC_BETWEEN
    int flags                # bitfield of BCCompareFlag; upper_incl for BC_BETWEEN
    int bool_value           # 0/1 for BC_LOAD_LIT_BOOL; 1 = is_nb_callable for BC_FUNCTION (legacy)
    PyObject* literal_obj    # for BC_LOAD_LIT_SCALAR / BC_LOAD_LIT_SET; lower bound for BC_BETWEEN; key for BC_EXTRACTION
    PyObject* literal_obj2   # upper bound scalar for BC_BETWEEN
    PyObject* compare_op_str # for BC_COMPARE (AnyOp fallback); op string for BC_BINARY_OP / BC_EXTRACTION
    int16_t left_type_code   # BCTypeCode: BC_TYPE_NONE / DATE / TIMESTAMP for BC_COMPARE / BC_BINARY_OP
    int16_t right_type_code  # BCTypeCode: BC_TYPE_NONE / DATE / TIMESTAMP for BC_COMPARE / BC_BINARY_OP
    PyObject* column_identity # for BC_LOAD_COL — bytes
    PyObject* column_name     # for BC_LOAD_COL — bytes
    PyObject* source_node     # unused (legacy field, kept for ABI compatibility)
    PyObject* callable_ref   # for BC_FUNCTION — kernel callable (legacy Python path) or NULL (C native)
    # Phase 9b: C function ABI fields (zero if legacy Python path)
    void* kernel_fn          # C function pointer: VecResult (*)(void* ctx, ...) for BC_FUNCTION/EXTRACTION/CAST/BINARY_OP
    void* ctx_ptr            # context struct pointer (op_code/unit/sub_op_code, etc.) or NULL


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
