# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""C++ CompiledExpression arena + typed bytecode builder.

Two public entry points:
  lower(node)            — lower a Python Node tree into a CompiledExpressionHandle
  build_bytecode(handle) — linearise the arena tree into a typed CompiledBytecode

CompiledBytecode is consumed by execute_bytecode() in the evaluator package.
Hot-path types (BytecodeInstr, CompiledBytecode internals) live in the .pxd
so the executor can read C struct fields directly with no Python protocol.

CLAUDE.md §2/§3: no `object`-typed fields in the hot data layout, no Python
list as instruction store, no method dispatch through PyObject during exec.
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.ref cimport PyObject, Py_INCREF
from libc.string cimport memset

import draken.draken_native as _draken_native
from opteryx.exceptions import IncorrectTypeError


# ---------------------------------------------------------------------------
# Phase 9b: Kernel resolution and context wrapper
# ---------------------------------------------------------------------------

cdef class _KernelContextWrapper:
    """Wraps a C context pointer for lifetime management.
    Ensures the context is freed when the wrapper is garbage collected."""
    cdef public unsigned long long ctx_ptr

    def __cinit__(self, unsigned long long ctx_ptr):
        self.ctx_ptr = ctx_ptr

    def __dealloc__(self):
        if self.ctx_ptr != 0:
            from draken.ops.kernels._kernel_registry import free_context
            free_context(self.ctx_ptr)

    def __repr__(self):
        return f"<KernelContextWrapper {self.ctx_ptr}>"


def _resolve_kernel_and_context(str kernel_name, context_allocator=None, context_arg=None):
    """Resolve a kernel by name and allocate context if needed.

    Returns (kernel_fn_ptr, context_wrapper_or_none).
    Returns (None, None) if kernel not found — no exception.
    Raises ValueError if context allocation fails (control flow, not fallback).
    """
    from draken.ops.kernels._kernel_registry import lookup_kernel

    fn_ptr, ctx_ptr = lookup_kernel(kernel_name)
    if fn_ptr is None:
        return None, None

    context_wrapper = None
    if context_allocator is not None:
        if context_arg is not None:
            ctx_ptr = context_allocator(context_arg)
        else:
            ctx_ptr = context_allocator()
        if ctx_ptr is None:
            raise ValueError(f"Failed to allocate context for kernel '{kernel_name}'")
        context_wrapper = _KernelContextWrapper(ctx_ptr)

    return fn_ptr, context_wrapper


# ---------------------------------------------------------------------------
# NodeType integer constants — must mirror NodeType IntEnum in
# opteryx/expression/__init__.py (verified at startup).
# ---------------------------------------------------------------------------
DEF _NT_AND = 17
DEF _NT_OR = 18
DEF _NT_XOR = 19
DEF _NT_NOT = 20
DEF _NT_DNF = 21
DEF _NT_CNF = 22
DEF _NT_CASE = 32
DEF _NT_COMPARISON_OPERATOR = 34
DEF _NT_BINARY_OPERATOR = 35
DEF _NT_UNARY_OPERATOR = 36
DEF _NT_FUNCTION = 37
DEF _NT_IDENTIFIER = 38
DEF _NT_NESTED = 40
DEF _NT_AGGREGATOR = 41
DEF _NT_LITERAL = 42
DEF _NT_EVALUATED = 44
DEF _NT_CAST = 45
DEF _NT_EXTRACTION_OPERATOR = 46
DEF _NT_BETWEEN = 47


# ---------------------------------------------------------------------------
# Bind-time lookups built lazily on first use.
# ---------------------------------------------------------------------------

cdef dict _OP_CODES = None
cdef object _OrsoTypes_DATE = None
cdef object _OrsoTypes_TIMESTAMP = None
cdef object _OrsoTypes_BOOLEAN = None
cdef object _OrsoTypes_VARCHAR = None
cdef object _OrsoTypes_ARRAY = None
cdef object _OrsoTypes_BLOB = None
cdef tuple _STRING_FAMILY = ()
cdef type _CarcharSetWrapper_t = None
cdef type _PerfectHashSet_t = None

# Result-handling flag bits (read by execute_bytecode after kernel return).
# Set at bind time; used to dispatch result wrapping without isinstance/type checks.
BC_RESULT_NEEDS_NB_WRAP = 0x10  # result is a raw nanobind Vector → wrap in Cython shim
BC_RESULT_WRAP_AS_BOOL  = 0x20  # wrap as BoolVector (else Vector); valid only with NEEDS_NB_WRAP
BC_RESULT_NO_DV         = 0x40  # result has no DV* (constant / scalar / not a vector) → store NULL in dv_stack

# Binary op string → BCBinaryOpCode. Built once at first use.
_BOP_CODE = {
    "Plus":          BOP_PLUS,
    "Minus":         BOP_MINUS,
    "Multiply":      BOP_MULTIPLY,
    "Divide":        BOP_DIVIDE,
    "Modulo":        BOP_MODULO,
    "MyIntegerDivide": BOP_INT_DIVIDE,
    "StringConcat":  BOP_STRING_CONCAT,
    "BitwiseOr":     BOP_BITWISE_OR,
    "BitwiseAnd":    BOP_BITWISE_AND,
    "BitwiseXor":    BOP_BITWISE_XOR,
    "ShiftLeft":     BOP_SHIFT_LEFT,
    "ShiftRight":    BOP_SHIFT_RIGHT,
}

# Unary op string → BCUnaryOpCode. Built once at module load.
_UOP_CODE = {
    "IsNull":      UOP_IS_NULL,
    "IsNotNull":   UOP_IS_NOT_NULL,
    "IsEmpty":     UOP_IS_EMPTY,
    "IsNotEmpty":  UOP_IS_NOT_EMPTY,
    "BitwiseNot":  UOP_BITWISE_NOT,
    "IsTrue":      UOP_IS_TRUE,
    "IsNotFalse":  UOP_IS_NOT_FALSE,
    "IsFalse":     UOP_IS_FALSE,
    "IsNotTrue":   UOP_IS_NOT_TRUE,
}


cdef inline int16_t _orso_type_to_code(object orso_type):
    """Convert an OrsoTypes value to a BCTypeCode integer. Returns BC_TYPE_NONE for None or non-temporal."""
    _ensure_orso_types()
    if orso_type is _OrsoTypes_DATE:
        return <int16_t>BC_TYPE_DATE
    if orso_type is _OrsoTypes_TIMESTAMP:
        return <int16_t>BC_TYPE_TIMESTAMP
    return <int16_t>BC_TYPE_NONE


cdef inline dict _get_op_codes():
    global _OP_CODES
    if _OP_CODES is None:
        from opteryx.expression.evaluator import _OP_CODE
        _OP_CODES = _OP_CODE
    return _OP_CODES


cdef inline _ensure_orso_types():
    global _OrsoTypes_DATE, _OrsoTypes_TIMESTAMP, _OrsoTypes_BOOLEAN
    global _OrsoTypes_VARCHAR, _OrsoTypes_ARRAY, _OrsoTypes_BLOB
    global _STRING_FAMILY
    if _OrsoTypes_DATE is None:
        from opteryx.types import OrsoTypes
        _OrsoTypes_DATE = OrsoTypes.DATE
        _OrsoTypes_TIMESTAMP = OrsoTypes.TIMESTAMP
        _OrsoTypes_BOOLEAN = OrsoTypes.BOOLEAN
        _OrsoTypes_VARCHAR = OrsoTypes.VARCHAR
        _OrsoTypes_ARRAY = OrsoTypes.ARRAY
        _OrsoTypes_BLOB = OrsoTypes.BLOB
        _STRING_FAMILY = (_OrsoTypes_VARCHAR, _OrsoTypes_BLOB)


cdef inline _ensure_set_types():
    global _CarcharSetWrapper_t, _PerfectHashSet_t
    if _CarcharSetWrapper_t is None:
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
        from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet
        _CarcharSetWrapper_t = CarcharSetWrapper
        _PerfectHashSet_t = PerfectHashSet


# ---------------------------------------------------------------------------
# CompiledBytecode container
# ---------------------------------------------------------------------------

DEF _INITIAL_CAP = 16


cdef class CompiledBytecode:
    """Owns a contiguous C array of BytecodeInstr and the Python refs the
    instructions point at.

    The executor (execute_bytecode in evaluation.pyx) reads instrs[i].opcode
    and the typed PyObject* slots directly. CompiledBytecode never INCREFs
    during execution; all owning refs live in _held_refs.
    """

    def __cinit__(self):
        self.instrs = NULL
        self.count = 0
        self.capacity = 0
        self.max_stack_depth = 0
        self._held_refs = []

    def __dealloc__(self):
        if self.instrs != NULL:
            PyMem_Free(self.instrs)
            self.instrs = NULL

    @property
    def length(self):
        return self.count

    @property
    def stack_depth(self):
        return self.max_stack_depth

    cdef BytecodeInstr* _push_instr(self) except NULL:
        cdef Py_ssize_t new_cap
        cdef BytecodeInstr* new_buf
        if self.count == self.capacity:
            new_cap = _INITIAL_CAP if self.capacity == 0 else self.capacity * 2
            new_buf = <BytecodeInstr*>PyMem_Realloc(
                self.instrs, <size_t>(new_cap * sizeof(BytecodeInstr))
            )
            if new_buf == NULL:
                raise MemoryError("CompiledBytecode: failed to grow instr array")
            self.instrs = new_buf
            self.capacity = new_cap
        cdef BytecodeInstr* slot = &self.instrs[self.count]
        memset(<void*>slot, 0, sizeof(BytecodeInstr))
        self.count += 1
        return slot

    cdef inline void _hold(self, object obj):
        # Anchor obj in the held-refs list. The slot reads <PyObject*>obj
        # which is a borrowed pointer; the list holds the strong ref.
        if obj is not None:
            self._held_refs.append(obj)


# ---------------------------------------------------------------------------
# Postfix lineariser
# ---------------------------------------------------------------------------

cdef Py_ssize_t _linearize(
    CompiledExpression* node,
    CompiledBytecode bc,
    Py_ssize_t depth,
) except -1:
    """Recursive postfix walk. Returns the resulting stack height after
    executing the emitted instructions for this subtree. `depth` is the
    stack height BEFORE this subtree. The maximum height seen is recorded
    on bc.max_stack_depth.

    Native opcodes pop their inputs and push one result (depth = depth+1
    after the instruction). Legacy opcodes are arity=0 (no children
    traversed) and also push one result.
    """
    cdef int nt = node.node_type
    cdef BytecodeInstr* slot
    cdef Py_ssize_t n, i, sub_depth
    cdef object value_obj
    cdef object schema_col
    cdef object identity_obj
    cdef object name_obj
    cdef bytes identity_bytes
    cdef bytes name_bytes
    cdef object left_sc, right_sc, left_type, right_type
    cdef object op_str
    cdef dict op_codes
    cdef int op_code_val
    cdef int flags
    # Variables for new native opcodes
    cdef object between_val, lower_obj, upper_obj
    cdef bint lower_incl, upper_incl
    cdef object bin_left_sc, bin_right_sc, bin_left_type, bin_right_type, bin_op_str
    cdef object unary_op_str
    cdef object func_val, func_ref_obj, func_py_node, func_ref_meta, callable_obj
    cdef object extr_op_str, extr_key, extr_callable, extr_key_vec
    cdef bint right_is_inlist_literal
    cdef object inlist_set_obj
    cdef object cast_target_type, cast_unit, cast_params, cast_kernel, cast_py_node
    cdef object src

    # ------------------------------------------------------------------
    # NT_NESTED — transparent, no instruction emitted
    # ------------------------------------------------------------------
    if nt == _NT_NESTED:
        if node.centre != NULL:
            return _linearize(node.centre, bc, depth)
        return depth

    # ------------------------------------------------------------------
    # NT_LITERAL — pre-categorise into bool / set / scalar
    # ------------------------------------------------------------------
    if nt == _NT_LITERAL:
        value_obj = <object>node.value
        slot = bc._push_instr()
        if isinstance(value_obj, bool):
            slot.opcode = BC_LOAD_LIT_BOOL
            slot.bool_value = 1 if value_obj else 0
        else:
            _ensure_set_types()
            if isinstance(value_obj, _CarcharSetWrapper_t) or isinstance(value_obj, _PerfectHashSet_t):
                slot.opcode = BC_LOAD_LIT_SET
            else:
                slot.opcode = BC_LOAD_LIT_SCALAR
            bc._hold(value_obj)
            slot.literal_obj = <PyObject*>value_obj
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # NT_IDENTIFIER / NT_EVALUATED / NT_AGGREGATOR — pre-resolve column
    # identity + encoded name bytes
    # ------------------------------------------------------------------
    if nt == _NT_IDENTIFIER or nt == _NT_EVALUATED or nt == _NT_AGGREGATOR:
        schema_col = <object>node.schema_column
        if schema_col is None:
            raise ValueError("compiled_expression: IDENTIFIER node missing schema_column")
        identity_obj = schema_col.identity
        name_obj = schema_col.name
        if isinstance(name_obj, bytes):
            name_bytes = <bytes>name_obj
        else:
            name_bytes = (<str>name_obj).encode()
        if isinstance(identity_obj, bytes):
            identity_bytes = <bytes>identity_obj
        else:
            # morsel.column accepts the identity object directly; we still
            # need to anchor it. Most callers use bytes already.
            identity_bytes = identity_obj
        slot = bc._push_instr()
        slot.opcode = BC_LOAD_COL
        bc._hold(identity_bytes)
        bc._hold(name_bytes)
        slot.column_identity = <PyObject*>identity_bytes
        slot.column_name = <PyObject*>name_bytes
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # Binary boolean combinators: AND / OR / XOR
    # ------------------------------------------------------------------
    if nt == _NT_AND or nt == _NT_OR or nt == _NT_XOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: binary boolean op missing operand")
        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)
        slot = bc._push_instr()
        if nt == _NT_AND:
            slot.opcode = BC_AND
        elif nt == _NT_OR:
            slot.opcode = BC_OR
        else:
            slot.opcode = BC_XOR
        return sub_depth - 1   # pop 2, push 1 → net -1

    # ------------------------------------------------------------------
    # Unary NOT
    # ------------------------------------------------------------------
    if nt == _NT_NOT:
        if node.centre == NULL:
            raise ValueError("compiled_expression: NOT missing operand")
        sub_depth = _linearize(node.centre, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_NOT
        return sub_depth          # pop 1, push 1 → net 0

    # ------------------------------------------------------------------
    # Variadic AND/OR — DNF / CNF
    # ------------------------------------------------------------------
    if nt == _NT_DNF or nt == _NT_CNF:
        n = <Py_ssize_t>node.parameters.size()
        if n == 0:
            raise ValueError("compiled_expression: DNF/CNF with no parameters")
        sub_depth = depth
        for i in range(n):
            if node.parameters[i] == NULL:
                raise ValueError("compiled_expression: DNF/CNF parameter NULL")
            sub_depth = _linearize(node.parameters[i], bc, sub_depth)
        slot = bc._push_instr()
        slot.opcode = BC_DNF if nt == _NT_DNF else BC_CNF
        slot.arity = <int>n
        return sub_depth - n + 1   # pop n, push 1

    # ------------------------------------------------------------------
    # NT_COMPARISON_OPERATOR — resolve op string to integer code,
    # pre-read schema types, run temporal validation once.
    #
    # IN-list fold (BC_CMP_INLIST_INLINE): when the right operand is an
    # NT_LITERAL whose value is a set/list/CarcharSet we fold it directly
    # into the BC_COMPARE instruction (slot.literal_obj) instead of
    # emitting a separate BC_LOAD_LIT_SET instruction.  This removes the
    # set from the execution stack entirely — sets cannot become
    # DrakenVector* and must not appear as stack operands.
    # Folded BC_COMPARE pops ONE item (left) and pushes one result;
    # non-folded pops TWO items (left + right) and pushes one result.
    # ------------------------------------------------------------------
    if nt == _NT_COMPARISON_OPERATOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: COMPARISON missing operand")

        # Read schema types from children BEFORE linearising them.
        left_sc = <object>node.left.schema_column
        right_sc = <object>node.right.schema_column
        left_type = getattr(left_sc, "type", None) if left_sc is not None else None
        right_type = getattr(right_sc, "type", None) if right_sc is not None else None
        op_str = <object>node.value
        _validate_temporal_at_bind(
            node.left.node_type, left_type,
            node.right.node_type, right_type,
            op_str,
        )

        # Detect set/list literal on the right — fold if found.
        right_is_inlist_literal = False
        inlist_set_obj = None
        if node.right != NULL and node.right.node_type == _NT_LITERAL:
            inlist_set_obj = <object>node.right.value
            _ensure_set_types()
            if (
                isinstance(inlist_set_obj, _CarcharSetWrapper_t)
                or isinstance(inlist_set_obj, _PerfectHashSet_t)
                or isinstance(inlist_set_obj, (list, tuple, set, frozenset))
            ):
                right_is_inlist_literal = True
            else:
                inlist_set_obj = None  # scalar literal — don't fold

        sub_depth = _linearize(node.left, bc, depth)
        if not right_is_inlist_literal:
            sub_depth = _linearize(node.right, bc, sub_depth)

        op_codes = _get_op_codes()
        op_code_val = <int>op_codes.get(op_str, 0)
        if op_code_val == 0:
            raise NotImplementedError(
                f"compiled_expression: unknown comparison operator {op_str!r}"
            )
        _ensure_orso_types()

        flags = 0
        if left_type is _OrsoTypes_DATE or left_type is _OrsoTypes_TIMESTAMP:
            flags |= BC_CMP_LEFT_TEMPORAL
        if right_type is _OrsoTypes_DATE or right_type is _OrsoTypes_TIMESTAMP:
            flags |= BC_CMP_RIGHT_TEMPORAL
        if right_is_inlist_literal:
            flags |= BC_CMP_INLIST_INLINE

        slot = bc._push_instr()
        slot.opcode = BC_COMPARE
        slot.op_code = op_code_val
        slot.flags = flags
        slot.left_type_code = _orso_type_to_code(left_type)
        slot.right_type_code = _orso_type_to_code(right_type)
        if right_is_inlist_literal:
            bc._hold(inlist_set_obj)
            slot.literal_obj = <PyObject*>inlist_set_obj
            return sub_depth      # pop 1 push 1 — net 0
        return sub_depth - 1      # pop 2 push 1 — net -1

    # ------------------------------------------------------------------
    # NT_BETWEEN — compile left operand, store bounds and inclusivity flags
    # at compile time; draken_between is called at execution.
    # ------------------------------------------------------------------
    if nt == _NT_BETWEEN:
        if node.left == NULL:
            raise ValueError("compiled_expression: BETWEEN missing left operand")
        sub_depth = _linearize(node.left, bc, depth)

        between_val = <object>node.value
        lower_incl = between_val[0]
        upper_incl = between_val[1]
        lower_obj = <object>node.right.value if node.right != NULL else None
        upper_obj = <object>node.centre.value if node.centre != NULL else None

        slot = bc._push_instr()
        slot.opcode = BC_BETWEEN
        slot.op_code = 1 if lower_incl else 0
        slot.bool_value = 1 if upper_incl else 0
        bc._hold(lower_obj)
        bc._hold(upper_obj)
        slot.literal_obj = <PyObject*>lower_obj if lower_obj is not None else NULL
        slot.literal_obj2 = <PyObject*>upper_obj if upper_obj is not None else NULL
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_BINARY_OPERATOR — Phase 6: resolve kernel at bind time, store
    # callable ref. Operand types stored for introspection/debugging.
    # ------------------------------------------------------------------
    if nt == _NT_BINARY_OPERATOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: BINARY_OPERATOR missing operand")
        bin_left_sc = <object>node.left.schema_column if node.left.schema_column != NULL else None
        bin_right_sc = <object>node.right.schema_column if node.right.schema_column != NULL else None
        bin_left_type = getattr(bin_left_sc, "type", None) if bin_left_sc is not None else None
        bin_right_type = getattr(bin_right_sc, "type", None) if bin_right_sc is not None else None
        bin_op_str = <object>node.value

        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)

        slot = bc._push_instr()
        slot.opcode = BC_BINARY_OP
        slot.op_code = <int>_BOP_CODE.get(bin_op_str, BOP_UNKNOWN)
        if slot.op_code == BOP_UNKNOWN:
            raise NotImplementedError(f"compiled_expression: unknown binary op {bin_op_str!r}")

        # Phase 6: resolve the kernel at bind time.
        from opteryx.expression.evaluator.arithmetic import resolve_binary_op
        binop_kernel = resolve_binary_op(slot.op_code, bin_left_type, bin_right_type)
        bc._hold(binop_kernel)
        slot.callable_ref = <PyObject*>binop_kernel

        # Phase 9b: Resolve C kernel function pointer (binary ops use dispatch kernel).
        _binop_kernel_names = {
            BOP_PLUS: "draken_add",
            BOP_MINUS: "draken_subtract",
            BOP_MULTIPLY: "draken_multiply",
            BOP_DIVIDE: "draken_divide",
            BOP_MODULO: "draken_modulo",
            BOP_INT_DIVIDE: "draken_divide",  # Integer division via dispatch
            BOP_STRING_CONCAT: "draken_string_concat",
            BOP_BITWISE_OR: "draken_bitwise_or",
            BOP_BITWISE_AND: "draken_bitwise_and",
            BOP_BITWISE_XOR: "draken_bitwise_xor",
            BOP_SHIFT_LEFT: "draken_bitwise_shift_left",
            BOP_SHIFT_RIGHT: "draken_bitwise_shift_right",
        }
        kernel_name = _binop_kernel_names.get(slot.op_code)
        if kernel_name is not None:
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(kernel_name, None, None)
            if fn_ptr is not None:
                slot.kernel_fn = <void*>fn_ptr
                if ctx_wrapper is not None:
                    bc._hold(ctx_wrapper)
                    slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                slot.flags |= BC_INSTR_C_NATIVE
            else:
                # Fail-fast: all mapped binary ops have C kernels; a miss is a bug.
                raise ValueError(f"Binary operator kernel '{kernel_name}' not found in registry")

        # Phase 1 result-wrap pattern: kernels return nanobind Vectors.
        slot.flags |= BC_RESULT_NEEDS_NB_WRAP
        # Binary ops never return BOOL, so BC_RESULT_WRAP_AS_BOOL stays false.

        # Keep type codes for debugging / introspection (not used in executor).
        slot.left_type_code = _orso_type_to_code(bin_left_type)
        slot.right_type_code = _orso_type_to_code(bin_right_type)
        # Note: slot.compare_op_str no longer needed for BC_BINARY_OP, but field stays.
        return sub_depth - 1   # pop 2, push 1

    # ------------------------------------------------------------------
    # NT_UNARY_OPERATOR — compile centre operand, store op string.
    # ------------------------------------------------------------------
    if nt == _NT_UNARY_OPERATOR:
        if node.centre == NULL:
            raise ValueError("compiled_expression: UNARY_OPERATOR missing centre operand")
        unary_op_str = <object>node.value
        sub_depth = _linearize(node.centre, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_UNARY_OP
        slot.op_code = <int>_UOP_CODE.get(unary_op_str, UOP_UNKNOWN)
        # compare_op_str not set for BC_UNARY_OP — executor uses op_code int directly
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_FUNCTION — compile each parameter, store callable and arity.
    # _PASSTHRU is transparent: just compile the single parameter.
    # ------------------------------------------------------------------
    if nt == _NT_FUNCTION:
        func_val = <object>node.value
        if func_val == "_PASSTHRU":
            if node.parameters.size() == 0:
                raise ValueError("compiled_expression: _PASSTHRU FUNCTION has no parameters")
            return _linearize(node.parameters[0], bc, depth)

        n = <Py_ssize_t>node.parameters.size()
        func_ref_obj = <object>node.source_node
        func_py_node = func_ref_obj
        func_ref_meta = getattr(func_py_node, "function_ref", None)
        if func_ref_meta is None:
            raise ValueError(
                f"compiled_expression: FUNCTION '{func_val}' has no function_ref — not bound"
            )
        callable_obj = func_ref_meta.selected_overload.kernel.callable_ref

        sub_depth = depth
        for i in range(n):
            if node.parameters[i] == NULL:
                raise ValueError("compiled_expression: FUNCTION parameter NULL")
            sub_depth = _linearize(node.parameters[i], bc, sub_depth)

        # Pre-compute nb_func flag at bind time — eliminates runtime
        # `type(callable).__name__ == "nb_func"` string comparison per call.
        is_nb_callable = type(callable_obj).__name__ == "nb_func"

        slot = bc._push_instr()
        slot.opcode = BC_FUNCTION
        slot.arity = <int>n
        slot.bool_value = 1 if is_nb_callable else 0

        # Set result-wrap flags based on kernel return type (resolved at bind time).
        # This eliminates runtime isinstance/type checks on the executor path.
        slot.flags = 0
        if is_nb_callable:
            slot.flags |= BC_RESULT_NEEDS_NB_WRAP
            _ensure_orso_types()
            if func_ref_meta.inferred_return_type is _OrsoTypes_BOOLEAN:
                slot.flags |= BC_RESULT_WRAP_AS_BOOL

        # Phase 9b: Resolve C kernel function pointer for function calls.
        # Function kernels (Phase 9a-fn) are under development; resolution is optional.
        # If a C kernel exists, use it; otherwise, fall back to Python callable_ref.
        # This is the intended behaviour while function kernels are being ported.
        func_name = func_val.upper() if func_val else None
        if func_name is not None:
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(f"draken_{func_name.lower()}")
            if fn_ptr is not None:
                slot.kernel_fn = <void*>fn_ptr
                # Function kernels generally don't need context structs
                slot.flags |= BC_INSTR_C_NATIVE
            # else: kernel not available yet (pending Phase 9a-fn); callable_ref path remains

        bc._hold(callable_obj)
        slot.callable_ref = <PyObject*>callable_obj
        if n == 0:
            return depth + 1
        return sub_depth - n + 1   # pop n, push 1

    # ------------------------------------------------------------------
    # NT_CAST — compile source expression, resolve at bind time.
    # Phase 5: resolve_cast(source_orso, target_type, args, unit) returns a
    # pre-specialized kernel/closure; stored as callable_ref, invoked per-morsel.
    # ------------------------------------------------------------------
    if nt == _NT_CAST:
        if node.left == NULL:
            raise ValueError("compiled_expression: CAST missing source operand")
        sub_depth = _linearize(node.left, bc, depth)

        cast_py_node = <object>node.source_node
        cast_target_type = cast_py_node.value
        if cast_target_type.startswith("TRY_"):
            cast_target_type = cast_target_type[4:]

        cast_unit = None
        _unit_map = {
            "_TIMESTAMP_NS": ("TIMESTAMP", "ns"),
            "_TIMESTAMP_MS": ("TIMESTAMP", "ms"),
            "_TIMESTAMP_S":  ("TIMESTAMP", "s"),
            "_TIMESTAMP_US": ("TIMESTAMP", "us"),
            "_TIMESTAMP_DAYS": ("TIMESTAMP", "days"),
        }
        if cast_target_type in _unit_map:
            cast_target_type, cast_unit = _unit_map[cast_target_type]

        cast_params = tuple(
            p.value for p in (cast_py_node.parameters or [])
        )

        # Phase 5: get the source operand's type from schema_column for bind-time resolution.
        source_orso_name = None
        if node.left.schema_column != NULL:
            src_sc = <object>node.left.schema_column
            if src_sc is not None:
                source_orso = src_sc.type
                if source_orso is not None:
                    # Extract the OrsoType name string (e.g., "INT64", "VARCHAR").
                    source_orso_name = getattr(source_orso, "name", None)

        from opteryx.expression.casts import resolve_cast
        try:
            cast_kernel = resolve_cast(source_orso_name, cast_target_type, cast_params, unit=cast_unit)
        except (NotImplementedError, ValueError) as e:
            raise ValueError(f"Unsupported CAST: {source_orso_name} → {cast_target_type}: {e}")

        slot = bc._push_instr()
        slot.opcode = BC_CAST
        # Phase 5: determine NEEDS_NB_WRAP based on resolved kernel return type.
        slot.flags = 0
        _ensure_orso_types()

        # Check if this is a no-op cast (source == target).
        cast_target_orso = getattr(cast_py_node, "inferred_type", None)
        is_noop_cast = (source_orso_name == cast_target_type or
                        (source_orso == cast_target_orso and cast_target_orso is not None))

        # Determine if the resolved kernel returns a nanobind Vector that needs wrapping.
        # - Passthrough (no-op) casts return Cython Vectors → no wrap
        # - cast_to_boolean returns a BoolVector (Cython) → no wrap
        # - cast_to_varchar, cast_to_int, cast_to_double return nanobind Vectors → wrap
        # - Native kernels (vector_cast_*) return nanobind Vectors → wrap
        needs_nb_wrap = False
        if not is_noop_cast and cast_target_type != "BOOLEAN":
            # Non-trivial casts (except BOOLEAN) return nanobind Vectors.
            needs_nb_wrap = True

        if needs_nb_wrap:
            slot.flags |= BC_RESULT_NEEDS_NB_WRAP

        if cast_target_orso is _OrsoTypes_BOOLEAN:
            slot.flags |= BC_RESULT_WRAP_AS_BOOL

        # Phase 9b: Resolve C kernel function pointer for cast operations.
        # Build kernel name from source and target types.
        # Map OrsoTypes enum names (e.g., "INTEGER", "DOUBLE") to registry type tokens (e.g., "int64", "float64").
        _orso_to_type_name = {
            "INTEGER": "int64",
            "DOUBLE": "float64",
            "VARCHAR": "string",
            "BOOLEAN": "bool",
            "DATE": "date32",
            "TIMESTAMP": "timestamp",
            "BLOB": "string",
        }
        # Map target types to dispatch kernels for unsupported source types.
        _dispatch_kernels = {
            "VARCHAR": "draken_cast_to_varchar",
            "INTEGER": "draken_cast_to_int64",
            "DOUBLE": "draken_cast_to_float64",
            "BOOLEAN": "draken_cast_to_bool",
            "DATE": "draken_cast_to_date",
            "TIMESTAMP": "draken_cast_to_timestamp",
            "DECIMAL": "draken_cast_to_decimal",
            "ARRAY": "draken_cast_to_array",
            "VECTOR": "draken_cast_to_vector",
        }

        src_type_name = _orso_to_type_name.get(source_orso_name)
        dst_type_name = _orso_to_type_name.get(cast_target_type)

        kernel_name = None
        context_allocator = None
        context_args = ()

        # Try specific source→target kernel if both types are supported.
        if src_type_name and dst_type_name:
            # Check for identity cast first (source == target).
            if source_orso_name == cast_target_type:
                kernel_name = "draken_cast_identity"
            else:
                kernel_name = f"draken_cast_{src_type_name}_to_{dst_type_name}"

            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(kernel_name, None, None)
            if fn_ptr is None:
                # Fail-fast: supported type combo but kernel missing is a bug.
                raise ValueError(
                    f"Kernel '{kernel_name}' not found in registry for cast {source_orso_name} → {cast_target_type}. "
                    f"This is a supported combination but kernel is missing or incorrectly named."
                )
        else:
            # Unsupported source type or parameterized cast; try dispatch kernel.
            if cast_target_type == "TIMESTAMP" and cast_unit is not None:
                # Parameterized TIMESTAMP cast uses dispatch kernel with context.
                from draken.ops.kernels._kernel_registry import alloc_cast_timestamp_ctx
                unit_code = {"ns": 1, "us": 2, "ms": 3, "s": 4, "days": 5}.get(cast_unit, 0)
                context_allocator = alloc_cast_timestamp_ctx
                context_args = (unit_code,)
                kernel_name = "draken_cast_to_timestamp"
            else:
                # Generic dispatch kernel for target type.
                kernel_name = _dispatch_kernels.get(cast_target_type)

            if kernel_name:
                fn_ptr, ctx_wrapper = _resolve_kernel_and_context(kernel_name, context_allocator, context_args[0] if context_args else None)
            else:
                fn_ptr, ctx_wrapper = None, None

        if fn_ptr is not None:
            slot.kernel_fn = <void*>fn_ptr
            if ctx_wrapper is not None:
                bc._hold(ctx_wrapper)
                slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
            slot.flags |= BC_INSTR_C_NATIVE

        bc._hold(cast_kernel)
        slot.callable_ref = <PyObject*>cast_kernel
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_EXTRACTION_OPERATOR — Phase 3: bind-time resolution
    #
    # Resolve (op_str, operand_type) → kernel + sub-op flag at bind time.
    # No Python wrappers, no runtime type dispatch. The executor calls the
    # resolved native kernel directly via the sub-op code in slot.op_code.
    #
    # op_code stores the sub-op flag (BC_EXTR_MAP_STRING, etc.)
    # literal_obj stores either:
    #   - raw key bytes for JSON extraction (Arrow / LongArrow)
    #   - length-1 INT64 key Vector for string map access
    # bool_value stores:
    #   - scalar int64 key for ARRAY map access (option B: store int directly)
    # ------------------------------------------------------------------
    if nt == _NT_EXTRACTION_OPERATOR:
        if node.left == NULL:
            raise ValueError("compiled_expression: EXTRACTION_OPERATOR missing left operand")

        _ensure_orso_types()
        extr_op_str = <object>node.value
        extr_key = <object>node.right.value if node.right != NULL else None

        # Resolve operand type from schema_column on the left operand node.
        left_sc = <object>node.left.schema_column if node.left.schema_column != NULL else None
        if left_sc is None:
            raise ValueError("compiled_expression: EXTRACTION_OPERATOR left operand missing schema_column")
        left_orso = left_sc.type

        # Sub-op + kernel selection: resolve at bind time.
        sub_op = BC_EXTR_UNKNOWN
        extr_literal = None
        slot_bool_val = 0

        if extr_op_str == "MapAccess":
            if left_orso == _OrsoTypes_ARRAY:
                # MapAccess on ARRAY: store scalar int64 key in bool_value (Option B).
                # The scalar is extracted from the constant key at bind time.
                sub_op = BC_EXTR_MAP_ARRAY
                # Store the int64 key directly in bool_value.
                slot_bool_val = int(extr_key)
            elif left_orso in _STRING_FAMILY:
                # MapAccess on string: store length-1 INT64 key Vector.
                sub_op = BC_EXTR_MAP_STRING
                extr_literal = _draken_native.vector_from_constant(int(extr_key), 1)
            else:
                raise IncorrectTypeError(
                    f"MapAccess: operand must be ARRAY or string family; got {left_orso!r}"
                )
        elif extr_op_str == "Arrow":
            if left_orso not in _STRING_FAMILY:
                raise IncorrectTypeError(
                    f"-> requires a string/JSON operand; got {left_orso!r}"
                )
            sub_op = BC_EXTR_JSON_PTR
            # Store raw key bytes.
            extr_literal = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
        elif extr_op_str == "LongArrow":
            if left_orso not in _STRING_FAMILY:
                raise IncorrectTypeError(
                    f"->> requires a string/JSON operand; got {left_orso!r}"
                )
            sub_op = BC_EXTR_JSON_KEY
            # Store raw key bytes.
            extr_literal = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
        else:
            raise ValueError(f"unknown EXTRACTION_OPERATOR: {extr_op_str!r}")

        sub_depth = _linearize(node.left, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_EXTRACTION
        slot.op_code = sub_op
        slot.flags = BC_RESULT_NEEDS_NB_WRAP

        # Phase 9b: Resolve C kernel function pointer for extraction operations.
        _extr_kernel_names = {
            BC_EXTR_MAP_STRING: "draken_map_access_string",
            BC_EXTR_MAP_ARRAY: "draken_array_map_access",
            BC_EXTR_JSON_PTR: "draken_json_extract",
            BC_EXTR_JSON_KEY: "draken_json_extract",
        }
        if sub_op in _extr_kernel_names:
            from draken.ops.kernels._kernel_registry import alloc_extraction_ctx
            context_allocator = alloc_extraction_ctx
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                _extr_kernel_names[sub_op],
                context_allocator,
                sub_op
            )
            if fn_ptr is None:
                raise ValueError(
                    f"Extraction kernel '{_extr_kernel_names[sub_op]}' not found in registry. "
                    f"This is a supported extraction operation but kernel is missing."
                )

            slot.kernel_fn = <void*>fn_ptr
            if ctx_wrapper is not None:
                bc._hold(ctx_wrapper)
                slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
            slot.flags |= BC_INSTR_C_NATIVE

        # Store the extracted literal (bytes or Vector).
        if extr_literal is not None:
            bc._hold(extr_literal)
            slot.literal_obj = <PyObject*>extr_literal
        # For MapAccess ARRAY, the bool_value was set above.
        if sub_op == BC_EXTR_MAP_ARRAY:
            slot.bool_value = slot_bool_val

        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_CASE — compile all WHEN conditions and THEN/ELSE results to
    # bytecode at bind time; resolve output type and select assembly kernel
    # at bind time; store a pre-built closure in callable_ref.
    # At execution time BC_CASE simply calls callable_ref(morsel) — no
    # runtime type dispatch.
    # ------------------------------------------------------------------
    if nt == _NT_CASE:
        src = <object>node.source_node

        cond_bcs = [build_bytecode(lower(c)) for c in src.conditions]
        result_bcs = [build_bytecode(lower(r)) for r in src.results]
        else_bc = build_bytecode(lower(src.else_result)) if src.else_result is not None else None

        # Phase 7: resolve output type from inferred types at bind time
        _ensure_orso_types()
        case_inferred_type = getattr(src, "inferred_type", None)

        # Select the assembly kernel based on the inferred result type.
        # All THEN/ELSE branches must agree on type (enforced by binder).
        from opteryx.expression.evaluator.case_eval import build_case_fn

        # Kernel type constants match the DEF values in case_eval.pyx
        _ASSEMBLE_BOOL = 0
        _ASSEMBLE_FIXED = 1
        _ASSEMBLE_STRING = 2

        # Determine kernel type from inferred type
        if case_inferred_type is _OrsoTypes_BOOLEAN:
            kernel_type = _ASSEMBLE_BOOL
        elif case_inferred_type in (_OrsoTypes_VARCHAR, _OrsoTypes_BLOB):
            kernel_type = _ASSEMBLE_STRING
        elif case_inferred_type is None:
            # Fallback when inferred_type is None: defer to runtime type dispatch.
            # Use -1 as a sentinel to trigger runtime dispatch in build_case_fn.
            kernel_type = -1
        else:
            # Fixed-width (numeric, date, timestamp, etc.)
            kernel_type = _ASSEMBLE_FIXED

        case_callable = build_case_fn(cond_bcs, result_bcs, else_bc, kernel_type)

        slot = bc._push_instr()
        slot.opcode = BC_CASE
        # CASE closure returns a nanobind Vector; set NEEDS_NB_WRAP.
        slot.flags = BC_RESULT_NEEDS_NB_WRAP
        if case_inferred_type is _OrsoTypes_BOOLEAN:
            slot.flags |= BC_RESULT_WRAP_AS_BOOL
        bc._hold(case_callable)
        slot.callable_ref = <PyObject*>case_callable
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # Bind-time invariant: every supported node type has an explicit branch
    # above. Reaching here is a planner/compiler bug.
    # ------------------------------------------------------------------
    raise NotImplementedError(f"compiled_expression: unsupported node type {nt}")


cdef _validate_temporal_at_bind(
    int left_nt, left_type, int right_nt, right_type, op
):
    """Raise IncompatibleTypesError at bind time if a temporal comparison
    has an un-cast literal on one side. Runs once per COMPARISON node.
    """
    _ensure_orso_types()
    cdef bint left_is_temporal = (left_type is _OrsoTypes_DATE) or (left_type is _OrsoTypes_TIMESTAMP)
    cdef bint right_is_temporal = (right_type is _OrsoTypes_DATE) or (right_type is _OrsoTypes_TIMESTAMP)

    if not (left_is_temporal or right_is_temporal):
        return
    if left_is_temporal and right_is_temporal:
        return

    cdef int non_temporal_nt = right_nt if left_is_temporal else left_nt
    non_temporal_side = "right" if left_is_temporal else "left"

    if non_temporal_nt != _NT_IDENTIFIER:
        from opteryx.exceptions import IncompatibleTypesError
        raise IncompatibleTypesError(
            message=(
                f"Temporal comparison requires literals to be explicitly cast to temporal types.\n"
                f"The {non_temporal_side} side is missing an explicit CAST or :: operator.\n\n"
                f"Examples of valid syntax:\n"
                f"  - col {op} literal::DATE\n"
                f"  - col {op} literal::TIMESTAMP[ms]\n\n"
                f"Supported temporal types: DATE, TIMESTAMP[ms], TIMESTAMP[us], TIMESTAMP[s], TIMESTAMP[ns], TIMESTAMP[d]"
            )
        )


# ---------------------------------------------------------------------------
# CompiledExpressionHandle (unchanged from previous wedge)
# ---------------------------------------------------------------------------

cdef class CompiledExpressionHandle:
    """Owns one CompiledExpressionArena and the root pointer into it."""

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
        if self._root == NULL:
            return []
        return self._arena.node_type_walk(self._root)

    cdef CompiledExpression* root(self) noexcept:
        return self._root


def lower(node):
    """Lower an opteryx Node tree into a CompiledExpressionHandle."""
    cdef CompiledExpressionHandle handle = CompiledExpressionHandle()
    handle._root = handle._arena.lower(node)
    return handle


_PURE_BITMAP_OPCODES = frozenset({
    BC_LOAD_COL, BC_LOAD_LIT_BOOL,
    BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF,
})

def build_bytecode(CompiledExpressionHandle handle):
    """Linearise the lowered tree into a typed CompiledBytecode container."""
    if handle._root == NULL:
        raise ValueError("build_bytecode: handle has no lowered root")
    cdef CompiledBytecode bc = CompiledBytecode()
    _linearize(handle._root, bc, 0)

    # Scan opcodes: is_pure_bitmap is True when every instruction is GIL-free.
    # BC_LOAD_COL is included; the runtime pre-pass verifies the column type.
    cdef Py_ssize_t k
    bc.is_pure_bitmap = True
    for k in range(bc.count):
        if bc.instrs[k].opcode not in _PURE_BITMAP_OPCODES:
            bc.is_pure_bitmap = False
            break

    return bc
