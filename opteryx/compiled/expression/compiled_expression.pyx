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
cdef type _CarcharSetWrapper_t = None
cdef type _PerfectHashSet_t = None


cdef inline dict _get_op_codes():
    global _OP_CODES
    if _OP_CODES is None:
        from opteryx.expression.evaluator import _OP_CODE
        _OP_CODES = _OP_CODE
    return _OP_CODES


cdef inline _ensure_orso_types():
    global _OrsoTypes_DATE, _OrsoTypes_TIMESTAMP
    if _OrsoTypes_DATE is None:
        from opteryx.types import OrsoTypes
        _OrsoTypes_DATE = OrsoTypes.DATE
        _OrsoTypes_TIMESTAMP = OrsoTypes.TIMESTAMP


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
    cdef object extr_op_str, extr_key
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
    # pre-read schema types, run temporal validation once
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

        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)

        op_codes = _get_op_codes()
        op_code_val = <int>op_codes.get(op_str, 0)
        _ensure_orso_types()

        flags = 0
        if left_type is _OrsoTypes_DATE or left_type is _OrsoTypes_TIMESTAMP:
            flags |= BC_CMP_LEFT_TEMPORAL
        if right_type is _OrsoTypes_DATE or right_type is _OrsoTypes_TIMESTAMP:
            flags |= BC_CMP_RIGHT_TEMPORAL

        slot = bc._push_instr()
        slot.opcode = BC_COMPARE
        slot.op_code = op_code_val
        slot.flags = flags
        bc._hold(op_str)
        slot.compare_op_str = <PyObject*>op_str
        if left_type is not None:
            bc._hold(left_type)
            slot.left_orso_type = <PyObject*>left_type
        if right_type is not None:
            bc._hold(right_type)
            slot.right_orso_type = <PyObject*>right_type
        return sub_depth - 1

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
    # NT_BINARY_OPERATOR — compile both operands, store op string and
    # operand types for temporal coercion at execution.
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
        bc._hold(bin_op_str)
        slot.compare_op_str = <PyObject*>bin_op_str
        if bin_left_type is not None:
            bc._hold(bin_left_type)
            slot.left_orso_type = <PyObject*>bin_left_type
        if bin_right_type is not None:
            bc._hold(bin_right_type)
            slot.right_orso_type = <PyObject*>bin_right_type
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
        bc._hold(unary_op_str)
        slot.compare_op_str = <PyObject*>unary_op_str
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

        slot = bc._push_instr()
        slot.opcode = BC_FUNCTION
        slot.arity = <int>n
        bc._hold(callable_obj)
        slot.callable_ref = <PyObject*>callable_obj
        if n == 0:
            return depth + 1
        return sub_depth - n + 1   # pop n, push 1

    # ------------------------------------------------------------------
    # NT_CAST — compile source expression, pre-resolve cast closure once.
    # The cast() factory is called at compile time; the returned callable
    # is stored as callable_ref and invoked per-morsel in the executor.
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

        from opteryx.expression.casts import cast as _cast_factory
        cast_kernel = _cast_factory(None, cast_target_type, cast_params, unit=cast_unit)

        slot = bc._push_instr()
        slot.opcode = BC_CAST
        bc._hold(cast_kernel)
        slot.callable_ref = <PyObject*>cast_kernel
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_EXTRACTION_OPERATOR — compile left operand, store op and key.
    # Key (node.right.value) is always a literal resolved at compile time.
    # ------------------------------------------------------------------
    if nt == _NT_EXTRACTION_OPERATOR:
        if node.left == NULL:
            raise ValueError("compiled_expression: EXTRACTION_OPERATOR missing left operand")
        extr_op_str = <object>node.value
        extr_key = <object>node.right.value if node.right != NULL else None

        sub_depth = _linearize(node.left, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_EXTRACTION
        bc._hold(extr_op_str)
        bc._hold(extr_key)
        slot.compare_op_str = <PyObject*>extr_op_str
        slot.literal_obj = <PyObject*>extr_key if extr_key is not None else NULL
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # Everything else — LEGACY: defer the whole subtree to _eval_value.
    # The legacy path is the GIL-required slow lane that handles CASE /
    # CAST until native opcodes cover them.
    # ------------------------------------------------------------------
    src = <object>node.source_node
    slot = bc._push_instr()
    slot.opcode = BC_LEGACY
    bc._hold(src)
    slot.source_node = <PyObject*>src
    depth += 1
    if depth > bc.max_stack_depth:
        bc.max_stack_depth = depth
    return depth


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
