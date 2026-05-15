"""Main expression evaluation engine (Cython orchestration layer).

Layering (CLAUDE.md):
- Python   : user-facing API + planner/binder only.
- Cython   : execution orchestration (this file: tree walk + dispatch).
- C++      : execution kernels (Draken vector ops, called from here).

NodeType integer constants are inlined as compile-time DEFs to turn the
dispatch chain into a series of C-level integer compares. They MUST match
the values declared on the NodeType IntEnum in opteryx/expression/__init__.py;
a runtime check in opteryx/expression/evaluator/__init__.py verifies this.
"""

import datetime
import sys as _sys

from opteryx.compiled.structures.carchar_set import CarcharSetWrapper as _CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet as _PerfectHashSet
from opteryx.compiled.vector_ops import (
    vector_bitwise_not as _vector_bitwise_not,
    vector_string_is_empty as _vector_string_is_empty,
    vector_string_is_not_empty as _vector_string_is_not_empty,
)
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError, IncompatibleTypesError
from opteryx.types import OrsoTypes as _OrsoTypes
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


# Imports from draken are safe at module level — draken does not import opteryx.expression.
from draken.vectors.bool_vector import BoolVector as _BoolVector
from draken.vectors.int64_vector import Int64Vector as _Int64Vector
from draken.vectors.string_vector import StringVector as _StringVector
from draken.vectors.scalar_constructors import from_scalar as _const_scalar
from draken.morsels.morsel import Morsel as _Morsel
from draken.interop.vector_sequence import (
    bool_vector_from_uint64_eq as _bool_vector_from_uint64_eq,
    vector_from_sequence as _vector_from_sequence,
)


# NodeType integer values — keep in sync with NodeType in opteryx/expression/__init__.py.
DEF NT_UNKNOWN = 0
DEF NT_AND = 17
DEF NT_OR = 18
DEF NT_XOR = 19
DEF NT_NOT = 20
DEF NT_DNF = 21
DEF NT_CNF = 22
DEF NT_CASE = 32
DEF NT_WILDCARD = 33
DEF NT_COMPARISON_OPERATOR = 34
DEF NT_BINARY_OPERATOR = 35
DEF NT_UNARY_OPERATOR = 36
DEF NT_FUNCTION = 37
DEF NT_IDENTIFIER = 38
DEF NT_SUBQUERY = 39
DEF NT_NESTED = 40
DEF NT_AGGREGATOR = 41
DEF NT_LITERAL = 42
DEF NT_EXPRESSION_LIST = 43
DEF NT_EVALUATED = 44
DEF NT_CAST = 45
DEF NT_EXTRACTION_OPERATOR = 46
DEF NT_BETWEEN = 47


_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)

_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


def _is_scalar_value(obj):
    """Deprecated: use is_scalar() from opteryx.utils.vector_types instead."""
    return is_scalar(obj)


def _eval_value(node, morsel):
    cdef int node_type = <int>node.node_type

    if node_type == NT_LITERAL:
        if isinstance(node.value, bool):
            return _BoolVector.from_constant(node.value, morsel.num_rows)

        if isinstance(node.value, (_CarcharSetWrapper, _PerfectHashSet)):
            return node.value

        vec = _const_scalar(node.value, morsel.num_rows)
        if vec is None:
            raise TypeError(
                f"_eval_value: cannot construct Draken vector for literal "
                f"{node.value!r} (type {type(node.value).__name__})"
            )
        return vec

    if node_type == NT_IDENTIFIER:
        return morsel.column(node.schema_column.identity, node.schema_column.name.encode())

    if node_type == NT_EVALUATED or node_type == NT_AGGREGATOR:
        try:
            return morsel.column(
                node.schema_column.identity, node.schema_column.name.encode()
            )
        except KeyError:
            raise ColumnReferencedBeforeEvaluationError(column=node.schema_column.name)

    if node_type == NT_NESTED:
        return _eval_value(node.centre, morsel)

    if node_type == NT_CASE:
        from opteryx.expression.evaluator.case_eval import evaluate_case
        return evaluate_case(node, morsel)

    if node_type == NT_EXPRESSION_LIST:
        return [_eval_value(parameter, morsel) for parameter in node.parameters]

    if node_type == NT_EXTRACTION_OPERATOR:
        left_vec = _eval_value(node.left, morsel)
        right_val = node.right.value
        op = node.value

        if op == "MapAccess":
            from opteryx.expression.binary_operators import MapAccessOp
            # Keep MapAccess in native vector space where possible to avoid
            # costly Arrow <-> Draken round-trips.
            key_vec = _Int64Vector.from_constant(int(right_val), 1)
            result = MapAccessOp(left_vec, key_vec)
            if is_draken_vector(result):
                return result
            raise TypeError(
                f"MapAccessOp expected Draken vector result; got {type(result).__name__}."
            )

        if op == "Arrow" or op == "LongArrow":
            from opteryx.expression.binary_operators import ArrowOp, LongArrowOp
            key_vec = _StringVector.from_constant(right_val, 1)
            return ArrowOp(left_vec, key_vec) if op == "Arrow" else LongArrowOp(left_vec, key_vec)

        raise NotImplementedError(
            f"_eval_value: EXTRACTION_OPERATOR {op!r} not supported in Draken path"
        )

    if node_type == NT_BINARY_OPERATOR:
        result = _eval_binary_op_draken(node, morsel)
        if result is not None:
            return result

    if node_type == NT_BINARY_OPERATOR or node_type == NT_CAST or node_type == NT_FUNCTION:
        sc = getattr(node, "schema_column", None)
        identity = getattr(sc, "identity", None) if sc is not None else None
        if identity is not None:
            try:
                vec = morsel.column(identity)
            except KeyError:
                vec = None
            if vec is not None:
                return vec

        from opteryx.expression import _inner_evaluate
        result = _inner_evaluate(node, morsel)
        if result is not None and is_draken_vector(result):
            return result
        raise TypeError(
            f"_eval_value: expected Draken vector for node {node.node_type!r}; "
            f"got {type(result).__name__}."
        )

    return evaluate_draken(node, morsel)


cdef _unary_draken(str op, centre_node, morsel):
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        return _is_null_as_boolvector(vec).not_vector()
    if op == "IsEmpty":
        return _vector_string_is_empty(vec)
    if op == "IsNotEmpty":
        return _vector_string_is_not_empty(vec)
    if op == "BitwiseNot":
        return _vector_bitwise_not(vec)
    if op == "IsTrue" or op == "IsNotFalse" or op == "IsFalse" or op == "IsNotTrue":
        bv = vec if get_vector_type(vec) == VectorType.BOOL else None
        if bv is None:
            raise TypeError(
                f"IS TRUE/IS FALSE requires a boolean expression; got {vec.__class__.__name__!r}"
            )
        if op == "IsTrue":
            return bv.equals(True)
        if op == "IsNotFalse":
            return bv.not_equals(False)
        if op == "IsFalse":
            return bv.equals(False)
        if op == "IsNotTrue":
            return bv.not_equals(True)
    raise NotImplementedError(f"evaluate_draken: unsupported unary op {op!r}")


cdef bint _is_temporal_type(orso_type):
    """Check if an OrsoType is DATE or TIMESTAMP."""
    if orso_type is None:
        return False
    return orso_type == _OrsoTypes.DATE or orso_type == _OrsoTypes.TIMESTAMP


cdef _validate_temporal_comparison(left_node, right_node, op):
    """
    Validate that temporal comparisons have literals explicitly cast.

    When comparing temporal and non-temporal operands, literals must be explicitly cast.
    Temporal columns do not require casting. Both operands must have temporal types.
    """
    left_sc = getattr(left_node, "schema_column", None)
    right_sc = getattr(right_node, "schema_column", None)
    left_type = getattr(left_sc, "type", None) if left_sc is not None else None
    right_type = getattr(right_sc, "type", None) if right_sc is not None else None

    cdef bint left_is_temporal = _is_temporal_type(left_type)
    cdef bint right_is_temporal = _is_temporal_type(right_type)

    if not (left_is_temporal or right_is_temporal):
        return
    if left_is_temporal and right_is_temporal:
        return

    non_temporal_node = right_node if left_is_temporal else left_node
    non_temporal_side = "right" if left_is_temporal else "left"

    if <int>non_temporal_node.node_type != NT_IDENTIFIER:
        raise IncompatibleTypesError(
            message=f"Temporal comparison requires literals to be explicitly cast to temporal types.\n"
            f"The {non_temporal_side} side is missing an explicit CAST or :: operator.\n\n"
            f"Examples of valid syntax:\n"
            f"  - col {op} literal::DATE\n"
            f"  - col {op} literal::TIMESTAMP[ms]\n"
            f"  - col::TIMESTAMP[ms] {op} literal::DATE\n\n"
            f"Supported temporal types: DATE, TIMESTAMP[ms], TIMESTAMP[us], TIMESTAMP[s], TIMESTAMP[ns], TIMESTAMP[d]"
        )


DEF _HASH_DISPATCH_MIN_ROWS = 1024

_TARGET_HASH_CACHE = {}
_TARGET_HASH_CACHE_MAX = 128


# Helpers shared with the legacy evaluate_and_append path in
# opteryx/expression/__init__.py. They live in the parent package because they
# straddle planning concerns; we import them lazily to avoid the import cycle.
# Cached on first use; the parent package is always fully loaded by the time
# evaluate_and_append_draken runs.
_legacy_helpers = None


cdef _get_legacy_helpers():
    global _legacy_helpers
    if _legacy_helpers is None:
        from opteryx.expression import (
            prioritize_evaluation,
            should_evaluate,
            _typed_constant_vector,
        )
        _legacy_helpers = (prioritize_evaluation, should_evaluate, _typed_constant_vector)
    return _legacy_helpers


cdef _compute_target_hash(target_names, target_vecs):
    target_morsel = _Morsel.from_vectors(target_names, target_vecs)
    target_hash_view = target_morsel.hash(target_names)
    return int(target_hash_view[0])


def _try_collect_numeric_eq_predicates(node):
    """Walk an AND-only subtree and collect IDENTIFIER = LITERAL predicates on
    fixed-width numeric/temporal columns. See module docstring of original.
    """
    eligible_types = (_OrsoTypes.INTEGER, _OrsoTypes.BOOLEAN)

    preds = []
    stack = [node]
    cdef int nt
    while stack:
        n = stack.pop()
        nt = <int>n.node_type
        if nt == NT_NESTED:
            stack.append(n.centre)
            continue
        if nt == NT_AND:
            stack.append(n.left)
            stack.append(n.right)
            continue
        if nt != NT_COMPARISON_OPERATOR or n.value != "Eq":
            return None

        left, right = n.left, n.right
        if <int>left.node_type == NT_IDENTIFIER and <int>right.node_type == NT_LITERAL:
            ident_node, lit_node = left, right
        elif <int>right.node_type == NT_IDENTIFIER and <int>left.node_type == NT_LITERAL:
            ident_node, lit_node = right, left
        else:
            return None

        sc = getattr(ident_node, "schema_column", None)
        if sc is None:
            return None
        col_type = getattr(sc, "type", None)
        if col_type not in eligible_types:
            return None
        lit_val = lit_node.value
        if lit_val is None:
            return None

        preds.append((sc.identity, sc.name.encode(), lit_val, col_type))

    if len(preds) < 3:
        return None
    return preds


cdef _evaluate_numeric_eq_via_hash(preds, morsel):
    """Evaluate a chain of IDENTIFIER = LITERAL predicates via a single hash."""
    cdef Py_ssize_t num_rows = morsel.num_rows
    if num_rows == 0:
        return None
    if num_rows < _HASH_DISPATCH_MIN_ROWS:
        return None

    target_names = []
    target_classes = []
    target_values = []
    hash_keys = []

    for ident, name, val, _col_type in preds:
        col_vec = morsel.column(ident, name)
        if col_vec is None:
            return None
        cls = type(col_vec)
        target_names.append(name)
        target_classes.append(cls)
        target_values.append(val)
        hash_keys.append(ident)

    cache_key = (
        tuple(target_names),
        tuple(target_classes),
        tuple(target_values),
    )
    target_hash = _TARGET_HASH_CACHE.get(cache_key)
    if target_hash is None:
        target_vecs = []
        for cls, val in zip(target_classes, target_values):
            try:
                target_vecs.append(cls.from_constant(val, 1))
            except (TypeError, ValueError, OverflowError):
                return None
        try:
            target_hash = _compute_target_hash(target_names, target_vecs)
        except Exception:
            return None
        if len(_TARGET_HASH_CACHE) >= _TARGET_HASH_CACHE_MAX:
            _TARGET_HASH_CACHE.clear()
        _TARGET_HASH_CACHE[cache_key] = target_hash

    row_hashes_view = morsel.hash(hash_keys)
    return _bool_vector_from_uint64_eq(row_hashes_view, target_hash)


def evaluate_draken(node, morsel):
    cdef int node_type = <int>node.node_type

    if node_type == NT_NESTED:
        return evaluate_draken(node.centre, morsel)

    if node_type == NT_AND:
        # Look up via sys.modules so tests can monkey-patch the helper to
        # disable the fast path (see tests/unit/core/test_expression_hash_eq_fastpath.py).
        preds = _sys.modules[__name__]._try_collect_numeric_eq_predicates(node)
        if preds is not None:
            fast = _evaluate_numeric_eq_via_hash(preds, morsel)
            if fast is not None:
                return fast
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.and_vector(right)

    if node_type == NT_OR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.or_vector(right)

    if node_type == NT_NOT:
        return evaluate_draken(node.centre, morsel).not_vector()

    if node_type == NT_XOR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.xor_vector(right)

    if node_type == NT_BETWEEN:
        col = _eval_value(node.left, morsel)
        lower_val = node.right.value
        upper_val = node.centre.value
        lower_inclusive, upper_inclusive = node.value
        return draken_between(col, lower_val, upper_val, lower_inclusive, upper_inclusive)

    if node_type == NT_DNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if not result.any():
                return result
            result = result.and_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NT_CNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if result.all():
                return result
            result = result.or_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NT_LITERAL:
        val = node.value
        scalar = bool(val) if val is not None else False
        return _BoolVector.from_constant(scalar, morsel.num_rows)

    if node_type == NT_COMPARISON_OPERATOR:
        # Validate that temporal comparisons have both sides explicitly typed
        _validate_temporal_comparison(node.left, node.right, node.value)

        left = _eval_value(node.left, morsel)
        right = _eval_value(node.right, morsel)

        left_sc = getattr(node.left, "schema_column", None)
        right_sc = getattr(node.right, "schema_column", None)
        left_schema_type = getattr(left_sc, "type", None) if left_sc is not None else None
        right_schema_type = getattr(right_sc, "type", None) if right_sc is not None else None
        if (
            left_schema_type == _OrsoTypes.DATE
            or left_schema_type == _OrsoTypes.TIMESTAMP
            or right_schema_type == _OrsoTypes.DATE
            or right_schema_type == _OrsoTypes.TIMESTAMP
        ):
            if _is_scalar_value(left) and (
                left_schema_type == _OrsoTypes.DATE or left_schema_type == _OrsoTypes.TIMESTAMP
            ):
                left = _coerce_temporal_scalar_for_arrow(left, left_schema_type)
            if _is_scalar_value(right) and (
                right_schema_type == _OrsoTypes.DATE or right_schema_type == _OrsoTypes.TIMESTAMP
            ):
                right = _coerce_temporal_scalar_for_arrow(right, right_schema_type)

        if is_scalar(left) and is_scalar(right):
            scalar_result = draken_compare(
                node.value,
                left,
                right,
                left_schema_type,
                right_schema_type,
            )
            if get_vector_type(scalar_result) != VectorType.BOOL:
                raise TypeError(
                    f"evaluate_draken: scalar comparison '{node.value!r}' returned "
                    f"{scalar_result.__class__.__name__!r}, expected BoolVector"
                )
            return scalar_result
        return draken_compare(node.value, left, right, left_schema_type, right_schema_type)

    if node_type == NT_UNARY_OPERATOR:
        return _unary_draken(node.value, node.centre, morsel)

    if node_type == NT_FUNCTION:
        if node.value == "_PASSTHRU":
            return evaluate_draken(node.parameters[0], morsel)
        parameters = [_eval_value(param, morsel) for param in node.parameters]
        if len(parameters) == 0:
            parameters = [morsel.num_rows]
        result = apply_bounded_function(node, *parameters)
        if isinstance(result, list):
            result = _vector_from_sequence(result)
        if get_vector_type(result) != VectorType.BOOL:
            raise TypeError(
                f"evaluate_draken: FUNCTION node returned {result.__class__.__name__!r}, expected BoolVector"
            )
        return result

    if node_type == NT_BINARY_OPERATOR:
        result = _eval_value(node, morsel)
        if get_vector_type(result) == VectorType.BOOL:
            return result
        raise TypeError(
            f"evaluate_draken: BINARY_OPERATOR '{node.value!r}' returned non-boolean {result.__class__.__name__!r}"
        )

    raise NotImplementedError(
        f"evaluate_draken: unsupported node type {node.node_type!r} (value={node.value!r})"
    )


def evaluate_and_append_draken(nodes, morsel):
    """Evaluate `nodes` against `morsel` and append the resulting columns.

    Parity contract with opteryx.expression._evaluate_and_append_morsel:
      - Expressions are processed in dependency order (`prioritize_evaluation`):
        non-EVALUATED-dependent first, dependent second. Safe no-op when
        callers (aggregate, hashed_inner_join) pass independent expressions.
      - LITERAL nodes use schema-typed constant encoding (`_typed_constant_vector`)
        when the schema type is supported, falling back to from_scalar for
        types not yet covered by the typed path.
      - Nodes that do not satisfy `should_evaluate` are skipped (matches the
        legacy filter).
      - The legacy path's `is_mask`/`create_mask` wrapping is NOT ported:
        any short result raises rather than being silently one-hot-padded.
        Per CLAUDE.md §1/9, we surface this rather than mask it.
    """
    prioritize_evaluation, should_evaluate, typed_constant_vector = _get_legacy_helpers()

    if not nodes:
        return morsel

    cdef list col_names = None
    cdef list col_vecs = None
    cdef set existing = None
    cdef bint appended = False
    cdef int node_type

    for node in prioritize_evaluation(nodes):
        if node.value == "_PASSTHRU":
            continue
        if not should_evaluate(node):
            continue
        identity = node.schema_column.identity

        if existing is None:
            existing = {n.decode() if isinstance(n, bytes) else n
                        for n in morsel.column_names}
        if identity in existing:
            continue

        if col_names is None:
            col_names = list(morsel.column_names)
            col_vecs = [morsel.column(n if isinstance(n, bytes) else n.encode())
                        for n in col_names]

        node_type = <int>node.node_type

        if node_type == NT_LITERAL:
            literal_vec = typed_constant_vector(node.value, morsel.num_rows, node.schema_column)
            if literal_vec is None:
                # Schema type not covered by the typed constant path; fall back to
                # the generic from_scalar that drives shape from the Python value.
                literal_vec = _const_scalar(node.value, morsel.num_rows)
            if literal_vec is None:
                raise TypeError(
                    f"evaluate_and_append_draken: cannot construct constant vector for "
                    f"LITERAL value {node.value!r} (type {type(node.value).__name__})."
                )
            col_names.append(identity)
            col_vecs.append(literal_vec)
            existing.add(identity)
            appended = True
            continue

        if node_type == NT_CASE:
            from opteryx.expression.evaluator.case_eval import evaluate_case
            result = evaluate_case(node, morsel)
            if not is_draken_vector(result):
                raise TypeError(
                    "evaluate_and_append_draken: CASE expression must return a Draken vector; "
                    f"got {type(result).__name__}."
                )
            col_names.append(identity)
            col_vecs.append(result)
            existing.add(identity)
            appended = True
            continue
        if node_type == NT_FUNCTION:
            parameters = []
            for param in node.parameters:
                value = _eval_value(param, morsel)
                if is_draken_vector(value):
                    parameters.append(value)
                    continue
                if isinstance(value, list) and all(is_draken_vector(v) for v in value):
                    parameters.append(value)
                    continue
                raise TypeError(
                    f"evaluate_and_append_draken: parameter for {node.value!r} "
                    f"must be a Draken vector (or list of Draken vectors); "
                    f"got {type(value).__name__}"
                )
            if len(parameters) == 0:
                parameters = [morsel.num_rows]
            result = apply_bounded_function(node, *parameters)
        else:
            result = _eval_value(node, morsel)
        if not is_draken_vector(result):
            raise TypeError(
                "evaluate_and_append_draken expected Draken vector result; "
                f"got {type(result).__name__} for expression {node.value!r}."
            )
        col_names.append(identity)
        col_vecs.append(result)
        existing.add(identity)
        appended = True

    if not appended:
        return morsel

    return _Morsel.from_vectors(col_names, col_vecs)


# ---------------------------------------------------------------------------
# Bytecode VM executor
#
# execute_bytecode() consumes the flat postfix instruction list produced by
# build_bytecode() at bind time.  It maintains a small operand stack of
# Draken vectors and dispatches on CompiledInstruction.node_type using a
# chain of C-level integer compares (Cython optimize.use_switch folds these
# into a switch statement in the generated C).
#
# Native nodes: pop `arity` vectors, push one result.
# Legacy nodes: call _eval_value(source_node, morsel), push one result.
# ---------------------------------------------------------------------------

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.ref cimport PyObject, Py_INCREF, Py_XINCREF, Py_XDECREF

# Cython 3 declares Py_INCREF/Py_XDECREF as taking `object`, not `PyObject*`.
# The bytecode executor stores raw PyObject* on its C stack, so we need direct
# C-level macros that accept pointers without going through the Python protocol.
cdef extern from "Python.h":
    void _incref "Py_INCREF" (PyObject* o)
    void _xdecref "Py_XDECREF" (PyObject* o)
from opteryx.compiled.expression.compiled_expression cimport (
    BC_AND,
    BC_CMP_LEFT_TEMPORAL,
    BC_CMP_RIGHT_TEMPORAL,
    BC_CNF,
    BC_COMPARE,
    BC_DNF,
    BC_LEGACY,
    BC_LOAD_COL,
    BC_LOAD_LIT_BOOL,
    BC_LOAD_LIT_SCALAR,
    BC_LOAD_LIT_SET,
    BC_NOT,
    BC_OR,
    BC_XOR,
    BytecodeInstr,
    CompiledBytecode,
)
from draken.morsels.morsel cimport Morsel
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.vector cimport Vector


def execute_bytecode(CompiledBytecode bc, Morsel morsel):
    """Execute a typed bytecode against `morsel`. Returns a Vector.

    No Python objects on the dispatch path: the instruction store is a C
    struct array (bc.instrs), the operand stack is a PyObject** C array,
    dispatch is a switch on the int opcode, kernel calls go through typed
    cpdef methods on Vector / BoolVector / Morsel. CLAUDE.md §2/§3.
    """
    cdef Py_ssize_t n_instrs = bc.count
    cdef Py_ssize_t cap = bc.max_stack_depth
    if cap < 1:
        cap = 1
    cdef PyObject** stack = <PyObject**>PyMem_Malloc(<size_t>(cap * sizeof(PyObject*)))
    if stack == NULL:
        raise MemoryError("execute_bytecode: failed to allocate stack")

    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i, j, base
    cdef int opcode
    cdef int arity
    cdef int flags
    cdef BytecodeInstr* slot
    cdef BoolVector b_left, b_right, b_result, b_cur
    cdef Vector v_left, v_right, v_result
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef object scalar_obj
    cdef object compare_result
    cdef object legacy_result
    cdef object left_type
    cdef object right_type

    try:
        for i in range(n_instrs):
            slot = &bc.instrs[i]
            opcode = slot.opcode

            # ----------------------------------------------------------
            # BC_LOAD_COL — typed Morsel.column dispatch (cpdef)
            # ----------------------------------------------------------
            if opcode == BC_LOAD_COL:
                v_result = morsel.column(
                    <bytes>slot.column_identity, <bytes>slot.column_name
                )
                if v_result is None:
                    raise ColumnReferencedBeforeEvaluationError(
                        column=(<bytes>slot.column_name).decode()
                    )
                Py_INCREF(v_result)
                stack[sp] = <PyObject*>v_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_BOOL — typed BoolVector.from_constant
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_BOOL:
                b_result = BoolVector.from_constant(
                    slot.bool_value != 0, num_rows
                )
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_SET — push the pre-resolved set object
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_SET:
                Py_XINCREF(slot.literal_obj)
                stack[sp] = slot.literal_obj
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_SCALAR — typed call into draken.from_scalar
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_SCALAR:
                scalar_obj = <object>slot.literal_obj
                v_result = _const_scalar(scalar_obj, num_rows)
                if v_result is None:
                    raise TypeError(
                        f"execute_bytecode: cannot construct vector for literal "
                        f"{scalar_obj!r} (type {type(scalar_obj).__name__})"
                    )
                Py_INCREF(v_result)
                stack[sp] = <PyObject*>v_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # Boolean combinators — typed BoolVector cpdef dispatch
            # ----------------------------------------------------------
            if opcode == BC_AND:
                sp -= 1
                b_right = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                sp -= 1
                b_left = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                b_result = b_left.and_vector(b_right)
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            if opcode == BC_OR:
                sp -= 1
                b_right = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                sp -= 1
                b_left = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                b_result = b_left.or_vector(b_right)
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            if opcode == BC_XOR:
                sp -= 1
                b_right = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                sp -= 1
                b_left = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                b_result = b_left.xor_vector(b_right)
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            if opcode == BC_NOT:
                sp -= 1
                b_cur = <BoolVector>stack[sp]
                Py_XDECREF(stack[sp])
                b_result = b_cur.not_vector()
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # Variadic AND/OR — DNF / CNF
            # ----------------------------------------------------------
            if opcode == BC_DNF:
                arity = slot.arity
                base = sp - arity
                b_result = <BoolVector>stack[base]
                Py_XDECREF(stack[base])
                for j in range(1, arity):
                    b_cur = <BoolVector>stack[base + j]
                    Py_XDECREF(stack[base + j])
                    if b_result.any() == 0:
                        continue   # already empty; remaining .and_vector
                                   # short-circuits — but we must still
                                   # drain refs from the stack slots above
                    b_result = b_result.and_vector(b_cur)
                sp = base
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            if opcode == BC_CNF:
                arity = slot.arity
                base = sp - arity
                b_result = <BoolVector>stack[base]
                Py_XDECREF(stack[base])
                for j in range(1, arity):
                    b_cur = <BoolVector>stack[base + j]
                    Py_XDECREF(stack[base + j])
                    if b_result.all() != 0:
                        continue
                    b_result = b_result.or_vector(b_cur)
                sp = base
                Py_INCREF(b_result)
                stack[sp] = <PyObject*>b_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_COMPARE — typed draken_compare (cpdef)
            # ----------------------------------------------------------
            if opcode == BC_COMPARE:
                sp -= 1
                v_right = <Vector>stack[sp]
                Py_XDECREF(stack[sp])
                sp -= 1
                v_left = <Vector>stack[sp]
                Py_XDECREF(stack[sp])
                flags = slot.flags
                left_type = <object>slot.left_orso_type if slot.left_orso_type != NULL else None
                right_type = <object>slot.right_orso_type if slot.right_orso_type != NULL else None
                if flags != 0:
                    if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(v_left):
                        v_left = _coerce_temporal_scalar_for_arrow(v_left, left_type)
                    if (flags & BC_CMP_RIGHT_TEMPORAL) and _is_scalar_value(v_right):
                        v_right = _coerce_temporal_scalar_for_arrow(v_right, right_type)
                compare_result = draken_compare(
                    <str>slot.compare_op_str, v_left, v_right, left_type, right_type
                )
                Py_INCREF(compare_result)
                stack[sp] = <PyObject*>compare_result
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LEGACY — GIL-required fallback to the tree-walker
            # ----------------------------------------------------------
            if opcode == BC_LEGACY:
                legacy_result = _eval_value(<object>slot.source_node, morsel)
                Py_INCREF(legacy_result)
                stack[sp] = <PyObject*>legacy_result
                sp += 1
                continue

            raise NotImplementedError(
                f"execute_bytecode: unknown opcode {opcode}"
            )

        if sp != 1:
            raise ValueError(
                f"execute_bytecode: expected 1 result on stack, got {sp}"
            )

        # Transfer the single result out of the stack with no net refcount
        # change: the stack slot's strong ref becomes the returned value's
        # strong ref. Setting sp=0 prevents the finally cleanup from
        # double-decreffing.
        v_result = <Vector>stack[0]
        Py_XDECREF(stack[0])
        sp = 0
        return v_result
    finally:
        for j in range(sp):
            Py_XDECREF(stack[j])
        PyMem_Free(stack)


__all__ = [
    "draken_compare",
    "evaluate_and_append_draken",
    "evaluate_draken",
    "execute_bytecode",
]
