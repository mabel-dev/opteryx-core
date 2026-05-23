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
from opteryx.compiled.vector_ops import vector_bitwise_not as _vector_bitwise_not
from opteryx.compiled.nanobind.vector_accessors import (
    vector_string_is_empty as _vector_string_is_empty,
    vector_string_is_not_empty as _vector_string_is_not_empty,
)
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError, IncompatibleTypesError
from opteryx.types import OrsoTypes as _OrsoTypes
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


# Imports from draken are safe at module level — draken does not import opteryx.expression.
from draken.vectors.bool_vector import BoolVector as _BoolVector
from draken.vectors.integer64_vector import Integer64Vector as _Integer64Vector
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
            key_vec = _Integer64Vector.from_constant(int(right_val), 1)
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


cdef _unary_op_kernel(str op, vec):
    """Apply a unary op to a pre-evaluated vector (bytecode executor path)."""
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
    raise NotImplementedError(f"_unary_op_kernel: unsupported unary op {op!r}")


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

from opteryx.compiled.expression.compiled_expression cimport (
    BC_AND,
    BC_BETWEEN,
    BC_BINARY_OP,
    BC_CAST,
    BC_CMP_LEFT_TEMPORAL,
    BC_CMP_RIGHT_TEMPORAL,
    BC_CNF,
    BC_COMPARE,
    BC_DNF,
    BC_EXTRACTION,
    BC_FUNCTION,
    BC_LEGACY,
    BC_LOAD_COL,
    BC_LOAD_LIT_BOOL,
    BC_LOAD_LIT_SCALAR,
    BC_LOAD_LIT_SET,
    BC_NOT,
    BC_OR,
    BC_UNARY_OP,
    BC_XOR,
    BytecodeInstr,
    CompiledBytecode,
)
from libc.stdint cimport uint8_t, int8_t, uintptr_t, uint32_t

from draken.core.buffers cimport DrakenVector
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

from draken.morsels.morsel cimport Morsel
from draken.vectors.bool_vector cimport (
    BoolVector,
    bool_vector_from_bits,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    c_get_bitmap_ptrs,
)
from draken.vectors.vector cimport Vector, simd_popcount


# ---------------------------------------------------------------------------
# C-callable interface — worker item and global function pointer.
# Declared extern here; the global and setter are defined in bytecode_worker.cpp.
# ---------------------------------------------------------------------------

cdef extern from "bytecode_worker.h" nogil:
    ctypedef struct BytecodeWorkerItem:
        const void*  instrs
        size_t       n_instrs
        const void*  col_cache
        uint8_t**    bitmaps
        uint8_t**    null_bitmaps
        int8_t*      slot_has_null
        size_t       n_slots
        size_t       nbytes
        size_t       n_rows
        int          error_code

    ctypedef int (*opteryx_worker_fn_t)(BytecodeWorkerItem*)
    opteryx_worker_fn_t opteryx_worker_fn
    void opteryx_set_worker_fn(opteryx_worker_fn_t fn)


# ---------------------------------------------------------------------------
# Bitmap VM — three-phase GIL-free predicate evaluation
#
# Phase 1 (_execute_bytecode_prepass): GIL held.
#   Resolves BC_LOAD_COL columns; mallocs scratch bitmap buffers.
# Phase 2 (c_execute_bytecode_inner): noexcept nogil.
#   Operates entirely on uint8_t* scratch bitmaps; no Python objects.
# Phase 3 (_execute_bytecode_postpass): GIL held.
#   Wraps the result bitmap into a BoolVector for Python callers.
#
# Only runs when bc.is_pure_bitmap is True — bytecodes containing only
# BC_LOAD_LIT_BOOL, BC_LOAD_COL (BoolVector columns), and boolean
# combinators (AND/OR/XOR/NOT/DNF/CNF).
# ---------------------------------------------------------------------------

ctypedef struct ColCache:
    uint8_t*        data       # ptr to BoolVector bitmap data (unified view)
    uint8_t*        null_bm    # ptr to validity bitmap (NULL = no nulls)
    const uint32_t* sel        # per-logical-row selection into `data`
    bint            is_bool    # True if the column resolved to a BoolVector


cdef int _execute_bytecode_prepass(
    CompiledBytecode bc,
    Morsel morsel,
    Py_ssize_t num_rows,
    ColCache* col_cache,
    uint8_t** bitmaps,
    uint8_t** null_bitmaps,
    int8_t* slot_has_null,
    Py_ssize_t n_slots,
    Py_ssize_t nbytes,
    list anchors,
) except -1:
    """GIL-held pre-pass: resolve columns and malloc scratch bitmap buffers.

    Returns -1 (exception) if any BC_LOAD_COL column is not a BoolVector
    (caller must fall back to execute_bytecode).  On success returns 0.
    """
    cdef Py_ssize_t j, k
    cdef BytecodeInstr* slot
    cdef Vector v
    cdef BoolVector bv
    cdef uint8_t* p
    cdef DrakenVector* uv

    # Allocate n_slots + 2 bitmap buffers:
    #   [0 .. n_slots-1] = stack slots
    #   [n_slots]        = primary scratch for binary ops
    #   [n_slots+1]      = secondary scratch for DNF/CNF fold
    for j in range(n_slots + 2):
        p = <uint8_t*>malloc(nbytes)
        if p == NULL:
            raise MemoryError("evaluate_bitmap: failed to allocate bitmap buffer")
        memset(p, 0, nbytes)
        bitmaps[j] = p

        p = <uint8_t*>malloc(nbytes)
        if p == NULL:
            raise MemoryError("evaluate_bitmap: failed to allocate null bitmap buffer")
        memset(p, 0, nbytes)
        null_bitmaps[j] = p

        slot_has_null[j] = 0

    # Resolve BC_LOAD_COL instructions
    for k in range(bc.count):
        slot = &bc.instrs[k]
        if slot.opcode != BC_LOAD_COL:
            col_cache[k].is_bool = False
            continue

        v = morsel.column(<bytes>slot.column_identity, <bytes>slot.column_name)
        if not isinstance(v, BoolVector):
            return -1  # not a BoolVector — caller must fall back

        bv = <BoolVector>v
        anchors.append(bv)  # keep alive during inner loop
        uv = bv.unified()
        col_cache[k].is_bool = True
        col_cache[k].data = <uint8_t*>uv.data
        col_cache[k].null_bm = uv.validity
        col_cache[k].sel = uv.selection

    return 0


cdef int c_execute_bytecode_inner(
    BytecodeInstr* instrs,
    Py_ssize_t n_instrs,
    ColCache* col_cache,
    uint8_t** bitmaps,
    uint8_t** null_bitmaps,
    int8_t* slot_has_null,
    Py_ssize_t n_slots,
    Py_ssize_t nbytes,
    Py_ssize_t num_rows,
) noexcept nogil:
    """Nogil VM inner loop for pure-bitmap bytecodes.

    Operates entirely on pre-allocated uint8_t* scratch buffers — no Python
    objects, no GIL. Stack slots are indices into the bitmaps/null_bitmaps
    arrays. Binary ops write to bitmaps[n_slots] (scratch) then swap pointers.

    Returns 0 on success, 1 if an unexpected opcode is encountered.
    """
    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i, j, base, arity
    cdef int opcode
    cdef BytecodeInstr* slot
    cdef uint8_t* tmp_ptr
    cdef bint had_null
    cdef Py_ssize_t scratch0 = n_slots
    cdef Py_ssize_t scratch1 = n_slots + 1
    cdef Py_ssize_t popcount_val

    for i in range(n_instrs):
        slot = &instrs[i]
        opcode = slot.opcode

        # ------------------------------------------------------------------
        # BC_LOAD_LIT_BOOL — fill bitmap slot with constant pattern
        # ------------------------------------------------------------------
        if opcode == BC_LOAD_LIT_BOOL:
            if slot.bool_value != 0:
                memset(bitmaps[sp], 0xFF, nbytes)
                if (num_rows & 7) != 0:
                    bitmaps[sp][nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
            else:
                memset(bitmaps[sp], 0x00, nbytes)
            slot_has_null[sp] = 0
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_LOAD_COL — copy pre-resolved BoolVector bitmap into stack slot
        # ------------------------------------------------------------------
        if opcode == BC_LOAD_COL:
            if not col_cache[i].is_bool:
                return 1  # unexpected non-bool column
            memset(bitmaps[sp], 0, nbytes)
            for j in range(num_rows):
                base = col_cache[i].sel[j]
                if (col_cache[i].data[base >> 3] >> (base & 7)) & 1:
                    bitmaps[sp][j >> 3] |= <uint8_t>(1 << (j & 7))
            if col_cache[i].null_bm != NULL:
                memcpy(null_bitmaps[sp], col_cache[i].null_bm, nbytes)
                slot_has_null[sp] = 1
            else:
                slot_has_null[sp] = 0
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_AND — binary AND with pointer-swap to avoid aliasing
        # ------------------------------------------------------------------
        if opcode == BC_AND:
            sp -= 2
            had_null = c_and_bitmap(
                bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                null_bitmaps[scratch0],
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_OR — binary OR with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_OR:
            sp -= 2
            had_null = c_or_bitmap(
                bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                null_bitmaps[scratch0],
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_XOR — binary XOR with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_XOR:
            sp -= 2
            had_null = c_xor_bitmap(
                bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                null_bitmaps[scratch0],
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_NOT — unary NOT with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_NOT:
            sp -= 1
            had_null = c_not_bitmap(
                bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                null_bitmaps[scratch0],
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_DNF — variadic AND fold (uses scratch0 as accumulator, scratch1
        # as output; alternates to avoid aliasing)
        # ------------------------------------------------------------------
        if opcode == BC_DNF:
            arity = slot.arity
            base = sp - arity
            # initialise accumulator from bitmaps[base]
            memcpy(bitmaps[scratch0], bitmaps[base], nbytes)
            memcpy(null_bitmaps[scratch0], null_bitmaps[base], nbytes)
            slot_has_null[scratch0] = slot_has_null[base]
            for j in range(1, arity):
                # short-circuit: if accumulator is all-false, skip the rest
                popcount_val = <Py_ssize_t>simd_popcount(bitmaps[scratch0], <size_t>nbytes)
                if popcount_val == 0 and not slot_has_null[scratch0]:
                    break
                had_null = c_and_bitmap(
                    bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
                    null_bitmaps[scratch1],
                    nbytes, num_rows,
                )
                # swap scratch0 <-> scratch1 (accumulate into scratch0)
                tmp_ptr = bitmaps[scratch0]
                bitmaps[scratch0] = bitmaps[scratch1]
                bitmaps[scratch1] = tmp_ptr
                tmp_ptr = null_bitmaps[scratch0]
                null_bitmaps[scratch0] = null_bitmaps[scratch1]
                null_bitmaps[scratch1] = tmp_ptr
                slot_has_null[scratch0] = had_null
            # swap accumulator into bitmaps[base]
            tmp_ptr = bitmaps[base]
            bitmaps[base] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[base]
            null_bitmaps[base] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[base] = slot_has_null[scratch0]
            sp = base + 1
            continue

        # ------------------------------------------------------------------
        # BC_CNF — variadic OR fold
        # ------------------------------------------------------------------
        if opcode == BC_CNF:
            arity = slot.arity
            base = sp - arity
            memcpy(bitmaps[scratch0], bitmaps[base], nbytes)
            memcpy(null_bitmaps[scratch0], null_bitmaps[base], nbytes)
            slot_has_null[scratch0] = slot_has_null[base]
            for j in range(1, arity):
                # short-circuit: if accumulator is all-true, skip the rest
                popcount_val = <Py_ssize_t>simd_popcount(bitmaps[scratch0], <size_t>nbytes)
                if popcount_val == num_rows and not slot_has_null[scratch0]:
                    break
                had_null = c_or_bitmap(
                    bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
                    null_bitmaps[scratch1],
                    nbytes, num_rows,
                )
                tmp_ptr = bitmaps[scratch0]
                bitmaps[scratch0] = bitmaps[scratch1]
                bitmaps[scratch1] = tmp_ptr
                tmp_ptr = null_bitmaps[scratch0]
                null_bitmaps[scratch0] = null_bitmaps[scratch1]
                null_bitmaps[scratch1] = tmp_ptr
                slot_has_null[scratch0] = had_null
            tmp_ptr = bitmaps[base]
            bitmaps[base] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[base]
            null_bitmaps[base] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[base] = slot_has_null[scratch0]
            sp = base + 1
            continue

        return 1  # unexpected opcode

    return 0


cdef int _c_bytecode_worker_trampoline(BytecodeWorkerItem* item) noexcept nogil:
    """C-callable trampoline for moodycamel worker threads.

    Calls c_execute_bytecode_inner with no GIL held. On return, item.error_code
    is 0 (success, result at item.bitmaps[0]) or 1 (unexpected opcode; caller
    must re-run via execute_bytecode from a GIL-held thread).
    """
    cdef int rc = c_execute_bytecode_inner(
        <BytecodeInstr*>item.instrs,
        <Py_ssize_t>item.n_instrs,
        <ColCache*>item.col_cache,
        item.bitmaps,
        item.null_bitmaps,
        item.slot_has_null,
        <Py_ssize_t>item.n_slots,
        <Py_ssize_t>item.nbytes,
        <Py_ssize_t>item.n_rows,
    )
    item.error_code = rc
    return rc


def get_bytecode_worker_fn_ptr():
    """Return the trampoline function pointer as a Python int.

    Allows C++ code loaded via ctypes to retrieve the opteryx_worker_fn
    address without a Python callback round-trip. Value is stable for the
    lifetime of the process.
    """
    return <uintptr_t>opteryx_worker_fn


cdef BoolVector _execute_bytecode_postpass(
    uint8_t* result_bitmap,
    uint8_t* result_null,
    bint has_null,
    Py_ssize_t num_rows,
):
    """Wrap a raw bitmap into a BoolVector. GIL held."""
    return bool_vector_from_bits(
        result_bitmap,
        result_null if has_null else NULL,
        num_rows,
    )


cpdef BoolVector evaluate_bitmap(CompiledBytecode bc, Morsel morsel):
    """GIL-free predicate evaluation path for pure-bitmap bytecodes.

    Allocates scratch buffers (GIL held), runs the nogil bitmap VM, then
    wraps the result bitmap into a BoolVector. Falls back to execute_bytecode
    if any BC_LOAD_COL column is not a BoolVector at runtime.
    """
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef Py_ssize_t n_slots = bc.max_stack_depth
    if n_slots < 1:
        n_slots = 1

    # Allocate ColCache (one entry per instruction) on the C heap
    cdef ColCache* col_cache = <ColCache*>malloc(bc.count * sizeof(ColCache))
    if col_cache == NULL:
        raise MemoryError("evaluate_bitmap: failed to allocate ColCache")

    # Allocate bitmap pointer arrays (n_slots + 2 slots: stack + 2 scratch)
    cdef uint8_t** bitmaps = <uint8_t**>malloc((n_slots + 2) * sizeof(uint8_t*))
    cdef uint8_t** null_bitmaps = <uint8_t**>malloc((n_slots + 2) * sizeof(uint8_t*))
    cdef int8_t* slot_has_null = <int8_t*>malloc((n_slots + 2) * sizeof(int8_t))
    if bitmaps == NULL or null_bitmaps == NULL or slot_has_null == NULL:
        free(col_cache); free(bitmaps); free(null_bitmaps); free(slot_has_null)
        raise MemoryError("evaluate_bitmap: failed to allocate stack arrays")

    cdef list anchors = []  # keeps BoolVector Python objects alive during inner loop
    cdef int rc
    cdef Py_ssize_t j

    try:
        rc = _execute_bytecode_prepass(
            bc, morsel, num_rows,
            col_cache, bitmaps, null_bitmaps, slot_has_null,
            n_slots, nbytes, anchors,
        )
        if rc == -1:
            # A BC_LOAD_COL column is not a BoolVector — fall back
            return execute_bytecode(bc, morsel)

        with nogil:
            rc = c_execute_bytecode_inner(
                bc.instrs, bc.count,
                col_cache, bitmaps, null_bitmaps, slot_has_null,
                n_slots, nbytes, num_rows,
            )

        if rc != 0:
            # Unexpected opcode — fall back (shouldn't happen if is_pure_bitmap is correct)
            return execute_bytecode(bc, morsel)

        return _execute_bytecode_postpass(
            bitmaps[0],
            null_bitmaps[0],
            slot_has_null[0] != 0,
            num_rows,
        )
    finally:
        for j in range(n_slots + 2):
            free(bitmaps[j])
            free(null_bitmaps[j])
        free(col_cache)
        free(bitmaps)
        free(null_bitmaps)
        free(slot_has_null)


cpdef execute_bytecode(CompiledBytecode bc, Morsel morsel):
    """Execute a typed bytecode against `morsel`. Returns a Vector.

    If bc.is_pure_bitmap, delegates to evaluate_bitmap (nogil bitmap path).
    Otherwise uses a pre-allocated Python list as the operand stack — Cython's
    list item assignment manages refcounts via PyObject_SetItem, eliminating
    manual Py_INCREF/XDECREF. CLAUDE.md §2/§3.

    Promoted to cpdef so callers within the _operators compilation unit dispatch
    at C level — no Python function call boundary. The Python `def` wrapper is
    still synthesised by Cython for external callers (Filter, tests, etc.).
    """
    if bc.is_pure_bitmap:
        return evaluate_bitmap(bc, morsel)
    cdef Py_ssize_t n_instrs = bc.count
    cdef Py_ssize_t cap = bc.max_stack_depth
    if cap < 1:
        cap = 1

    cdef list stack = [None] * cap

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
    cdef object func_args
    cdef Py_ssize_t func_base
    cdef object extr_key
    cdef str extr_op
    cdef object key_vec

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
            stack[sp] = v_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LOAD_LIT_BOOL — typed BoolVector.from_constant
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_BOOL:
            b_result = BoolVector.from_constant(
                slot.bool_value != 0, num_rows
            )
            stack[sp] = b_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LOAD_LIT_SET — push the pre-resolved set object
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_SET:
            stack[sp] = <object>slot.literal_obj
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
            stack[sp] = v_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # Boolean combinators — typed BoolVector cpdef dispatch
        # ----------------------------------------------------------
        if opcode == BC_AND:
            sp -= 1
            b_right = <BoolVector>stack[sp]
            sp -= 1
            b_left = <BoolVector>stack[sp]
            b_result = b_left.and_vector(b_right)
            stack[sp] = b_result
            sp += 1
            continue

        if opcode == BC_OR:
            sp -= 1
            b_right = <BoolVector>stack[sp]
            sp -= 1
            b_left = <BoolVector>stack[sp]
            b_result = b_left.or_vector(b_right)
            stack[sp] = b_result
            sp += 1
            continue

        if opcode == BC_XOR:
            sp -= 1
            b_right = <BoolVector>stack[sp]
            sp -= 1
            b_left = <BoolVector>stack[sp]
            b_result = b_left.xor_vector(b_right)
            stack[sp] = b_result
            sp += 1
            continue

        if opcode == BC_NOT:
            sp -= 1
            b_cur = <BoolVector>stack[sp]
            b_result = b_cur.not_vector()
            stack[sp] = b_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # Variadic AND/OR — DNF / CNF
        # ----------------------------------------------------------
        if opcode == BC_DNF:
            arity = slot.arity
            base = sp - arity
            b_result = <BoolVector>stack[base]
            for j in range(1, arity):
                b_cur = <BoolVector>stack[base + j]
                if b_result.any() == 0:
                    continue
                b_result = b_result.and_vector(b_cur)
            sp = base
            stack[sp] = b_result
            sp += 1
            continue

        if opcode == BC_CNF:
            arity = slot.arity
            base = sp - arity
            b_result = <BoolVector>stack[base]
            for j in range(1, arity):
                b_cur = <BoolVector>stack[base + j]
                if b_result.all() != 0:
                    continue
                b_result = b_result.or_vector(b_cur)
            sp = base
            stack[sp] = b_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_COMPARE — typed draken_compare (cpdef)
        # ----------------------------------------------------------
        if opcode == BC_COMPARE:
            sp -= 1
            v_right = <Vector>stack[sp]
            sp -= 1
            v_left = <Vector>stack[sp]
            flags = slot.flags
            left_type = <object>slot.left_orso_type if slot.left_orso_type != NULL else None
            right_type = <object>slot.right_orso_type if slot.right_orso_type != NULL else None
            if flags != 0:
                if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(v_left):
                    v_left = _coerce_temporal_scalar_for_arrow(v_left, left_type)
                if (flags & BC_CMP_RIGHT_TEMPORAL) and _is_scalar_value(v_right):
                    v_right = _coerce_temporal_scalar_for_arrow(v_right, right_type)
            if slot.op_code != OP_UNKNOWN:
                compare_result = draken_compare_int(
                    slot.op_code, v_left, v_right, left_type, right_type
                )
            else:
                compare_result = draken_compare(
                    <str>slot.compare_op_str, v_left, v_right, left_type, right_type
                )
            stack[sp] = compare_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_BETWEEN — typed draken_between (cpdef)
        # ----------------------------------------------------------
        if opcode == BC_BETWEEN:
            sp -= 1
            v_left = <Vector>stack[sp]
            compare_result = draken_between(
                v_left,
                <object>slot.literal_obj if slot.literal_obj != NULL else None,
                <object>slot.literal_obj2 if slot.literal_obj2 != NULL else None,
                slot.op_code != 0,
                slot.bool_value != 0,
            )
            stack[sp] = compare_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_BINARY_OP — arithmetic / string / date ops on two vecs
        # ----------------------------------------------------------
        if opcode == BC_BINARY_OP:
            sp -= 1
            v_right = <Vector>stack[sp]
            sp -= 1
            v_left = <Vector>stack[sp]
            left_type = <object>slot.left_orso_type if slot.left_orso_type != NULL else None
            right_type = <object>slot.right_orso_type if slot.right_orso_type != NULL else None
            legacy_result = _binary_op_from_vecs(
                <str>slot.compare_op_str, v_left, v_right,
                left_type, right_type, num_rows,
            )
            stack[sp] = legacy_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_UNARY_OP — IS NULL / IS NOT NULL / bitwise-not / etc.
        # ----------------------------------------------------------
        if opcode == BC_UNARY_OP:
            sp -= 1
            v_left = <Vector>stack[sp]
            legacy_result = _unary_op_kernel(<str>slot.compare_op_str, v_left)
            stack[sp] = legacy_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_FUNCTION — call pre-resolved kernel callable
        # ----------------------------------------------------------
        if opcode == BC_FUNCTION:
            arity = slot.arity
            if arity == 0:
                legacy_result = (<object>slot.callable_ref)(num_rows)
            else:
                func_base = sp - arity
                func_args = []
                for j in range(arity):
                    func_args.append(stack[func_base + j])
                sp = func_base
                legacy_result = (<object>slot.callable_ref)(*func_args)
            stack[sp] = legacy_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_EXTRACTION — Arrow / LongArrow / MapAccess
        # ----------------------------------------------------------
        if opcode == BC_EXTRACTION:
            sp -= 1
            v_left = <Vector>stack[sp]
            extr_key = <object>slot.literal_obj if slot.literal_obj != NULL else None
            extr_op = <str>slot.compare_op_str
            if extr_op == "MapAccess":
                from opteryx.expression.binary_operators import MapAccessOp
                key_vec = _Integer64Vector.from_constant(int(extr_key), 1)
                legacy_result = MapAccessOp(v_left, key_vec)
            elif extr_op == "Arrow":
                from opteryx.expression.binary_operators import ArrowOp
                key_vec = _StringVector.from_constant(extr_key, 1)
                legacy_result = ArrowOp(v_left, key_vec)
            else:
                from opteryx.expression.binary_operators import LongArrowOp
                key_vec = _StringVector.from_constant(extr_key, 1)
                legacy_result = LongArrowOp(v_left, key_vec)
            stack[sp] = legacy_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_CAST — pre-resolved cast closure, pop 1 push 1
        # ----------------------------------------------------------
        if opcode == BC_CAST:
            sp -= 1
            v_left = <Vector>stack[sp]
            legacy_result = (<object>slot.callable_ref)(v_left)
            stack[sp] = legacy_result
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LEGACY — GIL-required fallback to the tree-walker
        # ----------------------------------------------------------
        if opcode == BC_LEGACY:
            legacy_result = _eval_value(<object>slot.source_node, morsel)
            stack[sp] = legacy_result
            sp += 1
            continue

        raise NotImplementedError(
            f"execute_bytecode: unknown opcode {opcode}"
        )

    if sp != 1:
        raise ValueError(
            f"execute_bytecode: expected 1 result on stack, got {sp}"
        )

    return <Vector>stack[0]


# Wire the trampoline into the global function pointer so C++ worker threads
# can call it without holding the GIL. Done once at module import time.
opteryx_set_worker_fn(_c_bytecode_worker_trampoline)


