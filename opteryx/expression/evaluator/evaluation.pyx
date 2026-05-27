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
import decimal as _decimal_eval
import sys as _sys

from opteryx.compiled.structures.carchar_set import CarcharSetWrapper as _CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet as _PerfectHashSet
from opteryx.compiled.nanobind.vector_bitwise import vector_bitwise_not as _vector_bitwise_not
from opteryx.compiled.nanobind.vector_accessors import (
    vector_string_is_empty as _vector_string_is_empty,
    vector_string_is_not_empty as _vector_string_is_not_empty,
)
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError, IncompatibleTypesError
from opteryx.types import OrsoTypes as _OrsoTypes
from opteryx.types._datetime_conversion import timestamp_to_int64_us as _ts_to_us
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


# Imports from draken are safe at module level — draken does not import opteryx.expression.
from draken.vectors.bool_vector import BoolVector as _BoolVector
from draken.morsels.morsel import Morsel as _Morsel
import draken.draken_native as _draken_native
from opteryx.compiled.nanobind.vector_bool_ops import vector_uint64_eq_scalar as _vector_uint64_eq_scalar

# ---------------------------------------------------------------------------
# C-level imports needed by the native bitmap helpers and unary operators.
# Must appear before any cdef/cpdef that uses these types.
# The execute_bytecode section at the bottom of this file repeats some of
# these; duplicates are harmless (Cython deduplicates internally).
# ---------------------------------------------------------------------------
from libc.stdint cimport uint8_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libc.stddef cimport size_t
from draken.core.buffers cimport DrakenVector
from draken.vectors.bool_vector cimport (
    BoolVector,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport simd_popcount

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

# Truth-test op codes for _bv_truth_test_native.
DEF _BV_IS_TRUE = 0
DEF _BV_IS_FALSE = 1
DEF _BV_IS_NOT_TRUE = 2
DEF _BV_IS_NOT_FALSE = 3

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)

# Cached OrsoTypes sentinels — used to reconstruct Python type objects from
# BC_TYPE_DATE / BC_TYPE_TIMESTAMP int codes on the AnyOp/temporal-coercion paths.
_OrsoTypes_DATE = _OrsoTypes.DATE
_OrsoTypes_TIMESTAMP = _OrsoTypes.TIMESTAMP

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


cdef Vector _scalar_to_draken_constant(object value, Py_ssize_t n):
    """Convert a Python scalar literal to a Draken constant Vector of length n.

    Dispatches to the appropriate draken_native typed constructor. Raises
    TypeError for unrecognised types. Booleans must be handled by the caller
    before reaching this function (use BoolVector.from_constant instead).
    """
    cdef long long ordinal, us
    if value is None:
        return Vector(_draken_native.vector_null_from_length(n))
    if isinstance(value, int):
        return Vector(_draken_native.vector_from_constant(value, n))
    if isinstance(value, float):
        return Vector(_draken_native.vector_float64_from_constant(value, n))
    if isinstance(value, str):
        return Vector(_draken_native.vector_varchar_from_constant(value, n))
    if isinstance(value, bytes):
        return Vector(_draken_native.vector_varchar_from_constant(value.decode("utf-8", "replace"), n))
    if isinstance(value, _decimal_eval.Decimal):
        sign, digits, exponent = value.as_tuple()
        scale = max(0, -int(exponent))
        precision = max(len(digits), scale + 1)
        return Vector(_draken_native.vector_decimal_from_constant(value, n, precision, scale))
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        ordinal = (value - _EPOCH_DATE).days
        int_vec = _draken_native.vector_from_constant(ordinal, n)
        return Vector(_draken_native.vector_reinterpret_as_date32(int_vec))
    if isinstance(value, datetime.datetime):
        return Vector(_draken_native.vector_timestamp_from_constant(value, n))
    raise TypeError(
        f"_scalar_to_draken_constant: cannot construct Draken vector for literal "
        f"{value!r} (type {type(value).__name__})"
    )


# Module-level caches for lazy imports used in _eval_value native helpers.
# Populated on first use; avoids repeated `from x import y` overhead.
_cast_factory_fn = None
_binary_ops_fn = None


cdef _eval_cast_draken(node, morsel):
    """Evaluate a CAST / TRY_CAST node — no _inner_evaluate round-trip."""
    global _cast_factory_fn
    if _cast_factory_fn is None:
        from opteryx.expression.casts import cast as _cast_import
        _cast_factory_fn = _cast_import

    source = _eval_value(node.left, morsel)
    target_type = node.value[4:] if (<str>node.value).startswith("TRY_") else <str>node.value
    unit = None
    if target_type == "_TIMESTAMP_NS":
        target_type = "TIMESTAMP"; unit = "ns"
    elif target_type == "_TIMESTAMP_MS":
        target_type = "TIMESTAMP"; unit = "ms"
    elif target_type == "_TIMESTAMP_S":
        target_type = "TIMESTAMP"; unit = "s"
    elif target_type == "_TIMESTAMP_US":
        target_type = "TIMESTAMP"; unit = "us"
    elif target_type == "_TIMESTAMP_DAYS":
        target_type = "TIMESTAMP"; unit = "days"

    params = []
    if node.parameters:
        params = [_eval_value(param, morsel) for param in node.parameters]

    kernel = _cast_factory_fn(None, target_type, tuple(params), unit=unit)
    result = kernel(source)
    if not is_draken_vector(result):
        raise TypeError(
            f"_eval_cast_draken: CAST returned {type(result).__name__!r}; expected Draken vector"
        )
    return result


cdef _eval_function_draken(node, morsel):
    """Evaluate a FUNCTION node — no _inner_evaluate round-trip."""
    if node.value == "_PASSTHRU":
        return _eval_value(node.parameters[0], morsel)

    parameters = []
    for param in node.parameters:
        value = _eval_value(param, morsel)
        if is_draken_vector(value):
            if getattr(value, "_nb", None) is None:
                value = Vector(value)
            parameters.append(value)
            continue
        if isinstance(value, list) and all(is_draken_vector(v) for v in value):
            parameters.append(value)
            continue
        raise TypeError(
            f"_eval_function_draken: parameter for {node.value!r} must be a Draken vector "
            f"(or list of vectors); got {type(value).__name__!r}"
        )
    if len(parameters) == 0:
        parameters = [morsel.num_rows]

    result = apply_bounded_function(node, *parameters)
    if not is_draken_vector(result):
        raise TypeError(
            f"_eval_function_draken: FUNCTION {node.value!r} returned {type(result).__name__!r}; "
            f"expected Draken vector"
        )
    return result


cdef _eval_binary_op_residual(node, morsel):
    """Evaluate BINARY_OPERATOR cases not covered by _eval_binary_op_draken
    (string concat, integer divide, bitwise ops, etc.) — no _inner_evaluate.
    """
    global _binary_ops_fn
    if _binary_ops_fn is None:
        from opteryx.expression.binary_operators import binary_operations as _bo
        _binary_ops_fn = _bo

    left = _eval_value(node.left, morsel)
    right = _eval_value(node.right, morsel)
    result = _binary_ops_fn(
        left, node.left.schema_column.type,
        node.value,
        right, node.right.schema_column.type,
    )
    if not is_draken_vector(result):
        raise TypeError(
            f"_eval_binary_op_residual: BINARY_OPERATOR {node.value!r} returned "
            f"{type(result).__name__!r}; expected Draken vector"
        )
    return result


def _eval_value(node, morsel):
    cdef int node_type = <int>node.node_type

    if node_type == NT_LITERAL:
        if isinstance(node.value, bool):
            return _BoolVector.from_constant(node.value, morsel.num_rows)

        if isinstance(node.value, (_CarcharSetWrapper, _PerfectHashSet)):
            return node.value

        return _scalar_to_draken_constant(node.value, morsel.num_rows)

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
            key_vec = _draken_native.vector_from_constant(int(right_val), 1)
            result = MapAccessOp(left_vec, key_vec)
            if is_draken_vector(result):
                return result
            raise TypeError(
                f"MapAccessOp expected Draken vector result; got {type(result).__name__}."
            )

        if op == "Arrow" or op == "LongArrow":
            from opteryx.expression.binary_operators import ArrowOp, LongArrowOp
            key_vec = _draken_native.vector_from_string_sequence(
                [right_val if isinstance(right_val, bytes) else right_val.encode("utf-8")]
            )
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

        if node_type == NT_CAST:
            return _eval_cast_draken(node, morsel)
        if node_type == NT_FUNCTION:
            return _eval_function_draken(node, morsel)
        # NT_BINARY_OPERATOR residual: string concat, bitwise, integer divide, etc.
        return _eval_binary_op_residual(node, morsel)

    return evaluate_draken(node, morsel)


cdef _unary_draken(str op, centre_node, morsel):
    cdef BoolVector is_null_bv
    cdef DrakenVector* is_null_dv
    cdef uint32_t nn_rows
    cdef Py_ssize_t nn_nbytes
    cdef BoolVector _truth_bv
    cdef Py_ssize_t _t_rows, _t_nbytes
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        is_null_bv = <BoolVector>_is_null_as_boolvector(vec)
        is_null_dv = is_null_bv.unified()
        nn_rows = is_null_dv.length
        nn_nbytes = (<Py_ssize_t>nn_rows + 7) >> 3
        return _bv_not_native(is_null_bv, nn_nbytes, nn_rows)
    if op == "IsEmpty":
        return _BoolVector(_vector_string_is_empty(_nb_vec_unwrap(vec)))
    if op == "IsNotEmpty":
        return _BoolVector(_vector_string_is_not_empty(_nb_vec_unwrap(vec)))
    if op == "BitwiseNot":
        return Vector(_vector_bitwise_not(_nb_vec_unwrap(vec)))
    if op == "IsTrue" or op == "IsNotFalse" or op == "IsFalse" or op == "IsNotTrue":
        if get_vector_type(vec) != VectorType.BOOL:
            raise TypeError(
                f"IS TRUE/IS FALSE requires a boolean expression; got {vec.__class__.__name__!r}"
            )
        _truth_bv = <BoolVector>vec
        _t_rows = morsel.num_rows
        _t_nbytes = (_t_rows + 7) >> 3
        if op == "IsTrue":
            return _bv_truth_test_native(_truth_bv, _BV_IS_TRUE, _t_nbytes, <uint32_t>_t_rows)
        if op == "IsNotFalse":
            return _bv_truth_test_native(_truth_bv, _BV_IS_NOT_FALSE, _t_nbytes, <uint32_t>_t_rows)
        if op == "IsFalse":
            return _bv_truth_test_native(_truth_bv, _BV_IS_FALSE, _t_nbytes, <uint32_t>_t_rows)
        return _bv_truth_test_native(_truth_bv, _BV_IS_NOT_TRUE, _t_nbytes, <uint32_t>_t_rows)
    raise NotImplementedError(f"evaluate_draken: unsupported unary op {op!r}")


cdef _unary_op_kernel(int op_code, vec):
    """Apply a unary op to a pre-evaluated vector (bytecode executor path).

    op_code is a BCUnaryOpCode integer — no Python string comparison.
    """
    cdef BoolVector is_null_bv
    cdef DrakenVector* is_null_dv
    cdef uint32_t nn_rows
    cdef Py_ssize_t nn_nbytes
    cdef BoolVector _tt_bv
    cdef DrakenVector* _tt_dv
    cdef uint32_t _tt_rows
    cdef Py_ssize_t _tt_nbytes
    if op_code == UOP_IS_NULL:
        return _is_null_as_boolvector(vec)
    if op_code == UOP_IS_NOT_NULL:
        is_null_bv = <BoolVector>_is_null_as_boolvector(vec)
        is_null_dv = is_null_bv.unified()
        nn_rows = is_null_dv.length
        nn_nbytes = (<Py_ssize_t>nn_rows + 7) >> 3
        return _bv_not_native(is_null_bv, nn_nbytes, nn_rows)
    if op_code == UOP_IS_EMPTY:
        return _BoolVector(_vector_string_is_empty(_nb_vec_unwrap(vec)))
    if op_code == UOP_IS_NOT_EMPTY:
        return _BoolVector(_vector_string_is_not_empty(_nb_vec_unwrap(vec)))
    if op_code == UOP_BITWISE_NOT:
        return Vector(_vector_bitwise_not(_nb_vec_unwrap(vec)))
    if op_code == UOP_IS_TRUE or op_code == UOP_IS_NOT_FALSE or op_code == UOP_IS_FALSE or op_code == UOP_IS_NOT_TRUE:
        if get_vector_type(vec) != VectorType.BOOL:
            raise TypeError(
                f"IS TRUE/IS FALSE requires a boolean expression; got {vec.__class__.__name__!r}"
            )
        _tt_bv = <BoolVector>vec
        _tt_dv = _tt_bv.unified()
        _tt_rows = _tt_dv.length
        _tt_nbytes = (<Py_ssize_t>_tt_rows + 7) >> 3
        if op_code == UOP_IS_TRUE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_TRUE, _tt_nbytes, _tt_rows)
        if op_code == UOP_IS_NOT_FALSE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_NOT_FALSE, _tt_nbytes, _tt_rows)
        if op_code == UOP_IS_FALSE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_FALSE, _tt_nbytes, _tt_rows)
        return _bv_truth_test_native(_tt_bv, _BV_IS_NOT_TRUE, _tt_nbytes, _tt_rows)
    raise NotImplementedError(f"_unary_op_kernel: unsupported unary op code {op_code!r}")


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


# evaluate_case — lazy import cached on first BC_CASE instruction.
# Avoids a circular import: case_eval.pyx imports evaluate_draken from this
# module; importing it at module level would create a load-time cycle.
_evaluate_case_fn = None


cdef _get_evaluate_case():
    global _evaluate_case_fn
    if _evaluate_case_fn is None:
        from opteryx.expression.evaluator.case_eval import evaluate_case
        _evaluate_case_fn = evaluate_case
    return _evaluate_case_fn


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
    return _vector_uint64_eq_scalar(row_hashes_view, len(row_hashes_view), target_hash)


# ---------------------------------------------------------------------------
# Native bitmap helpers — replace Python BoolVector method dispatch
# ---------------------------------------------------------------------------

cdef inline const uint8_t* _bv_bitmap_ptr(
    BoolVector bv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    uint8_t** scratch_out,
) except NULL:
    """Return a dense uint8_t* bitmap for `bv`.

    Dense vectors (data_length == length): returns dv.data directly; *scratch_out = NULL.
    Constant-shape (data_length == 1): expands into a malloc'd buffer; *scratch_out = that buffer.

    Caller must free(*scratch_out) if it is non-NULL.
    Raises NotImplementedError for unexpected encoding shapes (§1: no silent fallback).
    """
    cdef DrakenVector* dv = bv.unified()
    cdef uint8_t fill
    cdef uint8_t* out
    scratch_out[0] = NULL
    if dv.data_length == dv.length:
        return <const uint8_t*>dv.data
    if dv.data_length == 1:
        out = <uint8_t*>malloc(<size_t>nbytes)
        if out == NULL:
            raise MemoryError("_bv_bitmap_ptr: malloc failed")
        fill = 0xFF if ((<const uint8_t*>dv.data)[0] & 1u) else 0x00
        memset(out, fill, <size_t>nbytes)
        if num_rows & 7u:
            out[nbytes - 1] = fill & <uint8_t>((1u << (num_rows & 7u)) - 1u)
        scratch_out[0] = out
        return out
    raise NotImplementedError(
        f"_bv_bitmap_ptr: unexpected BoolVector encoding "
        f"data_length={dv.data_length} length={dv.length} (CLAUDE.md §1: no silent fallback)"
    )


cdef BoolVector _bv_op2_native(
    BoolVector lbv,
    BoolVector rbv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    int op,
):
    """Apply a binary boolean bitmap operation with no Python method dispatch.

    op: 0 = AND, 1 = OR, 2 = XOR
    Returns a new dense BoolVector owning its own draken_malloc'd bitmap.
    """
    cdef const uint8_t* l_data
    cdef const uint8_t* r_data
    cdef uint8_t* l_scratch = NULL
    cdef uint8_t* r_scratch = NULL
    cdef uint8_t* out_data
    cdef uint8_t* out_null
    cdef DrakenVector* lv = lbv.unified()
    cdef DrakenVector* rv = rbv.unified()
    cdef bint had_null
    cdef object result_obj

    l_data = _bv_bitmap_ptr(lbv, nbytes, num_rows, &l_scratch)
    r_data = _bv_bitmap_ptr(rbv, nbytes, num_rows, &r_scratch)

    out_data = <uint8_t*>malloc(<size_t>nbytes)
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_data == NULL or out_null == NULL:
        if l_scratch != NULL: free(l_scratch)
        if r_scratch != NULL: free(r_scratch)
        free(out_data)
        free(out_null)
        raise MemoryError("_bv_op2_native: malloc failed")

    if op == 0:
        had_null = c_and_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)
    elif op == 1:
        had_null = c_or_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)
    else:
        had_null = c_xor_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)

    try:
        result_obj = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if l_scratch != NULL: free(l_scratch)
        if r_scratch != NULL: free(r_scratch)

    # bool_vector_from_bits returns a nanobind Vector (not a cdef BoolVector);
    # wrap in _BoolVector so callers get a proper typed BoolVector instance.
    return _BoolVector(result_obj)


cdef BoolVector _bv_not_native(
    BoolVector bv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
):
    """Apply NOT to a BoolVector with no Python method dispatch."""
    cdef const uint8_t* src_data
    cdef uint8_t* src_scratch = NULL
    cdef uint8_t* out_data
    cdef uint8_t* out_null
    cdef DrakenVector* dv
    cdef bint had_null
    cdef object result_obj

    dv = bv.unified()
    src_data = _bv_bitmap_ptr(bv, nbytes, num_rows, &src_scratch)

    out_data = <uint8_t*>malloc(<size_t>nbytes)
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_data == NULL or out_null == NULL:
        if src_scratch != NULL: free(src_scratch)
        free(out_data)
        free(out_null)
        raise MemoryError("_bv_not_native: malloc failed")

    had_null = c_not_bitmap(out_data, out_null, src_data, dv.validity, <size_t>nbytes, num_rows)

    try:
        result_obj = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if src_scratch != NULL: free(src_scratch)

    # bool_vector_from_bits returns a nanobind Vector (not a cdef BoolVector);
    # wrap in _BoolVector so callers get a proper typed BoolVector instance.
    return _BoolVector(result_obj)


cdef inline bint _bv_any_native(BoolVector bv, Py_ssize_t nbytes) except -1:
    """Return True if the BoolVector has at least one True bit (ignoring nulls)."""
    cdef DrakenVector* dv = bv.unified()
    if dv.data_length == 1:
        return bool((<const uint8_t*>dv.data)[0] & 1u)
    return simd_popcount(<uint8_t*>dv.data, <size_t>nbytes) > 0


cdef inline bint _bv_all_native(
    BoolVector bv, Py_ssize_t nbytes, uint32_t num_rows,
) except -1:
    """Return True if all bits are True and there are no nulls."""
    cdef DrakenVector* dv = bv.unified()
    if dv.validity != NULL:
        return False  # has nulls — not all-true
    if dv.data_length == 1:
        return bool((<const uint8_t*>dv.data)[0] & 1u)
    return <uint32_t>simd_popcount(<uint8_t*>dv.data, <size_t>nbytes) == num_rows


cdef BoolVector _bv_truth_test_native(
    BoolVector bv, int op, Py_ssize_t nbytes, uint32_t num_rows,
):
    """Apply IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE with no Python dispatch.

    SQL three-value logic (validity bitmap: 1=valid, 0=null):
      IS TRUE      : data & validity
      IS FALSE     : ~data & validity
      IS NOT TRUE  : ~data | ~validity
      IS NOT FALSE : data | ~validity
    Result is always null-free (IS TRUE/FALSE always yield a definite boolean).
    """
    cdef DrakenVector* dv = bv.unified()
    cdef const uint8_t* data
    cdef uint8_t* scratch = NULL
    cdef const uint8_t* validity = dv.validity
    cdef uint8_t* out_data = <uint8_t*>malloc(<size_t>nbytes)
    cdef object result_obj
    cdef Py_ssize_t k
    cdef uint8_t tail_mask

    if out_data == NULL:
        raise MemoryError("_bv_truth_test_native: malloc failed")

    data = _bv_bitmap_ptr(bv, nbytes, num_rows, &scratch)

    try:
        if validity == NULL:
            # No nulls: IS TRUE == IS NOT FALSE == data;
            #            IS FALSE == IS NOT TRUE == ~data
            if op == _BV_IS_TRUE or op == _BV_IS_NOT_FALSE:
                for k in range(nbytes):
                    out_data[k] = data[k]
            else:
                for k in range(nbytes):
                    out_data[k] = ~data[k]
        else:
            if op == _BV_IS_TRUE:
                for k in range(nbytes):
                    out_data[k] = data[k] & validity[k]
            elif op == _BV_IS_FALSE:
                for k in range(nbytes):
                    out_data[k] = (~data[k]) & validity[k]
            elif op == _BV_IS_NOT_TRUE:
                for k in range(nbytes):
                    out_data[k] = (~data[k]) | (~validity[k])
            else:  # _BV_IS_NOT_FALSE
                for k in range(nbytes):
                    out_data[k] = data[k] | (~validity[k])

        # Mask tail bits beyond num_rows
        if num_rows & 7u:
            tail_mask = <uint8_t>((1u << (num_rows & 7u)) - 1u)
            out_data[nbytes - 1] &= tail_mask

        # Result has no nulls — IS TRUE/FALSE always yields a definite answer
        result_obj = bool_vector_from_bits(out_data, NULL, num_rows)
    finally:
        free(out_data)
        if scratch != NULL:
            free(scratch)

    return _BoolVector(result_obj)


cpdef evaluate_draken(node, morsel):
    cdef int node_type = <int>node.node_type
    cdef Py_ssize_t num_rows = morsel.num_rows
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef BoolVector lbv, rbv, result_bv
    cdef int j, arity

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
        lbv = <BoolVector>evaluate_draken(node.left, morsel)
        rbv = <BoolVector>evaluate_draken(node.right, morsel)
        return _bv_op2_native(lbv, rbv, nbytes, <uint32_t>num_rows, 0)

    if node_type == NT_OR:
        lbv = <BoolVector>evaluate_draken(node.left, morsel)
        rbv = <BoolVector>evaluate_draken(node.right, morsel)
        return _bv_op2_native(lbv, rbv, nbytes, <uint32_t>num_rows, 1)

    if node_type == NT_NOT:
        lbv = <BoolVector>evaluate_draken(node.centre, morsel)
        return _bv_not_native(lbv, nbytes, <uint32_t>num_rows)

    if node_type == NT_XOR:
        lbv = <BoolVector>evaluate_draken(node.left, morsel)
        rbv = <BoolVector>evaluate_draken(node.right, morsel)
        return _bv_op2_native(lbv, rbv, nbytes, <uint32_t>num_rows, 2)

    if node_type == NT_BETWEEN:
        col = _eval_value(node.left, morsel)
        lower_val = node.right.value
        upper_val = node.centre.value
        lower_inclusive, upper_inclusive = node.value
        return draken_between(col, lower_val, upper_val, lower_inclusive, upper_inclusive)

    if node_type == NT_DNF:
        arity = len(node.parameters)
        result_bv = <BoolVector>evaluate_draken(node.parameters[0], morsel)
        for j in range(1, arity):
            if not _bv_any_native(result_bv, nbytes):
                return result_bv
            rbv = <BoolVector>evaluate_draken(node.parameters[j], morsel)
            result_bv = _bv_op2_native(result_bv, rbv, nbytes, <uint32_t>num_rows, 0)
        return result_bv

    if node_type == NT_CNF:
        arity = len(node.parameters)
        result_bv = <BoolVector>evaluate_draken(node.parameters[0], morsel)
        for j in range(1, arity):
            if _bv_all_native(result_bv, nbytes, <uint32_t>num_rows):
                return result_bv
            rbv = <BoolVector>evaluate_draken(node.parameters[j], morsel)
            result_bv = _bv_op2_native(result_bv, rbv, nbytes, <uint32_t>num_rows, 1)
        return result_bv

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
            result = _draken_native.vector_from_sequence(result)
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


cpdef evaluate_and_append_draken(nodes, morsel):
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
            existing = set()
            for _n in morsel.column_names:
                if isinstance(_n, bytes):
                    existing.add(_n.decode())
                else:
                    existing.add(_n)
        if identity in existing:
            continue

        if col_names is None:
            col_names = list(morsel.column_names)
            col_vecs = []
            for _n in col_names:
                if isinstance(_n, bytes):
                    col_vecs.append(morsel.column(_n))
                else:
                    col_vecs.append(morsel.column(_n.encode()))

        node_type = <int>node.node_type

        if node_type == NT_LITERAL:
            literal_vec = typed_constant_vector(node.value, morsel.num_rows, node.schema_column)
            if literal_vec is None:
                # Schema type not covered by the typed constant path; fall back to
                # the generic from_constant that drives shape from the Python value.
                literal_vec = _draken_native.vector_from_constant(node.value, morsel.num_rows)
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
                    # Wrap nanobind Vector in Cython shim so all callables
                    # (typed Cython cpdef) receive consistent Vector objects.
                    if getattr(value, "_nb", None) is None:
                        value = Vector(value)
                    parameters.append(value)
                    continue
                if isinstance(value, list):
                    _all_vec = True
                    for _v in value:
                        if not is_draken_vector(_v):
                            _all_vec = False
                            break
                    if _all_vec:
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


cpdef Morsel execute_and_append(list compiled_evals, Morsel morsel):
    """Execute pre-compiled (identity, CompiledBytecode) pairs and append results.

    Replaces evaluate_and_append_draken at execution time.  Filtering
    (_PASSTHRU, should_evaluate) and ordering (prioritize_evaluation) must
    have been applied at bind time by compile_eval_nodes().

    The identity-already-present check is still performed at runtime because
    upstream operators may have materialised the column before this call.
    """
    cdef set existing = None
    cdef list col_names = None
    cdef list col_vecs = None
    cdef bint appended = False

    if not compiled_evals:
        return morsel

    for entry in compiled_evals:
        identity = entry[0]

        if existing is None:
            existing = set()
            for _n in morsel.column_names:
                if isinstance(_n, bytes):
                    existing.add(_n.decode())
                else:
                    existing.add(_n)

        if identity in existing:
            continue

        if col_names is None:
            col_names = list(morsel.column_names)
            col_vecs = []
            for _n in col_names:
                if isinstance(_n, bytes):
                    col_vecs.append(morsel.column(_n))
                else:
                    col_vecs.append(morsel.column(_n.encode()))

        result = execute_bytecode(entry[1], morsel)
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
    BC_CASE,
    BC_CAST,
    BC_CMP_LEFT_TEMPORAL,
    BC_CMP_RIGHT_TEMPORAL,
    BC_CMP_INLIST_INLINE,
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
    # Type codes
    BC_TYPE_NONE, BC_TYPE_DATE, BC_TYPE_TIMESTAMP,
    # Binary op codes
    BOP_UNKNOWN, BOP_PLUS, BOP_MINUS, BOP_MULTIPLY, BOP_DIVIDE,
    BOP_MODULO, BOP_INT_DIVIDE, BOP_STRING_CONCAT,
    BOP_BITWISE_OR, BOP_BITWISE_AND, BOP_BITWISE_XOR,
    BOP_SHIFT_LEFT, BOP_SHIFT_RIGHT,
    # Unary op codes
    UOP_UNKNOWN, UOP_IS_NULL, UOP_IS_NOT_NULL, UOP_IS_EMPTY,
    UOP_IS_NOT_EMPTY, UOP_BITWISE_NOT,
    UOP_IS_TRUE, UOP_IS_NOT_FALSE, UOP_IS_FALSE, UOP_IS_NOT_TRUE,
)
from libc.stdint cimport uint8_t, int8_t, int16_t, uintptr_t, uint32_t

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_BOOL, draken_vector_from_dense
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

from draken.morsels.morsel cimport Morsel
from draken.vectors.bool_vector cimport (
    BoolVector,
    from_decoded,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    c_get_bitmap_ptrs,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport Vector, simd_popcount, from_decoded as vec_from_decoded
from draken.core.frame_arena cimport (
    DrakenFrameArena,
    draken_frame_arena_create,
    draken_frame_arena_destroy,
    draken_frame_arena_alloc,
    draken_frame_arena_release,
)
from draken.ops.compare_dv cimport draken_compare_dv
from draken.ops.arithmetic_dv cimport draken_arithmetic_dv


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
    #
    # Slot 0 is the result slot: allocated with draken_malloc so ownership can
    # be transferred to draken_vector_own_raw (via from_decoded) in the postpass.
    # All other slots are scratch and stay on libc malloc.
    for j in range(n_slots + 2):
        if j == 0:
            p = <uint8_t*>draken_malloc(nbytes)
        else:
            p = <uint8_t*>malloc(nbytes)
        if p == NULL:
            raise MemoryError("evaluate_bitmap: failed to allocate bitmap buffer")
        memset(p, 0, nbytes)
        bitmaps[j] = p

        if j == 0:
            p = <uint8_t*>draken_malloc(nbytes)
        else:
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
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
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
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
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
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
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
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
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
                    null_bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
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
                    null_bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
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
    """Wrap a draken_malloc'd result bitmap into a BoolVector.

    Ownership of result_bitmap and (if has_null) result_null is transferred
    to the returned BoolVector via from_decoded → draken_vector_own_raw.
    The caller must null out those pointers after this call so the finally
    block does not double-free them.
    """
    return from_decoded(
        <void*>result_bitmap,
        result_null if has_null else NULL,
        <size_t>num_rows,
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

        result = _execute_bytecode_postpass(
            bitmaps[0],
            null_bitmaps[0],
            slot_has_null[0] != 0,
            num_rows,
        )
        # Postpass transferred ownership of slot-0 buffers to the BoolVector.
        # Null them out so the finally block does not double-free.
        bitmaps[0] = NULL
        if slot_has_null[0]:
            null_bitmaps[0] = NULL
        return result
    finally:
        # Slot 0 was draken_malloc'd; use draken_free (NULL-safe if transferred).
        # Slots 1..n_slots+1 are libc malloc'd.
        if bitmaps[0] != NULL:
            draken_free(bitmaps[0])
        if null_bitmaps[0] != NULL:
            draken_free(null_bitmaps[0])
        for j in range(1, n_slots + 2):
            free(bitmaps[j])
            free(null_bitmaps[j])
        free(col_cache)
        free(bitmaps)
        free(null_bitmaps)
        free(slot_has_null)


cdef inline uint8_t* _ensure_dense_bitmap(
    DrakenVector* dv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    DrakenFrameArena* arena,
) except NULL:
    """Return a dense bitmap pointer for a DRAKEN_BOOL DV*.

    Dense (data_length == length): returns dv->data directly — no copy.
    Constant-shape (data_length == 1): expands to a dense arena allocation.
    Other shapes: raises NotImplementedError — fail fast (CLAUDE.md §1).
    """
    cdef uint8_t fill
    cdef uint8_t* out
    if dv.data_length == dv.length:
        return <uint8_t*>dv.data
    if dv.data_length == 1:
        fill = 0xFF if ((<uint8_t*>dv.data)[0] & 1u) else 0x00
        out = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
        if out == NULL:
            raise MemoryError("_ensure_dense_bitmap: arena alloc failed")
        memset(out, fill, <size_t>nbytes)
        if num_rows & 7:
            out[nbytes - 1] = fill & <uint8_t>((1u << (num_rows & 7u)) - 1u)
        return out
    raise NotImplementedError(
        f"boolean combinator: DRAKEN_BOOL vector with "
        f"data_length={dv.data_length} != length={dv.length} and != 1 "
        f"is not a supported encoding (CLAUDE.md §1 — no silent fallback)."
    )


cdef object _slot_to_pyobj(DrakenVector* dv, object anc, DrakenFrameArena* arena):
    """Recover a Python Vector from a DV* stack slot.

    Hot path (borrowed slot): anc is the Python Vector whose .unified() the DV*
    was taken from — return it directly, zero allocation.

    Cold path (arena slot): anc is None — the DV* is arena-owned.  Release the
    data/validity buffers from the arena (transferring ownership to the Python
    object we're about to create), then wrap via from_decoded / vec_from_decoded.
    Called only from Python-fallback paths (LIKE/RLIKE, string concat, etc.);
    never on the ordinal-compare hot path.
    """
    if anc is not None:
        return anc
    cdef void*    dp = dv.data
    cdef uint8_t* vp = dv.validity
    draken_frame_arena_release(arena, dp)
    if vp != NULL:
        draken_frame_arena_release(arena, vp)
    if dv.type == DRAKEN_BOOL:
        return from_decoded(dp, vp, <size_t>dv.length)
    return vec_from_decoded(dp, vp, dv.length, dv.type)


cpdef execute_bytecode(CompiledBytecode bc, Morsel morsel):
    """Execute a typed bytecode against `morsel`. Returns a Vector.

    If bc.is_pure_bitmap, delegates to evaluate_bitmap (nogil bitmap path).
    Otherwise uses a C-array DV* operand stack backed by a parallel Python
    anchor list. CLAUDE.md §2/§3.

    Phase 5 — DV* stack: every stack slot is a (DrakenVector*, Python anchor) pair.
    - dv_stack[sp]: raw DrakenVector* — borrowed (from Python Vector.unified()) or
      arena-allocated (from draken_compare_dv / draken_arithmetic_dv / combinator).
      NULL for non-vector slots (sets, CarcharSet, etc.).
    - anchor[sp]: Python object keeping the vector alive (None for arena results).

    Boolean combinators (BC_AND/OR/XOR/NOT) call the C-level bitmap kernels
    (c_and_bitmap etc.) directly on dv->data, avoiding intermediate Python
    BoolVector object creation. BC_COMPARE and BC_BINARY_OP fast paths push
    DV* from draken_compare_dv/draken_arithmetic_dv without from_decoded.
    BC_DNF/CNF use a native ping-pong bitmap loop (no Python objects).

    Promoted to cpdef so callers within the _operators compilation unit dispatch
    at C level — no Python function call boundary.
    """
    if bc.is_pure_bitmap:
        return evaluate_bitmap(bc, morsel)

    cdef Py_ssize_t n_instrs = bc.count
    cdef Py_ssize_t cap = bc.max_stack_depth
    if cap < 1:
        cap = 1
    if cap > 64:
        raise ValueError(
            f"execute_bytecode: expression stack depth {cap} exceeds maximum 64"
        )

    # DV* operand stack — C array of pointers.
    # dv_store: inline DrakenVector struct storage for combinator results
    # (bitmap data/validity are arena-allocated; the struct lives here).
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef list anchor = [None] * cap
    cdef Py_ssize_t ki
    for ki in range(64):
        dv_stack[ki] = NULL

    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i, j, base
    cdef int opcode
    cdef int arity
    cdef int flags
    cdef BytecodeInstr* slot
    cdef BoolVector b_result
    cdef Vector v_result
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (<Py_ssize_t>num_rows + 7) >> 3
    cdef object scalar_obj
    cdef object compare_result
    cdef object legacy_result
    cdef object py_left
    cdef object py_right
    cdef int16_t left_type_code
    cdef int16_t right_type_code
    cdef object func_args
    cdef Py_ssize_t func_base
    cdef object callable_obj
    cdef bint is_nb_callable
    cdef object inlist_right
    # DV fast-path variables
    cdef DrakenFrameArena* arena = NULL
    cdef DrakenVector* dv_left_ptr
    cdef DrakenVector* dv_right_ptr
    cdef DrakenVector* dv_result_ptr
    cdef void* result_data_ptr
    cdef uint8_t* result_val_ptr
    cdef uint8_t* left_data
    cdef uint8_t* left_null
    cdef uint8_t* right_data
    cdef uint8_t* right_null
    cdef uint32_t result_len_u32
    cdef DrakenType result_dtype
    cdef int dv_op
    cdef int had_null
    cdef uint8_t* cur_data
    cdef uint8_t* cur_null
    cdef uint8_t* next_data
    cdef uint8_t* next_null

    arena = draken_frame_arena_create()
    if arena == NULL:
        raise MemoryError("execute_bytecode: failed to create DrakenFrameArena")

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
                anchor[sp] = v_result
                dv_stack[sp] = (<Vector>v_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_BOOL — dense bitmap materialized in arena.
            # Avoids constant-shape BoolVector; c_and_bitmap requires dense.
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_BOOL:
                result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                if result_data_ptr == NULL:
                    raise MemoryError("execute_bytecode: BC_LOAD_LIT_BOOL alloc failed")
                if slot.bool_value != 0:
                    memset(<uint8_t*>result_data_ptr, 0xFF, <size_t>nbytes)
                    if num_rows & 7:
                        (<uint8_t*>result_data_ptr)[nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
                else:
                    memset(<uint8_t*>result_data_ptr, 0x00, <size_t>nbytes)
                dv_store[sp] = draken_vector_from_dense(
                    result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL, NULL
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_SET — non-DV slot (set/CarcharSet objects)
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_SET:
                anchor[sp] = <object>slot.literal_obj
                dv_stack[sp] = NULL
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LOAD_LIT_SCALAR — typed constant Vector
            # ----------------------------------------------------------
            if opcode == BC_LOAD_LIT_SCALAR:
                scalar_obj = <object>slot.literal_obj
                if isinstance(scalar_obj, bool):
                    # Bool scalar: dense bitmap in arena (constant-shape safe)
                    result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                    if result_data_ptr == NULL:
                        raise MemoryError("execute_bytecode: BC_LOAD_LIT_SCALAR bool alloc failed")
                    if scalar_obj:
                        memset(<uint8_t*>result_data_ptr, 0xFF, <size_t>nbytes)
                        if num_rows & 7:
                            (<uint8_t*>result_data_ptr)[nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
                    else:
                        memset(<uint8_t*>result_data_ptr, 0x00, <size_t>nbytes)
                    dv_store[sp] = draken_vector_from_dense(
                        result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL, NULL
                    )
                    dv_stack[sp] = &dv_store[sp]
                    anchor[sp] = None
                    sp += 1
                    continue
                if isinstance(scalar_obj, (_CarcharSetWrapper, _PerfectHashSet)):
                    anchor[sp] = scalar_obj
                    dv_stack[sp] = NULL
                    sp += 1
                    continue
                # Lists/sets/tuples are IN-list literals consumed immediately by BC_COMPARE.
                if isinstance(scalar_obj, (list, tuple, set, frozenset)):
                    anchor[sp] = scalar_obj
                    dv_stack[sp] = NULL
                    sp += 1
                    continue
                v_result = _scalar_to_draken_constant(scalar_obj, num_rows)
                anchor[sp] = v_result
                dv_stack[sp] = (<Vector>v_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # Boolean combinators — C-level bitmap kernels.
            #
            # _ensure_dense_bitmap handles dense (no-copy) and constant-shape
            # (expand in arena) inputs.  Non-dense non-constant shapes raise —
            # fail fast per CLAUDE.md §1.  No Python fallback.
            # ----------------------------------------------------------
            if opcode == BC_AND:
                sp -= 1
                dv_right_ptr = dv_stack[sp]
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                if dv_left_ptr == NULL or dv_right_ptr == NULL:
                    raise TypeError("BC_AND: operand is not a boolean DV* (NULL slot)")
                left_data  = _ensure_dense_bitmap(dv_left_ptr,  nbytes, <uint32_t>num_rows, arena)
                right_data = _ensure_dense_bitmap(dv_right_ptr, nbytes, <uint32_t>num_rows, arena)
                left_null  = dv_left_ptr.validity
                right_null = dv_right_ptr.validity
                result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if result_data_ptr == NULL or result_val_ptr == NULL:
                    raise MemoryError("execute_bytecode: BC_AND alloc failed")
                had_null = c_and_bitmap(
                    <uint8_t*>result_data_ptr, result_val_ptr,
                    left_data, left_null,
                    right_data, right_null,
                    <size_t>nbytes, <uint32_t>num_rows,
                )
                dv_store[sp] = draken_vector_from_dense(
                    result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL,
                    result_val_ptr if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            if opcode == BC_OR:
                sp -= 1
                dv_right_ptr = dv_stack[sp]
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                if dv_left_ptr == NULL or dv_right_ptr == NULL:
                    raise TypeError("BC_OR: operand is not a boolean DV* (NULL slot)")
                left_data  = _ensure_dense_bitmap(dv_left_ptr,  nbytes, <uint32_t>num_rows, arena)
                right_data = _ensure_dense_bitmap(dv_right_ptr, nbytes, <uint32_t>num_rows, arena)
                left_null  = dv_left_ptr.validity
                right_null = dv_right_ptr.validity
                result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if result_data_ptr == NULL or result_val_ptr == NULL:
                    raise MemoryError("execute_bytecode: BC_OR alloc failed")
                had_null = c_or_bitmap(
                    <uint8_t*>result_data_ptr, result_val_ptr,
                    left_data, left_null,
                    right_data, right_null,
                    <size_t>nbytes, <uint32_t>num_rows,
                )
                dv_store[sp] = draken_vector_from_dense(
                    result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL,
                    result_val_ptr if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            if opcode == BC_XOR:
                sp -= 1
                dv_right_ptr = dv_stack[sp]
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                if dv_left_ptr == NULL or dv_right_ptr == NULL:
                    raise TypeError("BC_XOR: operand is not a boolean DV* (NULL slot)")
                left_data  = _ensure_dense_bitmap(dv_left_ptr,  nbytes, <uint32_t>num_rows, arena)
                right_data = _ensure_dense_bitmap(dv_right_ptr, nbytes, <uint32_t>num_rows, arena)
                left_null  = dv_left_ptr.validity
                right_null = dv_right_ptr.validity
                result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if result_data_ptr == NULL or result_val_ptr == NULL:
                    raise MemoryError("execute_bytecode: BC_XOR alloc failed")
                had_null = c_xor_bitmap(
                    <uint8_t*>result_data_ptr, result_val_ptr,
                    left_data, left_null,
                    right_data, right_null,
                    <size_t>nbytes, <uint32_t>num_rows,
                )
                dv_store[sp] = draken_vector_from_dense(
                    result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL,
                    result_val_ptr if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            if opcode == BC_NOT:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                if dv_left_ptr == NULL:
                    raise TypeError("BC_NOT: operand is not a boolean DV* (NULL slot)")
                left_data = _ensure_dense_bitmap(dv_left_ptr, nbytes, <uint32_t>num_rows, arena)
                left_null = dv_left_ptr.validity
                result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
                result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if result_data_ptr == NULL or result_val_ptr == NULL:
                    raise MemoryError("execute_bytecode: BC_NOT alloc failed")
                had_null = c_not_bitmap(
                    <uint8_t*>result_data_ptr, result_val_ptr,
                    left_data, left_null,
                    <size_t>nbytes, <uint32_t>num_rows,
                )
                dv_store[sp] = draken_vector_from_dense(
                    result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL,
                    result_val_ptr if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            # ----------------------------------------------------------
            # Variadic AND/OR — DNF (AND-of-terms) / CNF (OR-of-terms).
            #
            # Native bitmap loop: no Python objects.  Ping-pong between
            # two arena buffer pairs — cur_{data,null} accumulates the
            # result; next_{data,null} is the per-step output.
            # After the loop the final pair is stored in dv_store[base].
            # ----------------------------------------------------------
            if opcode == BC_DNF:
                arity = slot.arity
                base = sp - arity
                sp = base
                dv_left_ptr = dv_stack[base]
                if dv_left_ptr == NULL:
                    raise TypeError("BC_DNF: first operand is NULL")
                # Accumulator: copy first operand's bitmap.
                cur_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                cur_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if cur_data == NULL or cur_null == NULL:
                    raise MemoryError("BC_DNF: alloc failed")
                memcpy(cur_data, _ensure_dense_bitmap(dv_left_ptr, nbytes, <uint32_t>num_rows, arena), <size_t>nbytes)
                if dv_left_ptr.validity != NULL:
                    memcpy(cur_null, dv_left_ptr.validity, <size_t>nbytes)
                else:
                    memset(cur_null, 0, <size_t>nbytes)
                for j in range(1, arity):
                    dv_right_ptr = dv_stack[base + j]
                    if dv_right_ptr == NULL:
                        raise TypeError(f"BC_DNF: operand {j} is NULL")
                    right_data = _ensure_dense_bitmap(dv_right_ptr, nbytes, <uint32_t>num_rows, arena)
                    right_null = dv_right_ptr.validity
                    next_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                    next_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                    if next_data == NULL or next_null == NULL:
                        raise MemoryError("BC_DNF: alloc failed")
                    had_null = c_and_bitmap(
                        next_data, next_null,
                        cur_data, cur_null,
                        right_data, right_null,
                        <size_t>nbytes, <uint32_t>num_rows,
                    )
                    cur_data = next_data
                    cur_null = next_null
                dv_store[sp] = draken_vector_from_dense(
                    cur_data, <uint32_t>num_rows, DRAKEN_BOOL,
                    cur_null if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            if opcode == BC_CNF:
                arity = slot.arity
                base = sp - arity
                sp = base
                dv_left_ptr = dv_stack[base]
                if dv_left_ptr == NULL:
                    raise TypeError("BC_CNF: first operand is NULL")
                cur_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                cur_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                if cur_data == NULL or cur_null == NULL:
                    raise MemoryError("BC_CNF: alloc failed")
                memcpy(cur_data, _ensure_dense_bitmap(dv_left_ptr, nbytes, <uint32_t>num_rows, arena), <size_t>nbytes)
                if dv_left_ptr.validity != NULL:
                    memcpy(cur_null, dv_left_ptr.validity, <size_t>nbytes)
                else:
                    memset(cur_null, 0, <size_t>nbytes)
                for j in range(1, arity):
                    dv_right_ptr = dv_stack[base + j]
                    if dv_right_ptr == NULL:
                        raise TypeError(f"BC_CNF: operand {j} is NULL")
                    right_data = _ensure_dense_bitmap(dv_right_ptr, nbytes, <uint32_t>num_rows, arena)
                    right_null = dv_right_ptr.validity
                    next_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                    next_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
                    if next_data == NULL or next_null == NULL:
                        raise MemoryError("BC_CNF: alloc failed")
                    had_null = c_or_bitmap(
                        next_data, next_null,
                        cur_data, cur_null,
                        right_data, right_null,
                        <size_t>nbytes, <uint32_t>num_rows,
                    )
                    cur_data = next_data
                    cur_null = next_null
                dv_store[sp] = draken_vector_from_dense(
                    cur_data, <uint32_t>num_rows, DRAKEN_BOOL,
                    cur_null if had_null else NULL,
                )
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = None
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_COMPARE — typed draken_compare (cpdef)
            #
            # Two shapes:
            #   Normal (flags & BC_CMP_INLIST_INLINE == 0):
            #     pop right DV*, pop left DV*, compare, push result DV*.
            #     Phase 4/5 fast path: draken_compare_dv for EQ/NE/LT/GT/LE/GE;
            #     result DV* stored in dv_stack — no from_decoded until needed.
            #   Inline IN-list (flags & BC_CMP_INLIST_INLINE != 0):
            #     right operand folded into slot.literal_obj — pop left DV* only.
            # ----------------------------------------------------------
            if opcode == BC_COMPARE:
                flags = slot.flags
                left_type_code = slot.left_type_code
                right_type_code = slot.right_type_code

                if flags & BC_CMP_INLIST_INLINE:
                    # Right is an inline set literal — pop ONE item.
                    sp -= 1
                    dv_left_ptr = dv_stack[sp]
                    py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                    inlist_right = <object>slot.literal_obj
                    if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(py_left):
                        py_left = _coerce_temporal_scalar_for_arrow(
                            py_left,
                            _OrsoTypes_DATE if left_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP,
                        )
                    if slot.op_code != OP_UNKNOWN:
                        compare_result = draken_compare_int(
                            slot.op_code, py_left, inlist_right, left_type_code, right_type_code
                        )
                    else:
                        compare_result = draken_compare(
                            <str>slot.compare_op_str, py_left, inlist_right,
                            None if left_type_code == BC_TYPE_NONE else (
                                _OrsoTypes_DATE if left_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP
                            ),
                            None,
                        )
                else:
                    # Normal case — pop TWO items.
                    sp -= 1
                    dv_right_ptr = dv_stack[sp]
                    sp -= 1
                    dv_left_ptr = dv_stack[sp]

                    # Phase 4/5: C-level fast path for ordinal EQ/NE/LT/GT/LE/GE.
                    dv_op = -1
                    if 0 < slot.op_code < 19:
                        dv_op = _DRAKEN_CMP_OP[slot.op_code]
                    if (dv_op >= 0 and dv_left_ptr != NULL and dv_right_ptr != NULL):
                        dv_result_ptr = draken_compare_dv(
                            dv_op,
                            dv_left_ptr, dv_right_ptr,
                            slot.left_type_code, slot.right_type_code,
                            <uint32_t>num_rows, arena,
                        )
                        if dv_result_ptr != NULL:
                            # Store DV* directly — no from_decoded until _slot_to_pyobj.
                            dv_stack[sp] = dv_result_ptr
                            anchor[sp] = None
                            sp += 1
                            continue

                    # Python fallback (unsupported types, LIKE/RLIKE/IN_LIST).
                    py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                    py_right = _slot_to_pyobj(dv_right_ptr, anchor[sp + 1], arena)
                    if flags != 0:
                        if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(py_left):
                            py_left = _coerce_temporal_scalar_for_arrow(
                                py_left,
                                _OrsoTypes_DATE if left_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP,
                            )
                        if (flags & BC_CMP_RIGHT_TEMPORAL) and _is_scalar_value(py_right):
                            py_right = _coerce_temporal_scalar_for_arrow(
                                py_right,
                                _OrsoTypes_DATE if right_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP,
                            )
                    if slot.op_code != OP_UNKNOWN:
                        compare_result = draken_compare_int(
                            slot.op_code, py_left, py_right, left_type_code, right_type_code
                        )
                    else:
                        compare_result = draken_compare(
                            <str>slot.compare_op_str, py_left, py_right,
                            None if left_type_code == BC_TYPE_NONE else (
                                _OrsoTypes_DATE if left_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP
                            ),
                            None if right_type_code == BC_TYPE_NONE else (
                                _OrsoTypes_DATE if right_type_code == BC_TYPE_DATE else _OrsoTypes_TIMESTAMP
                            ),
                        )
                anchor[sp] = compare_result
                dv_stack[sp] = (<Vector>compare_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_BETWEEN — typed draken_between (cpdef)
            # ----------------------------------------------------------
            if opcode == BC_BETWEEN:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                compare_result = draken_between(
                    py_left,
                    <object>slot.literal_obj if slot.literal_obj != NULL else None,
                    <object>slot.literal_obj2 if slot.literal_obj2 != NULL else None,
                    slot.op_code != 0,
                    slot.bool_value != 0,
                )
                anchor[sp] = compare_result
                dv_stack[sp] = (<Vector>compare_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_BINARY_OP — arithmetic / string / date ops on two vecs.
            #
            # Phase 4/5 fast path: draken_arithmetic_dv for PLUS..MODULO.
            # Result DV* stored in dv_stack — no vec_from_decoded until needed.
            # ----------------------------------------------------------
            if opcode == BC_BINARY_OP:
                sp -= 1
                dv_right_ptr = dv_stack[sp]
                sp -= 1
                dv_left_ptr = dv_stack[sp]

                if (BOP_PLUS <= slot.op_code <= BOP_MODULO
                        and dv_left_ptr != NULL and dv_right_ptr != NULL):
                    dv_result_ptr = draken_arithmetic_dv(
                        slot.op_code,
                        dv_left_ptr, dv_right_ptr,
                        <uint32_t>num_rows, arena,
                    )
                    if dv_result_ptr != NULL:
                        dv_stack[sp] = dv_result_ptr
                        anchor[sp] = None
                        sp += 1
                        continue

                # Non-arithmetic ops (string concat, integer divide, bitwise): route through
                # binary_operations() which dispatches to the correct C++ kernel.
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                py_right = _slot_to_pyobj(dv_right_ptr, anchor[sp + 1], arena)
                legacy_result = _binary_op_from_vecs(
                    slot.op_code,
                    py_left, py_right,
                    slot.left_type_code, slot.right_type_code,
                    <str>slot.compare_op_str,
                    num_rows,
                )
                anchor[sp] = legacy_result
                dv_stack[sp] = (<Vector>legacy_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_UNARY_OP — IS NULL / IS NOT NULL / bitwise-not / etc.
            # ----------------------------------------------------------
            if opcode == BC_UNARY_OP:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                legacy_result = _unary_op_kernel(slot.op_code, py_left)
                anchor[sp] = legacy_result
                dv_stack[sp] = (<Vector>legacy_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_FUNCTION — call pre-resolved kernel callable.
            #
            # nb_func callables receive raw nanobind Vectors (_nb unwrapped
            # via typed (<Vector>item)._nb — C-level struct access).
            # Non-nb callables receive Cython Vector shims.
            # _slot_to_pyobj materializes arena DV* slots on demand; zero
            # cost when anchor is not None (the common case).
            # ----------------------------------------------------------
            if opcode == BC_FUNCTION:
                arity = slot.arity
                callable_obj = <object>slot.callable_ref
                is_nb_callable = slot.bool_value != 0

                if arity == 0:
                    legacy_result = callable_obj(num_rows)
                else:
                    func_base = sp - arity
                    sp = func_base

                    if is_nb_callable:
                        if arity == 1:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                            )
                        elif arity == 2:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                            )
                        elif arity == 3:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 2], anchor[func_base + 2], arena))._nb,
                            )
                        else:
                            func_args = [
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + j], anchor[func_base + j], arena))._nb
                                for j in range(arity)
                            ]
                            legacy_result = callable_obj(*func_args)
                    else:
                        if arity == 1:
                            legacy_result = callable_obj(
                                _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena)
                            )
                        elif arity == 2:
                            legacy_result = callable_obj(
                                _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena),
                                _slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena),
                            )
                        elif arity == 3:
                            legacy_result = callable_obj(
                                _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena),
                                _slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena),
                                _slot_to_pyobj(dv_stack[func_base + 2], anchor[func_base + 2], arena),
                            )
                        else:
                            func_args = [
                                _slot_to_pyobj(dv_stack[func_base + j], anchor[func_base + j], arena)
                                for j in range(arity)
                            ]
                            legacy_result = callable_obj(*func_args)

                # nb_func callables return raw nanobind Vectors — wrap in Cython shim.
                if is_nb_callable and type(legacy_result).__name__ == "Vector":
                    if legacy_result.type == _draken_native.DrakenType.BOOL:
                        legacy_result = BoolVector(legacy_result)
                    else:
                        legacy_result = Vector(legacy_result)
                anchor[sp] = legacy_result
                if isinstance(legacy_result, Vector):
                    dv_stack[sp] = (<Vector>legacy_result).unified()
                else:
                    dv_stack[sp] = NULL
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_EXTRACTION — callable and key vector pre-resolved at bind time.
            # callable_ref is MapAccessOp / ArrowOp / LongArrowOp;
            # literal_obj is a pre-built constant key Vector (length=1).
            # ----------------------------------------------------------
            if opcode == BC_EXTRACTION:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                legacy_result = (<object>slot.callable_ref)(py_left, <object>slot.literal_obj)
                anchor[sp] = legacy_result
                dv_stack[sp] = (<Vector>legacy_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_CAST — pre-resolved cast closure, pop 1 push 1
            # ----------------------------------------------------------
            if opcode == BC_CAST:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                legacy_result = (<object>slot.callable_ref)(py_left)
                anchor[sp] = legacy_result
                dv_stack[sp] = (<Vector>legacy_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_CASE — CASE WHEN evaluation via evaluate_case (Phase 2a).
            # ----------------------------------------------------------
            if opcode == BC_CASE:
                legacy_result = _get_evaluate_case()(<object>slot.source_node, morsel)
                anchor[sp] = legacy_result
                dv_stack[sp] = (<Vector>legacy_result).unified()
                sp += 1
                continue

            # ----------------------------------------------------------
            # BC_LEGACY — GIL-required fallback to the tree-walker.
            # ----------------------------------------------------------
            if opcode == BC_LEGACY:
                legacy_result = _eval_value(<object>slot.source_node, morsel)
                anchor[sp] = legacy_result
                if isinstance(legacy_result, Vector):
                    dv_stack[sp] = (<Vector>legacy_result).unified()
                else:
                    dv_stack[sp] = NULL
                sp += 1
                continue

            raise NotImplementedError(
                f"execute_bytecode: unknown opcode {opcode}"
            )

        if sp != 1:
            raise ValueError(
                f"execute_bytecode: expected 1 result on stack, got {sp}"
            )

        return _slot_to_pyobj(dv_stack[0], anchor[0], arena)

    finally:
        draken_frame_arena_destroy(arena)


# Wire the trampoline into the global function pointer so C++ worker threads
# can call it without holding the GIL. Done once at module import time.
opteryx_set_worker_fn(_c_bytecode_worker_trampoline)
