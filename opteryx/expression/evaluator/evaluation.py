"""Main expression evaluation engine."""

import datetime

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar

from .arithmetic import _eval_binary_op_draken
from .comparisons import draken_between, draken_compare
from .function_execution import apply_bounded_function, is_draken_vector
from .type_coercion import (
    _coerce_date32,
    _coerce_date32_set,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_interval,
    _coerce_str,
    _coerce_str_set,
    _coerce_temporal_scalar_for_arrow,
    _coerce_timestamp,
    _coerce_timestamp_set,
    _constant_scalar_value,
    _dictionary_arrow_type,
    _dictionary_compare_vector,
    _is_constant_vector_like,
    _is_dictionary_encoded_vector,
    _is_null_as_boolvector,
    _is_typed_constant_encoded_vector,
)

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
    """Deprecated: use is_scalar() from opteryx.utils.vector_types instead.

    This function is kept for backward compatibility but new code should
    use is_scalar() which is centralized and consistent.
    """
    return is_scalar(obj)


def _eval_value(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.LITERAL:
        if isinstance(node.value, bool):
            from draken.vectors.bool_vector import BoolVector

            return BoolVector.from_constant(node.value, morsel.num_rows)

        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper

        if isinstance(node.value, CarcharSetWrapper):
            return node.value

        from draken.vectors.scalar_constructors import (
            from_scalar as _const_scalar,
        )

        vec = _const_scalar(node.value, morsel.num_rows)
        if vec is None:
            raise TypeError(
                f"_eval_value: cannot construct Draken vector for literal "
                f"{node.value!r} (type {type(node.value).__name__})"
            )
        return vec

    if node_type == NodeType.IDENTIFIER:
        vec = morsel.column(node.schema_column.identity, node.schema_column.name.encode())
        return vec

    if node_type in (NodeType.EVALUATED, NodeType.AGGREGATOR):
        try:
            vec = morsel.column(
                node.schema_column.identity, node.schema_column.name.encode()
            )
        except KeyError:
            raise ColumnReferencedBeforeEvaluationError(column=node.schema_column.name)
        return vec

    if node_type == NodeType.NESTED:
        return _eval_value(node.centre, morsel)

    if node_type == NodeType.EXPRESSION_LIST:
        return [_eval_value(parameter, morsel) for parameter in node.parameters]

    if node_type == NodeType.EXTRACTION_OPERATOR:
        left_vec = _eval_value(node.left, morsel)
        right_val = node.right.value
        op = node.value

        if op == "MapAccess":
            from draken.vectors.int64_vector import Int64Vector
            from opteryx.expression.binary_operators import MapAccessOp

            # Keep MapAccess in native vector space where possible to avoid
            # costly Arrow <-> Draken round-trips.
            key_vec = Int64Vector.from_constant(int(right_val), 1)
            result = MapAccessOp(left_vec, key_vec)
            if is_draken_vector(result):
                return result
            raise TypeError(
                f"MapAccessOp expected Draken vector result; got {type(result).__name__}."
            )

        if op in ("Arrow", "LongArrow"):
            from draken.vectors.string_vector import StringVector
            from opteryx.expression.binary_operators import ArrowOp, LongArrowOp

            key_vec = StringVector.from_constant(right_val, 1)
            return ArrowOp(left_vec, key_vec) if op == "Arrow" else LongArrowOp(left_vec, key_vec)

        raise NotImplementedError(
            f"_eval_value: EXTRACTION_OPERATOR {op!r} not supported in Draken path"
        )

    from opteryx.expression import NodeType as _NT

    if node.node_type == _NT.BINARY_OPERATOR:
        result = _eval_binary_op_draken(node, morsel)
        if result is not None:
            return result

    if node.node_type in (_NT.BINARY_OPERATOR, _NT.CAST, _NT.FUNCTION):
        identity = getattr(getattr(node, "schema_column", None), "identity", None)
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


def _unary_draken(op: str, centre_node, morsel):
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        return _is_null_as_boolvector(vec).not_vector()
    if op == "IsEmpty":
        from opteryx.compiled.vector_ops import vector_string_is_empty

        return vector_string_is_empty(vec)
    if op == "IsNotEmpty":
        from opteryx.compiled.vector_ops import vector_string_is_not_empty

        return vector_string_is_not_empty(vec)
    if op == "BitwiseNot":
        from opteryx.compiled.vector_ops import vector_bitwise_not

        return vector_bitwise_not(vec)
    if op in ("IsTrue", "IsNotFalse", "IsFalse", "IsNotTrue"):
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


def _is_temporal_type(orso_type):
    """Check if an OrsoType is DATE or TIMESTAMP."""
    from opteryx.types import OrsoTypes

    if orso_type is None:
        return False
    return orso_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)


def _validate_temporal_comparison(left_node, right_node, op):
    """
    Validate that temporal comparisons have literals explicitly cast.

    When comparing temporal and non-temporal operands, literals must be explicitly cast.
    Temporal columns do not require casting. Both operands must have temporal types.

    Rules:
    - Temporal columns (IDENTIFIER with temporal schema_column type) are implicitly valid
    - Literals and other operands must be explicitly cast to temporal types
    - Both operands must have temporal types in their schema_column

    Args:
        left_node: AST node for left operand
        right_node: AST node for right operand
        op: Comparison operator (Eq, Lt, Gt, etc.)

    Raises:
        IncompatibleTypesError: If a temporal comparison has an uncast literal operand
    """
    from opteryx.expression import NodeType

    left_type = getattr(getattr(left_node, "schema_column", None), "type", None)
    right_type = getattr(getattr(right_node, "schema_column", None), "type", None)

    left_is_temporal = _is_temporal_type(left_type)
    right_is_temporal = _is_temporal_type(right_type)

    # If neither side is temporal, no validation needed
    if not (left_is_temporal or right_is_temporal):
        return

    # If both sides are temporal, validation passes
    if left_is_temporal and right_is_temporal:
        return

    # At least one side is temporal but the other is not
    # Check if the non-temporal side is an uncast literal
    from opteryx.exceptions import IncompatibleTypesError

    non_temporal_node = right_node if left_is_temporal else left_node
    non_temporal_side = "right" if left_is_temporal else "left"

    # IDENTIFIER nodes with temporal columns are allowed without casting
    # All other non-temporal nodes (especially literals) must be cast
    if non_temporal_node.node_type != NodeType.IDENTIFIER:
        raise IncompatibleTypesError(
            message=f"Temporal comparison requires literals to be explicitly cast to temporal types.\n"
            f"The {non_temporal_side} side is missing an explicit CAST or :: operator.\n\n"
            f"Examples of valid syntax:\n"
            f"  - col {op} literal::DATE\n"
            f"  - col {op} literal::TIMESTAMP[ms]\n"
            f"  - col::TIMESTAMP[ms] {op} literal::DATE\n\n"
            f"Supported temporal types: DATE, TIMESTAMP[ms], TIMESTAMP[us], TIMESTAMP[s], TIMESTAMP[ns], TIMESTAMP[d]"
        )


_HASH_DISPATCH_MIN_ROWS = 1024

_TARGET_HASH_CACHE = {}
_TARGET_HASH_CACHE_MAX = 128


def _compute_target_hash(target_names, target_vecs):
    from draken.morsels.morsel import Morsel

    target_morsel = Morsel.from_vectors(target_names, target_vecs)
    target_hash_view = target_morsel.hash(target_names)
    return int(target_hash_view[0])


def _try_collect_numeric_eq_predicates(node):
    """Walk an AND-only subtree and collect IDENTIFIER = LITERAL predicates on
    fixed-width numeric/temporal columns.

    Returns a list of (identity_bytes, name_bytes, literal_value, orso_type)
    tuples if the entire subtree consists of such predicates (>= 2 of them) and
    every leaf is eligible. Returns None otherwise — caller takes the regular
    recursive path.
    """
    from opteryx.expression import NodeType
    from opteryx.types import OrsoTypes

    eligible_types = (OrsoTypes.INTEGER, OrsoTypes.BOOLEAN)

    preds = []
    stack = [node]
    while stack:
        n = stack.pop()
        nt = n.node_type
        if nt == NodeType.NESTED:
            stack.append(n.centre)
            continue
        if nt == NodeType.AND:
            stack.append(n.left)
            stack.append(n.right)
            continue
        if nt != NodeType.COMPARISON_OPERATOR or n.value != "Eq":
            return None

        left, right = n.left, n.right
        if (
            left.node_type == NodeType.IDENTIFIER
            and right.node_type == NodeType.LITERAL
        ):
            ident_node, lit_node = left, right
        elif (
            right.node_type == NodeType.IDENTIFIER
            and left.node_type == NodeType.LITERAL
        ):
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


def _evaluate_numeric_eq_via_hash(preds, morsel):
    """Evaluate a chain of IDENTIFIER = LITERAL predicates by hashing all
    referenced columns once and comparing against a single precomputed
    target hash.

    Collision safety: 64-bit combined hash. P(false positive per row) = 2^-64.
    For 100M rows that is ~5e-12 expected — well below noise floor of normal
    hardware. No verify pass.

    Returns a BoolVector or None if the fast-path could not be constructed
    (caller should fall back to the per-column path).
    """
    from draken.interop.vector_sequence import bool_vector_from_uint64_eq

    num_rows = morsel.num_rows
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
    return bool_vector_from_uint64_eq(row_hashes_view, target_hash)


def evaluate_draken(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.NESTED:
        return evaluate_draken(node.centre, morsel)

    if node_type == NodeType.AND:
        preds = _try_collect_numeric_eq_predicates(node)
        if preds is not None:
            fast = _evaluate_numeric_eq_via_hash(preds, morsel)
            if fast is not None:
                return fast
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.and_vector(right)

    if node_type == NodeType.OR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.or_vector(right)

    if node_type == NodeType.NOT:
        return evaluate_draken(node.centre, morsel).not_vector()

    if node_type == NodeType.XOR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.xor_vector(right)

    if node_type == NodeType.BETWEEN:
        col = _eval_value(node.left, morsel)
        lower_val = node.right.value
        upper_val = node.centre.value
        lower_inclusive, upper_inclusive = node.value
        return draken_between(col, lower_val, upper_val, lower_inclusive, upper_inclusive)

    if node_type == NodeType.DNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if not result.any():
                return result
            result = result.and_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NodeType.CNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if result.all():
                return result
            result = result.or_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NodeType.LITERAL:
        from draken.vectors.bool_vector import BoolVector

        val = node.value
        scalar = bool(val) if val is not None else False
        return BoolVector.from_constant(scalar, morsel.num_rows)

    if node_type == NodeType.COMPARISON_OPERATOR:
        # Validate that temporal comparisons have both sides explicitly typed
        _validate_temporal_comparison(node.left, node.right, node.value)

        left = _eval_value(node.left, morsel)
        right = _eval_value(node.right, morsel)
        from opteryx.types import OrsoTypes

        temporal_types = {OrsoTypes.DATE, OrsoTypes.TIMESTAMP}
        left_schema_type = getattr(getattr(node.left, "schema_column", None), "type", None)
        right_schema_type = getattr(getattr(node.right, "schema_column", None), "type", None)
        if left_schema_type in temporal_types or right_schema_type in temporal_types:
            if _is_scalar_value(left) and left_schema_type in temporal_types:
                left = _coerce_temporal_scalar_for_arrow(left, left_schema_type)
            if _is_scalar_value(right) and right_schema_type in temporal_types:
                right = _coerce_temporal_scalar_for_arrow(right, right_schema_type)

        if is_scalar(left) and is_scalar(right):
            from draken.vectors.bool_vector import BoolVector

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

    if node_type == NodeType.UNARY_OPERATOR:
        return _unary_draken(node.value, node.centre, morsel)

    if node_type == NodeType.FUNCTION:
        if node.value == "_PASSTHRU":
            return evaluate_draken(node.parameters[0], morsel)
        parameters = [_eval_value(param, morsel) for param in node.parameters]
        if len(parameters) == 0:
            parameters = [morsel.num_rows]
        result = apply_bounded_function(node, *parameters)
        if isinstance(result, list):
            from draken.interop.vector_sequence import vector_from_sequence

            result = vector_from_sequence(result)
        if get_vector_type(result) != VectorType.BOOL:
            raise TypeError(
                f"evaluate_draken: FUNCTION node returned {result.__class__.__name__!r}, expected BoolVector"
            )
        return result

    if node_type == NodeType.BINARY_OPERATOR:
        result = _eval_value(node, morsel)
        if get_vector_type(result) == VectorType.BOOL:
            return result
        raise TypeError(
            f"evaluate_draken: BINARY_OPERATOR '{node.value!r}' returned non-boolean {result.__class__.__name__!r}"
        )

    raise NotImplementedError(
        f"evaluate_draken: unsupported node type {node_type!r} (value={node.value!r})"
    )


def evaluate_and_append_draken(nodes, morsel):
    from draken.morsels.morsel import Morsel
    from opteryx.expression import NodeType

    col_names = list(morsel.column_names)
    col_vecs = [morsel.column(n if isinstance(n, bytes) else n.encode()) for n in col_names]
    existing = {n.decode() if isinstance(n, bytes) else n for n in col_names}

    for node in nodes:
        if node.value == "_PASSTHRU":
            continue
        identity = node.schema_column.identity
        if identity in existing:
            continue
        if node.node_type == NodeType.FUNCTION:
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

    return Morsel.from_vectors(col_names, col_vecs)


__all__ = ["draken_compare", "evaluate_and_append_draken", "evaluate_draken"]
