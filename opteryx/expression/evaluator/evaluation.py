"""Main expression evaluation engine."""

import datetime
import decimal

import numpy
import pyarrow as _pa

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .arithmetic import _eval_binary_op_draken
from .comparisons import draken_compare
from .function_execution import _is_draken_vector, apply_bounded_function
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
    """Check if object is a raw Python scalar (not a vector or array).

    Used to discriminate between scalars and vectors when both might have
    null_count attributes (e.g., Draken vectors vs raw Python values).

    Args:
        obj: Object to test

    Returns:
        True if obj is a raw Python scalar, False if it's a vector/array
    """
    # Explicit checks for common scalar types
    if obj is None or isinstance(obj, (bool, int, float, str, bytes, bytearray)):
        return True
    if isinstance(obj, (datetime.date, datetime.time, datetime.datetime, datetime.timedelta)):
        return True
    if isinstance(obj, decimal.Decimal):
        return True
    # Everything else (Arrow arrays, Draken vectors, etc.) is not a scalar
    return False


def _eval_value(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.LITERAL:
        # bool must stay as raw Python — bint Cython params coerce any non-None
        # object to True, so wrapping False in a BoolVector breaks bint params.
        if not isinstance(node.value, bool):
            from opteryx.compiled.draken.vectors.scalar_constructors import (
                from_scalar as _const_scalar,
            )

            vec = _const_scalar(node.value, morsel.num_rows)
            if vec is not None:
                return vec
        return node.value

    if node_type == NodeType.IDENTIFIER:
        vec = morsel.column(node.schema_column.identity.encode(), node.schema_column.name.encode())
        if vec.__class__.__name__ == "ArrowVector":
            from opteryx.compiled.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(vec.to_arrow())
        return vec

    if node_type in (NodeType.EVALUATED, NodeType.AGGREGATOR):
        try:
            vec = morsel.column(
                node.schema_column.identity.encode(), node.schema_column.name.encode()
            )
        except KeyError:
            raise ColumnReferencedBeforeEvaluationError(column=node.schema_column.name)
        if vec.__class__.__name__ == "ArrowVector":
            from opteryx.compiled.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(vec.to_arrow())
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
            from opteryx.compiled.draken.interop.arrow import (
                vector_from_arrow,
                vector_from_sequence,
            )
            from opteryx.expression.binary_operators import MapAccessOp

            source = left_vec.to_arrow() if hasattr(left_vec, "to_arrow") else left_vec
            result = MapAccessOp(source, [right_val])
            if isinstance(result, _pa.Array):
                return vector_from_arrow(result)
            return vector_from_sequence(result)

        if op in ("Arrow", "LongArrow"):
            from opteryx.compiled.draken.interop.arrow import vector_from_arrow
            from opteryx.expression.binary_operators import ArrowOp, LongArrowOp

            docs = left_vec.to_pylist()
            result = ArrowOp(docs, [right_val]) if op == "Arrow" else LongArrowOp(docs, [right_val])
            return vector_from_arrow(result)

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
                vec = morsel.column(identity if isinstance(identity, bytes) else identity.encode())
            except KeyError:
                vec = None
            if vec is not None:
                if vec.__class__.__name__ == "ArrowVector":
                    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

                    return vector_from_arrow(vec.to_arrow())
                return vec

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow, vector_from_sequence
        from opteryx.expression import _inner_evaluate

        arrow_table = morsel.to_arrow()
        result = _inner_evaluate(node, arrow_table)
        if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
            return vector_from_arrow(result)
        if result is not None and result.__class__.__name__.endswith("Vector"):
            return result
        if not hasattr(result, "__iter__") or isinstance(result, (str, bytes, numpy.generic)):
            from opteryx.compiled.draken.vectors.scalar_constructors import (
                from_scalar as _const_scalar,
            )

            vec = _const_scalar(result, morsel.num_rows)
            if vec is not None:
                return vec
            return vector_from_arrow(_pa.array([result] * morsel.num_rows))
        return vector_from_sequence(result)

    return evaluate_draken(node, morsel)


def _unary_draken(op: str, centre_node, morsel):
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        return _is_null_as_boolvector(vec).not_vector()
    if op in ("IsTrue", "IsNotFalse", "IsFalse", "IsNotTrue"):
        bv = vec if vec.__class__.__name__ == "BoolVector" else None
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


def evaluate_draken(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.NESTED:
        return evaluate_draken(node.centre, morsel)

    if node_type == NodeType.AND:
        left = evaluate_draken(node.left, morsel)
        if not left.any():
            return left
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

    if node_type == NodeType.DNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if not result.any():
                return result
            result = result.and_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NodeType.LITERAL:
        import pyarrow as pa

        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        val = node.value
        scalar = bool(val) if val is not None else False
        return BoolVector.from_arrow(pa.array([scalar] * morsel.num_rows, type=pa.bool_()))

    if node_type == NodeType.COMPARISON_OPERATOR:
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

        if _is_scalar_value(left) and _is_scalar_value(right):
            import pyarrow as pa

            from opteryx.compiled.draken.vectors.bool_vector import BoolVector
            from opteryx.expression.operations import filter_operations

            scalar_result = filter_operations(
                pa.array([left]),
                left_schema_type,
                node.value,
                pa.array([right]),
                right_schema_type,
            )[0].as_py()
            scalar_result = False if scalar_result is None else bool(scalar_result)
            return BoolVector.from_arrow(
                pa.array([scalar_result] * morsel.num_rows, type=pa.bool_())
            )
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
            result = numpy.array(result, dtype=object)
        if isinstance(result, numpy.ndarray):
            if result.ndim != 1:
                raise TypeError(
                    f"evaluate_draken: FUNCTION node returned ndarray with rank {result.ndim}, expected 1-dimensional boolean results"
                )
            if result.dtype.kind in ("b", "O", "f", "i", "u"):
                import pyarrow as pa

                from opteryx.compiled.draken.vectors.bool_vector import BoolVector

                try:
                    result = BoolVector.from_arrow(pa.array(result, type=pa.bool_()))
                except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError):
                    pass
        elif isinstance(result, _pa.Array) and _pa.types.is_boolean(result.type):
            from opteryx.compiled.draken.vectors.bool_vector import BoolVector

            result = BoolVector.from_arrow(result)
        if result.__class__.__name__ != "BoolVector":
            raise TypeError(
                f"evaluate_draken: FUNCTION node returned {result.__class__.__name__!r}, expected BoolVector"
            )
        return result

    if node_type == NodeType.BINARY_OPERATOR:
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        result = _eval_value(node, morsel)
        if result.__class__.__name__ == "BoolVector":
            return result
        if isinstance(result, _pa.Array) and _pa.types.is_boolean(result.type):
            return BoolVector.from_arrow(result)
        if isinstance(result, numpy.ndarray) and result.dtype.kind == "b":
            return BoolVector.from_arrow(_pa.array(result, type=_pa.bool_()))
        raise TypeError(
            f"evaluate_draken: BINARY_OPERATOR '{node.value!r}' returned non-boolean {result.__class__.__name__!r}"
        )

    raise NotImplementedError(
        f"evaluate_draken: unsupported node type {node_type!r} (value={node.value!r})"
    )


def evaluate_and_append_draken(nodes, morsel):
    from opteryx.compiled.draken.interop.arrow import vector_from_sequence
    from opteryx.compiled.draken.morsels.morsel import Morsel
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
            from opteryx.expression import NodeType as _NT
            from opteryx.expression import _inner_evaluate

            parameters = []
            for param in node.parameters:
                parameters.append(_eval_value(param, morsel))
            if len(parameters) == 0:
                parameters = [morsel.num_rows]
            result = apply_bounded_function(node, *parameters)
        else:
            result = _eval_value(node, morsel)
        if not _is_draken_vector(result):
            import pyarrow as _pa_local

            from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

            if isinstance(result, (_pa_local.Array, _pa_local.ChunkedArray)):
                result = _vfa(result)
            elif not hasattr(result, "__iter__") or isinstance(result, (str, bytes)):
                from opteryx.compiled.draken.vectors.scalar_constructors import (
                    from_scalar as _const_scalar,
                )

                vec = _const_scalar(result, morsel.num_rows)
                result = _vfa(_pa_local.array([result] * morsel.num_rows)) if vec is None else vec
            else:
                result = vector_from_sequence(result)
        col_names.append(identity)
        col_vecs.append(result)
        existing.add(identity)

    return Morsel.from_vectors(col_names, col_vecs)


__all__ = ["draken_compare", "evaluate_and_append_draken", "evaluate_draken"]
