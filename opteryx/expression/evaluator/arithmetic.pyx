"""Binary arithmetic operations."""

import datetime

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type



cdef _to_string_vec(v, n):
    """Ensure v is a StringVector of length n for vector_concat."""
    from draken.vectors.string_vector import StringVector
    if isinstance(v, StringVector):
        return v
    # Scalar str/bytes/None — broadcast to constant StringVector.
    if isinstance(v, str):
        v = v.encode("utf-8")
    return StringVector.from_constant(v, n)


cpdef _eval_binary_op_draken(node, morsel):
    op = node.value
    left = _eval_value(node.left, morsel)
    right = _eval_value(node.right, morsel)

    from opteryx.types import OrsoTypes

    if get_vector_type(left) == VectorType.UNKNOWN and node.left.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if node.left.schema_column.type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector
            left = Date32Vector.from_constant(_coerce_date32(left), morsel.num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector
            left = TimestampVector.from_constant(_coerce_timestamp(left), morsel.num_rows)

    if get_vector_type(right) == VectorType.UNKNOWN and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if node.right.schema_column.type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector
            right = Date32Vector.from_constant(_coerce_date32(right), morsel.num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector
            right = TimestampVector.from_constant(_coerce_timestamp(right), morsel.num_rows)

    left_type = get_vector_type(left)
    right_type = get_vector_type(right)

    cdef bint left_is_date = left_type in (VectorType.DATE32, VectorType.TIMESTAMP)
    cdef bint right_is_date = right_type in (VectorType.DATE32, VectorType.TIMESTAMP)

    if op == "Minus" and left_is_date and right_is_date:
        return _date_minus_date_draken(left, right)

    if op in ("Plus", "Minus"):
        left_is_interval = left_type == VectorType.INTERVAL
        right_is_interval = right_type == VectorType.INTERVAL
        if left_is_date and right_is_interval:
            return _date_interval_op_draken(left, right, op)
        if left_is_interval and right_is_date:
            return _date_interval_op_draken(right, left, op)

    if op == "StringConcat":
        from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc
        from draken.vectors.string_vector import StringVector as _SVT
        n = len(left) if isinstance(left, _SVT) else (len(right) if isinstance(right, _SVT) else 1)
        return _vc(_to_string_vec(left, n), _to_string_vec(right, n))

    from opteryx.expression.binary_operators import BINARY_OPERATORS

    if op not in BINARY_OPERATORS:
        return None

    result = call_arithmetic_op(op, left, right)

    if result is None:
        raise NotImplementedError(
            f"Operator `{op}` has no Draken kernel for {left.__class__.__name__} and "
            f"{right.__class__.__name__}."
        )

    if get_vector_type(result) == VectorType.UNKNOWN and not isinstance(
        result,
        (
            type(None),
            bool,
            int,
            float,
            str,
            bytes,
            datetime.date,
            datetime.datetime,
            datetime.time,
            tuple,
        ),
    ):
        raise TypeError(
            f"Arithmetic op `{op}` returned non-Draken value type {result.__class__.__name__}."
        )

    return result


cpdef _binary_op_from_vecs(str op, left, right, left_orso_type, right_orso_type, Py_ssize_t num_rows):
    """Execute a binary arithmetic op on pre-evaluated vectors.

    Equivalent to _eval_binary_op_draken but takes pre-evaluated vectors and
    orso types directly — no node or morsel access. Called from the bytecode
    executor for BC_BINARY_OP instructions.
    """
    from opteryx.types import OrsoTypes

    if get_vector_type(left) == VectorType.UNKNOWN and left_orso_type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if left_orso_type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector
            left = Date32Vector.from_constant(_coerce_date32(left), num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector
            left = TimestampVector.from_constant(_coerce_timestamp(left), num_rows)

    if get_vector_type(right) == VectorType.UNKNOWN and right_orso_type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if right_orso_type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector
            right = Date32Vector.from_constant(_coerce_date32(right), num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector
            right = TimestampVector.from_constant(_coerce_timestamp(right), num_rows)

    left_type = get_vector_type(left)
    right_type = get_vector_type(right)

    cdef bint left_is_date = left_type in (VectorType.DATE32, VectorType.TIMESTAMP)
    cdef bint right_is_date = right_type in (VectorType.DATE32, VectorType.TIMESTAMP)

    if op == "Minus" and left_is_date and right_is_date:
        return _date_minus_date_draken(left, right)

    if op in ("Plus", "Minus"):
        left_is_interval = left_type == VectorType.INTERVAL
        right_is_interval = right_type == VectorType.INTERVAL
        if left_is_date and right_is_interval:
            return _date_interval_op_draken(left, right, op)
        if left_is_interval and right_is_date:
            return _date_interval_op_draken(right, left, op)

    if op == "StringConcat":
        from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc
        from draken.vectors.string_vector import StringVector as _SVT
        n = len(left) if isinstance(left, _SVT) else (len(right) if isinstance(right, _SVT) else 1)
        return _vc(_to_string_vec(left, n), _to_string_vec(right, n))

    from opteryx.expression.binary_operators import BINARY_OPERATORS

    if op not in BINARY_OPERATORS:
        return None

    result = call_arithmetic_op(op, left, right)

    if result is None:
        raise NotImplementedError(
            f"Operator `{op}` has no Draken kernel for {left.__class__.__name__} and "
            f"{right.__class__.__name__}."
        )

    return result
