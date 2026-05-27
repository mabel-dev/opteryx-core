"""Binary arithmetic operations."""

import datetime

from libc.stdint cimport int16_t

import draken.draken_native as _draken_native
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type


cdef _to_string_vec(v, n):
    """Ensure v is a string Vector of length n for vector_concat.

    For scalar inputs produces a constant-shape Vector (O(1) allocation,
    data_length==1) via vector_varchar_from_constant.
    """
    _str_types = (_draken_native.VARCHAR, _draken_native.NVARCHAR)
    if getattr(v, "type", None) in _str_types:
        return v  # already a string Vector
    # Normalize scalar to str or None.
    if isinstance(v, bytes):
        v = v.decode("utf-8")
    elif isinstance(v, str):
        pass
    elif v is None:
        pass
    else:
        v = str(v)
    return _draken_native.vector_varchar_from_constant(v, n)


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
            left = _draken_native.vector_date32_from_constant(_coerce_date32(left), morsel.num_rows)
        else:
            left = _draken_native.vector_timestamp_from_constant(_coerce_timestamp(left), morsel.num_rows)

    if get_vector_type(right) == VectorType.UNKNOWN and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if node.right.schema_column.type == OrsoTypes.DATE:
            right = _draken_native.vector_date32_from_constant(_coerce_date32(right), morsel.num_rows)
        else:
            right = _draken_native.vector_timestamp_from_constant(_coerce_timestamp(right), morsel.num_rows)

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
        _str_types = (_draken_native.VARCHAR, _draken_native.NVARCHAR)
        n = len(left) if getattr(left, "type", None) in _str_types else (
            len(right) if getattr(right, "type", None) in _str_types else 1
        )
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


cpdef _binary_op_from_vecs(
    int op_code,
    left, right,
    int16_t left_type_code,
    int16_t right_type_code,
    str op_str,
    Py_ssize_t num_rows,
):
    """Execute a binary arithmetic op on pre-evaluated vectors.

    Equivalent to _eval_binary_op_draken but takes pre-evaluated vectors and
    BCTypeCode integers directly — no node or morsel access. Called from the
    bytecode executor for BC_BINARY_OP instructions.

    op_code: BCBinaryOpCode int (BOP_PLUS, BOP_MINUS, etc.)
    left_type_code / right_type_code: BCTypeCode (BC_TYPE_NONE=0, DATE=1, TIMESTAMP=2)
    op_str: still needed for call_arithmetic_op which dispatches to the kernel registry
    """
    if get_vector_type(left) == VectorType.UNKNOWN and (
        left_type_code == BC_TYPE_DATE or left_type_code == BC_TYPE_TIMESTAMP
    ):
        if left_type_code == BC_TYPE_DATE:
            left = _draken_native.vector_date32_from_constant(_coerce_date32(left), num_rows)
        else:
            left = _draken_native.vector_timestamp_from_constant(_coerce_timestamp(left), num_rows)

    if get_vector_type(right) == VectorType.UNKNOWN and (
        right_type_code == BC_TYPE_DATE or right_type_code == BC_TYPE_TIMESTAMP
    ):
        if right_type_code == BC_TYPE_DATE:
            right = _draken_native.vector_date32_from_constant(_coerce_date32(right), num_rows)
        else:
            right = _draken_native.vector_timestamp_from_constant(_coerce_timestamp(right), num_rows)

    left_type = get_vector_type(left)
    right_type = get_vector_type(right)

    cdef bint left_is_date = left_type in (VectorType.DATE32, VectorType.TIMESTAMP)
    cdef bint right_is_date = right_type in (VectorType.DATE32, VectorType.TIMESTAMP)

    if op_code == BOP_MINUS and left_is_date and right_is_date:
        return _date_minus_date_draken(left, right)

    if op_code == BOP_PLUS or op_code == BOP_MINUS:
        left_is_interval = left_type == VectorType.INTERVAL
        right_is_interval = right_type == VectorType.INTERVAL
        if left_is_date and right_is_interval:
            return _date_interval_op_draken(left, right, op_str)
        if left_is_interval and right_is_date:
            return _date_interval_op_draken(right, left, op_str)

    if op_code == BOP_STRING_CONCAT:
        from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc
        _str_types = (_draken_native.VARCHAR, _draken_native.NVARCHAR)
        n = len(left) if getattr(left, "type", None) in _str_types else (
            len(right) if getattr(right, "type", None) in _str_types else 1
        )
        return _vc(_to_string_vec(left, n), _to_string_vec(right, n))

    if op_code == BOP_UNKNOWN:
        return None

    result = call_arithmetic_op(op_str, left, right)

    if result is None:
        raise NotImplementedError(
            f"Operator `{op_str}` has no Draken kernel for {left.__class__.__name__} and "
            f"{right.__class__.__name__}."
        )

    return result
