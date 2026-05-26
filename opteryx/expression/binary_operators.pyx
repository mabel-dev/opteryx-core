"""Binary operator dispatch.

Cython migration of the former binary_operators.py. The `OPERATOR_FUNCTION_MAP`
dict carries one entry per supported operator; arithmetic ops fail-fast at
this layer (they're handled exclusively by arithmetic_dispatch.call_arithmetic_op),
and the remaining entries route to native bitwise / extraction / interval
kernels.
"""

from opteryx.compiled import vector_ops
from opteryx.compiled.nanobind.vector_bitwise import (
    vector_bitwise_or as _vector_bitwise_or,
    vector_bitwise_and as _vector_bitwise_and,
    vector_bitwise_xor as _vector_bitwise_xor,
    vector_bitwise_shift_left as _vector_bitwise_shift_left,
    vector_bitwise_shift_right as _vector_bitwise_shift_right,
)
from opteryx.exceptions import IncorrectTypeError, UnsupportedTypeError
from opteryx.expression.evaluator.arithmetic_dispatch import call_arithmetic_op
from opteryx.types import OrsoTypes

import draken.draken_native as _draken_native


cpdef bytes _json_key_constant(key):
    """Extract key bytes from a string Vector; value is taken from logical row 0."""
    key_type = getattr(key, "type", None)
    if key_type is None or key_type.name not in ("VARCHAR", "NVARCHAR", "VARBINARY"):
        raise IncorrectTypeError("JSON extraction key must be a string Vector")
    raw_key = key[0]
    if raw_key is None:
        raise IncorrectTypeError("JSON extraction key cannot be NULL")
    return raw_key


def ArrowOp(documents, elements):
    """JSON selector returning a VARCHAR DrakenVector. Maps to `->` SQL operator."""
    from opteryx.compiled.nanobind.vector_json import vector_json_extract

    element = _json_key_constant(elements)
    return vector_json_extract(documents, element)


def LongArrowOp(documents, elements):
    """JSON selector returning text as a VARCHAR DrakenVector. SQL `->>`."""
    from opteryx.compiled.nanobind.vector_json import vector_json_extract

    element = _json_key_constant(elements)
    return vector_json_extract(documents, element)


def MapAccessOp(array, key):
    """Map / iterable subscript over Draken vectors."""
    from opteryx.compiled.nanobind.vector_special import vector_map_access_array, vector_map_access_string

    key_type = getattr(key, "type", None)
    if key_type is None or key_type.name not in ("INT64", "INT32", "INT16", "INT8"):
        raise IncorrectTypeError("Map/iterable subscript key must be an integer Vector")

    array_type = getattr(array, "type", None)
    if array_type is not None and array_type.name in ("VARCHAR", "NVARCHAR", "VARBINARY"):
        return vector_map_access_string(array, key)
    if array_type is not None and array_type.name == "ARRAY":
        return _draken_native.vector_from_sequence(vector_map_access_array(array, key))
    raise IncorrectTypeError(
        f"Map access is only supported for Array/String vectors, "
        f"not {type(array).__name__}"
    )


cdef _dispatch_arithmetic_operation(str op, left, right, left_type, right_type):
    """Route arithmetic through the Draken kernel registry; fail-fast on miss."""
    result = call_arithmetic_op(op, left, right)
    if result is not None:
        return result
    raise NotImplementedError(
        f"Operator `{op}` is not implemented for types {left_type} and {right_type}. "
        f"Left: {type(left).__name__}, Right: {type(right).__name__}"
    )


cdef bytes _to_bytes_or_vec(v):
    """Normalise Python str -> bytes; pass everything else through unchanged."""
    if isinstance(v, str):
        return v.encode("utf-8")
    return v


cdef frozenset _ARITHMETIC_OPS = frozenset(
    ("Plus", "Minus", "Multiply", "Divide", "Modulo", "MyIntegerDivide")
)


def binary_operations(left, left_type, str operator, right, right_type):
    """Execute inline operators (e.g. the `+` in `3 + 4`)."""
    if operator in _ARITHMETIC_OPS:
        result = _dispatch_arithmetic_operation(
            operator, left, right, left_type, right_type
        )
        if result is not None:
            return result
        raise NotImplementedError(
            f"Operator `{operator}` is not implemented for "
            f"types {left_type} and {right_type}!"
        )

    operation = OPERATOR_FUNCTION_MAP.get(operator)
    if operation is None:
        raise NotImplementedError(f"Operator `{operator}` is not implemented!")

    if OrsoTypes.INTERVAL == left_type or OrsoTypes.INTERVAL == right_type:
        from opteryx.expression.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )
        return function(left, left_type, right, right_type, operator)

    if operator == "BitwiseOr" and (
        OrsoTypes.VARCHAR == left_type or OrsoTypes.VARCHAR == right_type
    ):
        from opteryx.compiled.nanobind.vector_misc import vector_ip_in_cidr
        return vector_ip_in_cidr(left, right)

    if operator == "StringConcat":
        from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc
        _str_types = (_draken_native.VARCHAR, _draken_native.NVARCHAR)
        left_is_str = getattr(left, "type", None) in _str_types
        right_is_str = getattr(right, "type", None) in _str_types
        n = len(left) if left_is_str else (len(right) if right_is_str else 1)
        def _to_str_vec(v):
            if getattr(v, "type", None) in _str_types:
                return v
            if isinstance(v, bytes):
                v = v.decode("utf-8")
            elif not isinstance(v, str):
                v = str(v) if v is not None else None
            return _draken_native.vector_varchar_from_constant(v, n)
        return _vc(_to_str_vec(left), _to_str_vec(right))

    return operation(left, right)


def _unsupported_bitwise_op(op_name):
    """Factory: returns a callable that raises for the named operator.

    Used as placeholder entries in OPERATOR_FUNCTION_MAP for ops that must
    have been routed via _dispatch_arithmetic_operation before reaching here.
    """

    def _op(left, right):
        raise TypeError(
            f"Bitwise operation '{op_name}' requires Draken vectors at "
            f"expression layer. Left type: {type(left).__name__}, "
            f"Right type: {type(right).__name__}"
        )

    return _op


# fmt:off
OPERATOR_FUNCTION_MAP = {
    # Arithmetic ops should have been dispatched before reaching this table.
    "Plus":            _unsupported_bitwise_op("Plus"),
    "Minus":           _unsupported_bitwise_op("Minus"),
    "Multiply":        _unsupported_bitwise_op("Multiply"),
    "Divide":          _unsupported_bitwise_op("Divide"),
    "Modulo":          _unsupported_bitwise_op("Modulo"),
    "MyIntegerDivide": _unsupported_bitwise_op("MyIntegerDivide"),
    "StringConcat":    _unsupported_bitwise_op("StringConcat"),
    # Bitwise: native vector ops.
    "BitwiseOr":  _vector_bitwise_or,
    "BitwiseAnd": _vector_bitwise_and,
    "BitwiseXor": _vector_bitwise_xor,
    "ShiftLeft":  _vector_bitwise_shift_left,
    "ShiftRight": _vector_bitwise_shift_right,
    # Extraction operators.
    "Arrow":     ArrowOp,
    "LongArrow": LongArrowOp,
    "MapAccess": MapAccessOp,
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys()) - {"Arrow", "LongArrow", "MapAccess"}
EXTRACTION_OPERATORS = {"Arrow", "LongArrow", "MapAccess"}
# fmt:on
