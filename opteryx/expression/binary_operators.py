# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any, Dict

from opteryx.compiled import vector_ops
from opteryx.types import OrsoTypes


def ArrowOp(documents, elements):
    """JSON selector returning a Draken vector."""
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
    from opteryx.compiled.vector_ops import vector_json_extract_variant
    from opteryx.exceptions import IncorrectTypeError

    element = _json_key_constant(elements)
    extracted_values = vector_json_extract_variant(documents, element)

    try:
        result = vector_from_sequence(extracted_values)
    except Exception as err:
        raise IncorrectTypeError("The `->` operator produced complex/mixed values.") from err

    if isinstance(result, list):
        raise IncorrectTypeError("The `->` operator produced complex/mixed values.")
    return result


def LongArrowOp(documents, elements):
    """JSON selector returning text as a StringVector of bytes."""
    from opteryx.compiled.vector_ops import vector_json_extract_text

    element = _json_key_constant(elements)
    return vector_json_extract_text(documents, element)


def _json_key_constant(key) -> bytes:
    from opteryx.compiled.draken import encoding as draken_encoding
    from opteryx.compiled.draken.vectors.string_vector import StringVector
    from opteryx.exceptions import IncorrectTypeError

    if not isinstance(key, StringVector):
        raise IncorrectTypeError("JSON extraction key must be a StringVector")
    if key.encoding != draken_encoding.DRAKEN_ENCODING_CONSTANT:
        raise IncorrectTypeError("JSON extraction key must be constant encoded")

    raw_key = key[0]
    if raw_key is None:
        raise IncorrectTypeError("JSON extraction key cannot be NULL")
    return raw_key


def MapAccessOp(array, key):
    """Map/iterable subscript accessor over Draken vectors."""

    from opteryx.compiled.draken import encoding as draken_encoding
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
    from opteryx.compiled.draken.vectors.array_vector import ArrayVector
    from opteryx.compiled.draken.vectors.int64_vector import Int64Vector
    from opteryx.compiled.draken.vectors.string_vector import StringVector
    from opteryx.compiled.vector_ops import vector_map_access_array, vector_map_access_string
    from opteryx.exceptions import IncorrectTypeError

    if not isinstance(key, Int64Vector):
        raise IncorrectTypeError("Map/iterable subscript key must be an Int64Vector")

    if getattr(key, "encoding", None) != draken_encoding.DRAKEN_ENCODING_CONSTANT:
        raise IncorrectTypeError("Map/iterable subscript key must be constant encoded")

    if isinstance(array, StringVector):
        return vector_map_access_string(array, key)
    if isinstance(array, ArrayVector):
        return vector_from_sequence(vector_map_access_array(array, key))
    raise IncorrectTypeError(
        f"Map access is only supported for ArrayVector/StringVector, not {type(array).__name__}"
    )


def _dispatch_arithmetic_operation(
    op: str, left, right, left_type: OrsoTypes, right_type: OrsoTypes
):
    """
    Dispatch arithmetic operations with Draken kernels.

    Per architectural contract (FAIL-FAST):
    - Expression layer ONLY accepts Draken vectors
    - Draken kernels handle all vector operations
    - If kernel returns None (unsupported), raise error immediately
    - NO silent fallback to Python operators
    """
    from opteryx.expression.evaluator.arithmetic_dispatch import call_arithmetic_op

    result = call_arithmetic_op(op, left, right)

    if result is not None:
        return result

    raise NotImplementedError(
        f"Operator `{op}` is not implemented for types {left_type} and {right_type}. "
        f"Left: {type(left).__name__}, Right: {type(right).__name__}"
    )


def binary_operations(left, left_type: OrsoTypes, operator: str, right, right_type: OrsoTypes):
    """
    Execute inline operators (e.g. the add in 3 + 4).
    """
    if operator in (
        "Plus",
        "Minus",
        "Multiply",
        "Divide",
        "Modulo",
        "MyIntegerDivide",
    ):
        result = _dispatch_arithmetic_operation(operator, left, right, left_type, right_type)
        if result is not None:
            return result
        raise NotImplementedError(
            f"Operator `{operator}` is not implemented for types {left_type} and {right_type}!"
        )

    operation = OPERATOR_FUNCTION_MAP.get(operator)

    if operation is None:
        raise NotImplementedError(f"Operator `{operator}` is not implemented!")

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.expression.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )

        return function(left, left_type, right, right_type, operator)

    if operator == "BitwiseOr" and OrsoTypes.VARCHAR in (left_type, right_type):
        from opteryx.compiled.vector_ops import vector_ip_in_cidr

        return vector_ip_in_cidr(left, right)

    if operator == "StringConcat":
        from opteryx.compiled.vector_ops import vector_string_concat_binary

        # Normalise Python str to bytes so the Cython kernel receives bytes or StringVector
        def _to_bytes_or_vec(v):
            if isinstance(v, str):
                return v.encode("utf-8")
            return v  # already bytes, None, or StringVector

        return vector_string_concat_binary(_to_bytes_or_vec(left), _to_bytes_or_vec(right))

    return operation(left, right)


def _unsupported_bitwise_op(op_name):
    """Factory for bitwise operations that are not supported in expression layer."""

    def _op(left, right):
        raise TypeError(
            f"Bitwise operation '{op_name}' requires Draken vectors at expression layer. "
            f"Left type: {type(left).__name__}, Right type: {type(right).__name__}"
        )

    return _op


# fmt:off
OPERATOR_FUNCTION_MAP: Dict[str, Any] = {
    # Arithmetic operators: dispatch via _dispatch_arithmetic_operation() first
    # If this table is reached, it's an error
    "Plus": lambda l, r: _unsupported_bitwise_op("Plus")(l, r),
    "Minus": lambda l, r: _unsupported_bitwise_op("Minus")(l, r),
    "Multiply": lambda l, r: _unsupported_bitwise_op("Multiply")(l, r),
    "Divide": lambda l, r: _unsupported_bitwise_op("Divide")(l, r),
    "Modulo": lambda l, r: _unsupported_bitwise_op("Modulo")(l, r),
    "MyIntegerDivide": lambda l, r: _unsupported_bitwise_op("MyIntegerDivide")(l, r),
    # String operations
    "StringConcat": lambda l, r: _unsupported_bitwise_op("StringConcat")(l, r),
    # Bitwise operations: route to native vector ops
    "BitwiseOr": lambda left, right: vector_ops.vector_bitwise_or(left, right),
    "BitwiseAnd": lambda left, right: vector_ops.vector_bitwise_and(left, right),
    "BitwiseXor": lambda left, right: vector_ops.vector_bitwise_xor(left, right),
    "ShiftLeft": lambda left, right: vector_ops.vector_left_shift(left, right),
    "ShiftRight": lambda left, right: vector_ops.vector_right_shift(left, right),
    # Special extraction operators
    "Arrow": ArrowOp,
    "LongArrow": LongArrowOp,
    "MapAccess": MapAccessOp,
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys()) - {"Arrow", "LongArrow", "MapAccess"}
EXTRACTION_OPERATORS = {"Arrow", "LongArrow", "MapAccess"}

# fmt:on
