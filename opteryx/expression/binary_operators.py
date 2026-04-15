# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any, Dict, List, Optional, Union

from opteryx.compiled import vector_ops
from opteryx.third_party.tktech import csimdjson as simdjson
from opteryx.types import OrsoTypes
from opteryx.utils.vector_types import is_draken_vector

# Initialize simdjson parser once
parser = simdjson.Parser()


def ArrowOp(documents, elements):
    """JSON Selector"""
    import pyarrow as _pyarrow

    element = elements[0]

    # Fast path: if the documents are dicts, delegate to the cython optimized op
    if len(documents) > 0 and isinstance(documents[0], dict):
        return vector_ops.cython_arrow_op(documents, element)

    if hasattr(documents, "to_numpy"):
        documents = documents.to_numpy(zero_copy_only=False)

    def extract(doc: bytes, elem: Union[bytes, str]) -> Any:
        value = parser.parse(doc).get(elem)  # type: ignore
        if hasattr(value, "as_list"):
            return value.as_list()
        if hasattr(value, "as_dict"):
            return value.mini
        return value

    try:
        extracted_values = [None if d is None else extract(d, element) for d in documents]
    except ValueError as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("The `->` operator can only be used on JSON documents.") from err

    return _pyarrow.array(extracted_values)


def LongArrowOp(documents, elements):
    """JSON Selector (as byte string)"""
    import pyarrow as _pyarrow

    element = elements[0]

    if len(documents) > 0 and isinstance(documents[0], dict):
        return vector_ops.cython_long_arrow_op(documents, element)

    if hasattr(documents, "to_numpy"):
        documents = documents.to_numpy(zero_copy_only=False)

    def extract(doc: bytes, elem: Union[bytes, str]) -> bytes:
        value = parser.parse(doc).get(elem)  # type: ignore
        if hasattr(value, "mini"):
            return value.mini  # type: ignore
        return None if value is None else str(value).encode()

    try:
        extracted_values = [None if d is None else extract(d, element) for d in documents]
    except ValueError as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("The `->>` operator can only be used on JSON documents.") from err

    return _pyarrow.array(extracted_values, type=_pyarrow.binary())


def MapAccessOp(array, key):
    """Map/iterable subscript accessor."""
    from opteryx.exceptions import IncorrectTypeError

    if hasattr(array, "to_numpy"):
        array = array.to_numpy(False)

    # Determine the type of the first non-null element.
    first_element = next((item for item in array if item is not None), None)
    if first_element is None:
        return [None] * len(array)

    raw_key = key[0]
    if hasattr(raw_key, "as_py"):
        raw_key = raw_key.as_py()
    if raw_key is None or isinstance(raw_key, bool) or not isinstance(raw_key, int):
        raise IncorrectTypeError("Map/iterable values must be subscripted with INTEGER values")
    index = int(raw_key)

    if isinstance(first_element, str):
        return [
            (
                None
                if value is None
                else (value[index] if -len(value) <= index < len(value) else None)
            )
            for value in array
        ]

    if isinstance(first_element, (bytes, bytearray, memoryview)):
        return [
            (
                None
                if value is None
                else (
                    bytes(value)[index : index + 1]
                    if -len(bytes(value)) <= index < len(bytes(value))
                    else None
                )
            )
            for value in array
        ]

    if isinstance(first_element, list):
        import pyarrow as _pyarrow

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        from opteryx.compiled.vector_ops import vector_get_element

        pa_arr = _pyarrow.array(list(array))
        return vector_get_element(vector_from_arrow(pa_arr), index)

    # PyArrow ListScalar — convert to Python lists first
    if hasattr(first_element, "as_py"):
        import pyarrow as _pyarrow

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        from opteryx.compiled.vector_ops import vector_get_element

        pa_arr = _pyarrow.array([r.as_py() if hasattr(r, "as_py") else r for r in array])
        return vector_get_element(vector_from_arrow(pa_arr), index)

    raise IncorrectTypeError(
        f"Map access is not supported for {type(first_element).__name__} values"
    )


def _ip_containment(left, right) -> List[Optional[bool]]:
    """
    Check if each IP address in 'left' is contained within the network in 'right'.

    Accepts Draken StringVector, PyArrow arrays, or plain Python iterables as 'left'.
    """
    from opteryx.compiled.vector_ops import vector_ip_in_cidr

    cidr_str = right if isinstance(right, str) else str(right[0])

    # Fast path: already a Draken StringVector — pass directly to the kernel
    if left.__class__.__name__ == "StringVector":
        return vector_ip_in_cidr(left, cidr_str)

    # Slower path: normalise arbitrary input to a Draken StringVector
    def _normalize_ip(v):
        if v is None:
            return None
        if hasattr(v, "as_py"):
            v = v.as_py()
            if v is None:
                return None
        if isinstance(v, memoryview):
            try:
                v = v.tobytes()
            except Exception:
                v = bytes(v)
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8")
            except Exception:
                return str(v)
        if not isinstance(v, str):
            return str(v)
        return v

    try:
        if hasattr(left, "to_numpy"):
            left_iter = left.to_numpy(zero_copy_only=False)
        else:
            left_iter = left

        normalized = [_normalize_ip(v) for v in left_iter]

        from opteryx.compiled.draken.vectors import string_vector as _sv_module

        n = len(normalized)
        builder = _sv_module.StringVectorBuilder.with_estimate(n, 20)
        for v in normalized:
            if v is None:
                builder.append_null()
            else:
                builder.append(v.encode("utf-8"))
        left_vec = builder.finish()

        return vector_ip_in_cidr(left_vec, cidr_str)

    except (IndexError, AttributeError, ValueError, TypeError) as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError(
            "The `|` operator can be used as bitwise OR or IP address containment only."
        ) from err


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

    Per architectural contract: only Draken vectors or Python scalars accepted.
    PyArrow/NumPy inputs are architectural violations (fail-fast).
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
        return _ip_containment(left, right)

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
