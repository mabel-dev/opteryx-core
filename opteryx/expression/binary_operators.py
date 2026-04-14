# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any, Dict, List, Optional, Union

import pyarrow
from pyarrow import compute

from opteryx.compiled import vector_ops
from opteryx.expression.intervals import MICROSECONDS_PER_DAY
from opteryx.third_party.tktech import csimdjson as simdjson
from opteryx.types import OrsoTypes
from opteryx.utils.vector_types import is_draken_vector

# Initialize simdjson parser once
parser = simdjson.Parser()


def ArrowOp(documents, elements) -> pyarrow.Array:
    """JSON Selector"""
    element = elements[0]

    # Fast path: if the documents are dicts, delegate to the cython optimized op
    if len(documents) > 0 and isinstance(documents[0], dict):
        return vector_ops.cython_arrow_op(documents, element)

    if hasattr(documents, "to_numpy"):
        documents = documents.to_numpy(zero_copy_only=False)

    # Function to extract value from a document
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

    # Return the result as a PyArrow array
    return pyarrow.array(extracted_values)


def LongArrowOp(documents, elements) -> pyarrow.Array:
    """JSON Selector (as byte string)"""
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

    # Return the result as a PyArrow array
    return pyarrow.array(extracted_values, type=pyarrow.binary())


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
        return pyarrow.array(
            [
                (
                    None
                    if value is None
                    else (value[index] if -len(value) <= index < len(value) else None)
                )
                for value in array
            ],
            type=pyarrow.string(),
        )

    if isinstance(first_element, (bytes, bytearray, memoryview)):
        return pyarrow.array(
            [
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
            ],
            type=pyarrow.binary(),
        )

    if isinstance(first_element, (list, pyarrow.ListScalar)):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        from opteryx.compiled.vector_ops import vector_get_element

        pa_arr = pyarrow.array(
            [r if not isinstance(r, pyarrow.ListScalar) else r.as_py() for r in array]
        )
        return vector_get_element(vector_from_arrow(pa_arr), index)

    raise IncorrectTypeError(
        f"Map access is not supported for {type(first_element).__name__} values"
    )


def _ip_containment(left: List[Optional[str]], right: List[str]) -> List[Optional[bool]]:
    """
    Check if each IP address in 'left' is contained within the network specified in 'right'.

    Parameters:
        left: List[Optional[str]]
            List of IP addresses as strings.
        right: List[str]
            List containing the network as a string.

    Returns:
        List[Optional[bool]]:
            A list of boolean values indicating if each corresponding IP in 'left' is in 'right'.
    """

    from opteryx.compiled.vector_ops import vector_ip_in_cidr

    # Normalize the left values to Python str (or None). The compiled
    # Cython routine expects Python str objects; some readers return bytes
    # which cause a TypeError inside the extension. Convert bytes/bytearray
    # and memoryview to str by decoding as utf-8, leave None as-is.
    def _normalize_ip(v):
        if v is None:
            return None
        # PyArrow scalar wrappers (BinaryScalar, StringScalar, etc.) — unwrap first
        if hasattr(v, "as_py"):
            v = v.as_py()
            if v is None:
                return None
        # memoryview -> bytes
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
        normalized_left = [_normalize_ip(v) for v in left]
        import pyarrow as _pyarrow

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vector_from_arrow

        arr = _pyarrow.array(normalized_left, type=_pyarrow.string())
        cidr_str = right if isinstance(right, str) else str(right[0])
        return vector_ip_in_cidr(_vector_from_arrow(arr), cidr_str)
    except (IndexError, AttributeError, ValueError, TypeError) as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError(
            "The `|` operator can be used as bitwise OR or IP address containment only."
        ) from err


def _dispatch_arithmetic_operation(
    op: str, left, right, left_type: OrsoTypes, right_type: OrsoTypes
) -> Union[None, pyarrow.Array]:
    """
    Dispatch arithmetic operations with Draken kernels.

    Per architectural contract (FAIL-FAST):
    - Expression layer ONLY accepts Draken vectors
    - Draken kernels handle all vector operations
    - If kernel returns None (unsupported), raise error immediately
    - NO silent fallback to Python operators

    Parameters:
        op: str - Operator name (e.g., "Plus", "Minus")
        left: Operand (Draken vector required)
        right: Operand (Draken vector required)
        left_type: OrsoTypes - Type of left operand
        right_type: OrsoTypes - Type of right operand

    Returns:
        Result of the operation

    Raises:
        NotImplementedError: If operation not supported for input types
    """
    from opteryx.expression.evaluator.arithmetic_dispatch import call_arithmetic_op

    # Use Draken kernels exclusively
    result = call_arithmetic_op(op, left, right)

    if result is not None:
        return result

    # Kernel not available: architectural violation
    raise NotImplementedError(
        f"Operator `{op}` is not implemented for types {left_type} and {right_type}. "
        f"Left: {type(left).__name__}, Right: {type(right).__name__}"
    )


def binary_operations(
    left, left_type: OrsoTypes, operator: str, right, right_type: OrsoTypes
) -> pyarrow.Array:
    """
    Execute inline operators (e.g. the add in 3 + 4).

    Per architectural contract: only Draken vectors or Python scalars accepted.
    PyArrow/NumPy inputs are architectural violations (fail-fast).

    Parameters:
        left: Operand (Draken vector or scalar)
        operator: str - Operator to apply
        right: Operand (Draken vector or scalar)
    Returns:
        Result of the binary operation
    """
    # Phase 5.3.2: Try Draken arithmetic dispatch first for arithmetic operators
    # This prioritizes native Draken kernels over NumPy operations (fail-fast)
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
        # Dispatcher returned None: operation not supported
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

    if (
        operator == "Minus"
        and left_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
        and right_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP)
    ):
        # date - date = INTERVAL (months=0, microseconds=days_diff * MICROS_PER_DAY)
        # Work directly with Draken vectors; no PyArrow intermediate
        from opteryx.expression.intervals import _intervals_to_month_day_nano

        # Convert left to int64 array
        if hasattr(left, "to_numpy"):
            # Draken vector
            left_values = left.to_numpy(False).astype(numpy.int64)
        elif isinstance(left, (list, tuple)):
            # Python sequence
            left_values = numpy.array(
                [int(x) if x is not None else 0 for x in left], dtype=numpy.int64
            )
        elif isinstance(left, int):
            # Python scalar
            left_values = numpy.array([left], dtype=numpy.int64)
        else:
            raise TypeError(f"Unsupported type for date subtraction (left): {type(left).__name__}")

        # Convert right to int64 array
        if hasattr(right, "to_numpy"):
            # Draken vector
            right_values = right.to_numpy(False).astype(numpy.int64)
        elif isinstance(right, (list, tuple)):
            # Python sequence
            right_values = numpy.array(
                [int(x) if x is not None else 0 for x in right], dtype=numpy.int64
            )
        elif isinstance(right, int):
            # Python scalar
            right_values = numpy.array([right], dtype=numpy.int64)
        else:
            raise TypeError(
                f"Unsupported type for date subtraction (right): {type(right).__name__}"
            )

        # Compute difference and convert to intervals
        day_diff = left_values - right_values
        rows = [(0, int(d) * MICROSECONDS_PER_DAY) for d in day_diff]
        return _intervals_to_month_day_nano(rows)

    elif operator == "BitwiseOr" and OrsoTypes.VARCHAR in (left_type, right_type):
        return _ip_containment(left, right)

    elif operator == "StringConcat":
        if hasattr(left, "type") and pyarrow.types.is_binary(left.type):
            left = left.cast(pyarrow.large_utf8())
        if hasattr(right, "type") and pyarrow.types.is_binary(right.type):
            right = right.cast(pyarrow.large_utf8())

        if isinstance(left, str):
            left = pyarrow.scalar(left, type=pyarrow.large_utf8())
        if isinstance(right, str):
            right = pyarrow.scalar(right, type=pyarrow.large_utf8())

        if isinstance(left, pyarrow.Scalar) and pyarrow.types.is_binary(left.type):
            left = left.cast(pyarrow.large_utf8())
        if isinstance(right, pyarrow.Scalar) and pyarrow.types.is_binary(right.type):
            right = right.cast(pyarrow.large_utf8())

        if hasattr(left, "type") and not pyarrow.types.is_large_string(left.type):
            try:
                left = left.cast(pyarrow.large_utf8())
            except Exception:
                pass
        if hasattr(right, "type") and not pyarrow.types.is_large_string(right.type):
            try:
                right = right.cast(pyarrow.large_utf8())
            except Exception:
                pass

        delim = pyarrow.scalar("", type=pyarrow.large_utf8())
        return compute.binary_join_element_wise(left, right, delim)

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
    "StringConcat": compute.binary_join_element_wise,
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
