# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import vector_ops
from opteryx.datatypes.intervals import MICROSECONDS_PER_DAY
from opteryx.third_party.tktech import csimdjson as simdjson

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
        value = parser.parse(doc).get(elem)  # type:ignore
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
        value = parser.parse(doc).get(elem)  # type:ignore
        if hasattr(value, "mini"):
            return value.mini  # type:ignore
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
        return numpy.full(len(array), None)

    raw_key = key[0]
    if hasattr(raw_key, "as_py"):
        raw_key = raw_key.as_py()
    if (
        raw_key is None
        or isinstance(raw_key, (bool, numpy.bool_))
        or not isinstance(raw_key, (int, numpy.integer))
    ):
        raise IncorrectTypeError("Map/iterable values must be subscripted with INTEGER values")
    index = int(raw_key)

    if isinstance(first_element, str):
        return pyarrow.array(
            [
                None
                if value is None
                else (value[index] if -len(value) <= index < len(value) else None)
                for value in array
            ],
            type=pyarrow.string(),
        )

    if isinstance(first_element, (bytes, bytearray, memoryview)):
        return pyarrow.array(
            [
                None
                if value is None
                else (
                    bytes(value)[index : index + 1]
                    if -len(bytes(value)) <= index < len(bytes(value))
                    else None
                )
                for value in array
            ],
            type=pyarrow.binary(),
        )

    if isinstance(first_element, (list, pyarrow.ListScalar, numpy.ndarray)):
        from opteryx.compiled.vector_ops import vector_get_element
        from opteryx.draken.interop.arrow import vector_from_arrow

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

        from opteryx.draken.interop.arrow import vector_from_arrow as _vector_from_arrow

        arr = _pyarrow.array(normalized_left, type=_pyarrow.string())
        return vector_ip_in_cidr(_vector_from_arrow(arr), str(right[0]))
    except (IndexError, AttributeError, ValueError, TypeError) as err:
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError(
            "The `|` operator can be used as bitwise OR or IP address containment only."
        ) from err


def binary_operations(
    left, left_type: OrsoTypes, operator: str, right, right_type: OrsoTypes
) -> Union[numpy.ndarray, pyarrow.Array]:
    """
    Execute inline operators (e.g. the add in 3 + 4).

    Parameters:
        left: Union[numpy.ndarray, pyarrow.Array]
            The left operand
        operator: str
            The operator to be applied
        right: Union[numpy.ndarray, pyarrow.Array]
            The right operand
    Returns:
        Union[numpy.ndarray, pyarrow.Array]
            The result of the binary operation
    """
    operation = OPERATOR_FUNCTION_MAP.get(operator)

    if operation is None:
        raise NotImplementedError(f"Operator `{operator}` is not implemented!")

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.datatypes.intervals import INTERVAL_KERNELS

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
        # Normalise both sides to a pyarrow date32 or timestamp array — avoids the
        # numpy object-array-of-datetime.date path that breaks astype(int64).
        from opteryx.datatypes.intervals import _intervals_to_month_day_nano

        def _to_pyarrow_date(arr):
            if hasattr(arr, "to_arrow"):
                return arr.to_arrow()
            if isinstance(arr, pyarrow.ChunkedArray):
                return arr.combine_chunks() if arr.num_chunks > 1 else arr.chunk(0)
            if isinstance(arr, pyarrow.Array):
                return arr
            # numpy object array (datetime.date values from _inner_evaluate)
            return pyarrow.array(arr)

        left_arr = _to_pyarrow_date(left)
        right_arr = _to_pyarrow_date(right)

        # Cast to int32 days-since-epoch (date32 → int32 is zero-copy in Arrow)
        left_days = left_arr.cast(pyarrow.int32())
        right_days = right_arr.cast(pyarrow.int32())
        day_diff = compute.subtract(left_days, right_days)

        rows = [
            None if not d.is_valid else (0, d.as_py() * MICROSECONDS_PER_DAY)
            for d in day_diff
        ]
        return _intervals_to_month_day_nano(rows)

    elif operator == "BitwiseOr" and OrsoTypes.VARCHAR in (left_type, right_type):
        return _ip_containment(left, right)

    elif operator == "StringConcat":
        return compute.binary_join_element_wise(left, right, "")

    return operation(left, right)


# fmt:off
OPERATOR_FUNCTION_MAP: Dict[str, Any] = {
    "Divide": numpy.divide,
    "Minus": numpy.subtract,
    "Modulo": numpy.mod,
    "Multiply": numpy.multiply,
    "Plus": numpy.add,
    "StringConcat": compute.binary_join_element_wise,
    "MyIntegerDivide": lambda left, right: numpy.trunc(numpy.divide(left, right)).astype(numpy.int64),
    "BitwiseOr": numpy.bitwise_or,
    "BitwiseAnd": numpy.bitwise_and,
    "BitwiseXor": numpy.bitwise_xor,
    "ShiftLeft": numpy.left_shift,
    "ShiftRight": numpy.right_shift,
    "Arrow": ArrowOp,
    "LongArrow": LongArrowOp,
    "MapAccess": MapAccessOp,
}

BINARY_OPERATORS = set(OPERATOR_FUNCTION_MAP.keys()) - {"Arrow", "LongArrow", "MapAccess"}
EXTRACTION_OPERATORS = {"Arrow", "LongArrow", "MapAccess"}

# fmt:on
