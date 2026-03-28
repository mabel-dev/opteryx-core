"""Binary arithmetic operations."""

import datetime

import numpy
import pyarrow as _pa
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .comparisons import _DATE_TYPES
from .comparisons import _INTERVAL_TYPES
from .function_execution import _is_draken_vector
from .function_execution import apply_bounded_function
from .temporal_ops import _date_interval_op_draken
from .temporal_ops import _date_minus_date_draken
from .type_coercion import _coerce_date32
from .type_coercion import _coerce_date32_set
from .type_coercion import _coerce_float
from .type_coercion import _coerce_float_set
from .type_coercion import _coerce_int64
from .type_coercion import _coerce_int64_set
from .type_coercion import _coerce_interval
from .type_coercion import _coerce_str
from .type_coercion import _coerce_str_set
from .type_coercion import _coerce_temporal_scalar_for_arrow
from .type_coercion import _coerce_timestamp
from .type_coercion import _coerce_timestamp_set
from .type_coercion import _constant_scalar_value
from .type_coercion import _dictionary_arrow_type
from .type_coercion import _dictionary_compare_vector
from .type_coercion import _is_constant_vector_like
from .type_coercion import _is_dictionary_encoded_vector
from .type_coercion import _is_null_as_boolvector
from .type_coercion import _is_typed_constant_encoded_vector

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


def _eval_binary_op_draken(node, morsel):
    from .evaluation import _eval_value

    op = node.value
    left = _eval_value(node.left, morsel)
    right = _eval_value(node.right, morsel)

    from orso.types import OrsoTypes

    if not hasattr(left, "null_count") and node.left.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arrow_type = (
            _pa.date32() if node.left.schema_column.type == OrsoTypes.DATE else _pa.timestamp("us")
        )
        scalar = _coerce_temporal_scalar_for_arrow(left, node.left.schema_column.type)
        left = vector_from_arrow(_pa.array([scalar] * morsel.num_rows, type=arrow_type))
    if not hasattr(right, "null_count") and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arrow_type = (
            _pa.date32() if node.right.schema_column.type == OrsoTypes.DATE else _pa.timestamp("us")
        )
        scalar = _coerce_temporal_scalar_for_arrow(right, node.right.schema_column.type)
        right = vector_from_arrow(_pa.array([scalar] * morsel.num_rows, type=arrow_type))

    left_cls = left.__class__.__name__
    right_cls = right.__class__.__name__

    if op == "Minus" and left_cls in _DATE_TYPES and right_cls in _DATE_TYPES:
        return _date_minus_date_draken(left, right)

    if op in ("Plus", "Minus"):
        if left_cls in _DATE_TYPES and right_cls in _INTERVAL_TYPES:
            return _date_interval_op_draken(left, right, op)
        if left_cls in _INTERVAL_TYPES and right_cls in _DATE_TYPES:
            return _date_interval_op_draken(right, left, op)

    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.compiled.draken.interop.arrow import vector_from_sequence
    from opteryx.expression.binary_operators import BINARY_OPERATORS
    from opteryx.expression.binary_operators import binary_operations

    if op not in BINARY_OPERATORS:
        return None

    if hasattr(left, "to_arrow"):
        left = left.to_arrow()
    if hasattr(right, "to_arrow"):
        right = right.to_arrow()

    result = binary_operations(
        left,
        node.left.schema_column.type,
        op,
        right,
        node.right.schema_column.type,
    )
    if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
        return vector_from_arrow(result)
    return vector_from_sequence(result)
