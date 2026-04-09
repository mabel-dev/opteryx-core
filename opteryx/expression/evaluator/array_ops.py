"""Array operation utilities."""

import datetime

import numpy
import pyarrow as _pa

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .function_execution import _is_draken_vector
from .function_execution import apply_bounded_function
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


def _ensure_array_vector(val):
    if val.__class__.__name__ == "ArrowVector":
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(val.to_arrow())
    return val
