"""Type coercion utilities for Draken vector operations."""

import datetime
import decimal

import numpy
import pyarrow as _pa

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)
_DRAKEN_ENCODING_CONSTANT = 3


def _dictionary_arrow_type(vec):
    if isinstance(vec, (_pa.Array, _pa.ChunkedArray)):
        return vec.type if _pa.types.is_dictionary(vec.type) else None

    to_arrow = getattr(vec, "to_arrow", None)
    if to_arrow is None:
        return None

    try:
        arrow_arr = to_arrow()
    except Exception:
        return None

    if isinstance(arrow_arr, (_pa.Array, _pa.ChunkedArray)) and _pa.types.is_dictionary(
        arrow_arr.type
    ):
        return arrow_arr.type

    return None


def _is_dictionary_encoded_vector(vec) -> bool:
    return _dictionary_arrow_type(vec) is not None


def _dictionary_compare_vector(vec):
    if not _is_dictionary_encoded_vector(vec):
        return None

    if all(
        hasattr(vec, method)
        for method in (
            "equals",
            "not_equals",
            "in_list",
            "like",
            "rlike",
            "contains",
            "less_than",
            "greater_than",
            "less_than_or_equals",
            "greater_than_or_equals",
        )
    ):
        return vec

    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

    arrow_arr = vec.to_arrow() if hasattr(vec, "to_arrow") else vec
    if isinstance(arrow_arr, _pa.ChunkedArray):
        if arrow_arr.num_chunks != 1:
            raise NotImplementedError(
                "Dictionary compare path does not support multi-chunk dictionary arrays."
            )
        arrow_arr = arrow_arr.chunk(0)

    return vector_from_arrow(arrow_arr)


def _coerce_str(value) -> bytes:
    value = _constant_scalar_value(value)
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()


def _coerce_str_set(values) -> frozenset:
    return frozenset(_coerce_str(v) for v in values)


def _coerce_float(value) -> float:
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _coerce_float_set(values) -> frozenset:
    return frozenset(_coerce_float(v) for v in values)


def _is_typed_constant_encoded_vector(value) -> bool:
    return getattr(value, "encoding", None) == _DRAKEN_ENCODING_CONSTANT


def _is_constant_vector_like(value) -> bool:
    return _is_typed_constant_encoded_vector(value)


def _constant_scalar_value(value):
    if _is_typed_constant_encoded_vector(value):
        if len(value) == 0:
            return None
        return value[0]
    return value


def _coerce_int64(value) -> int:
    value = _constant_scalar_value(value)
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.generic):
        value = value.item()
    if isinstance(value, datetime.datetime):
        return int(value.timestamp() * 1_000)
    if isinstance(value, datetime.date):
        return (value - _EPOCH_DATE).days
    if isinstance(value, numpy.datetime64):
        return int(value.astype("datetime64[D]").astype(numpy.int64))
    return int(value)


def _coerce_int64_set(values) -> frozenset:
    return frozenset(_coerce_int64(v) for v in values)


def _coerce_date32(value) -> int:
    value = _constant_scalar_value(value)
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.generic):
        value = value.item()
    if isinstance(value, datetime.datetime):
        return (value.date() - _EPOCH_DATE).days
    if isinstance(value, datetime.date):
        return (value - _EPOCH_DATE).days
    return int(value)


def _coerce_date32_set(values) -> frozenset:
    return frozenset(_coerce_date32(v) for v in values)


def _coerce_timestamp(value) -> int:
    value = _constant_scalar_value(value)
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.generic):
        value = value.item()
    if isinstance(value, (bytes, bytearray, memoryview, str)):
        from opteryx.expression.casts import parse_timestamp_value

        value = parse_timestamp_value(value)
    if isinstance(value, numpy.datetime64):
        return int(value.astype("datetime64[us]").astype(numpy.int64))
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return int((value - _EPOCH_DATETIME).total_seconds() * 1_000_000)
    if isinstance(value, datetime.date):
        return int(
            (
                datetime.datetime(value.year, value.month, value.day) - _EPOCH_DATETIME
            ).total_seconds()
            * 1_000_000
        )

    return int(value)


def _coerce_timestamp_set(values) -> frozenset:
    return frozenset(_coerce_timestamp(v) for v in values)


def _coerce_interval(value) -> tuple:
    if isinstance(value, (tuple, list)) and len(value) == 2:
        return (int(value[0]), int(value[1]))
    raise TypeError(f"Cannot coerce {type(value)!r} to interval literal")


def _coerce_temporal_scalar_for_arrow(value, target_type):
    from opteryx.expression.casts import parse_timestamp_value
    from opteryx.types import OrsoTypes

    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.generic):
        value = value.item()

    if target_type == OrsoTypes.DATE:
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, (int, numpy.integer)):
            return _EPOCH_DATE + datetime.timedelta(days=int(value))
        return parse_timestamp_value(value).date()

    if target_type == OrsoTypes.TIMESTAMP:
        if isinstance(value, (int, numpy.integer)):
            ivalue = int(value)
            if abs(ivalue) < 100_000_000_000 and ivalue % 1_000_000 == 0:
                return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=ivalue // 1_000_000)
        return parse_timestamp_value(value)

    return value


_FIXED_BUFFER_VECTOR_CLASSES = frozenset(
    {
        "BoolVector",
        "Int64Vector",
        "Float64Vector",
        "Date32Vector",
        "IntervalVector",
        "TimestampVector",
        "TimeVector",
    }
)


def _is_null_as_boolvector(vec):
    import pyarrow.compute as _pc

    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.compiled.vector_ops.function_definitions import (
        bool_vector_all_true,
        bool_vector_from_int8_mask,
        bool_vector_from_inverted_null_bitmap,
    )

    cls_name = vec.__class__.__name__
    n = len(vec)

    def _true_for_nulls(mask):
        # Arrow compute can preserve nulls in boolean results; for IS NULL we
        # want those positions to evaluate to True, not remain nullable.
        if getattr(mask, "null_count", 0):
            return _pc.fill_null(mask, True)
        return mask

    if _is_typed_constant_encoded_vector(vec):
        if getattr(vec, "null_count", 0) == n:
            return bool_vector_all_true(n)
        return BoolVector(n)

    if _is_dictionary_encoded_vector(vec):
        if hasattr(vec, "is_null_boolvector"):
            return vec.is_null_boolvector()

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

        arrow_mask = _true_for_nulls(_pc.is_null(vec.to_arrow()))
        if _pa.types.is_floating(vec.to_arrow().type):
            arrow_mask = _true_for_nulls(_pc.or_(arrow_mask, _pc.is_nan(vec.to_arrow())))
        return _vfa(arrow_mask)

    if cls_name in _FIXED_BUFFER_VECTOR_CLASSES:
        if cls_name == "Float64Vector":
            import pyarrow.compute as _pc

            from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

            arrow_arr = vec.to_arrow()
            return _vfa(_true_for_nulls(_pc.or_(_pc.is_null(arrow_arr), _pc.is_nan(arrow_arr))))
        return bool_vector_from_int8_mask(vec.is_null(), n)

    if _is_typed_constant_encoded_vector(vec):
        if len(vec) == 0 or vec[0] is None:
            return bool_vector_all_true(n)
        return BoolVector(n)

    nb = vec.null_bitmap()
    if nb is not None:
        return bool_vector_from_inverted_null_bitmap(nb, n)
    if getattr(vec, "null_count", 0) == 0:
        return BoolVector(n)

    from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

    arrow_mask = _true_for_nulls(_pc.is_null(vec.to_arrow()))
    if cls_name == "StringVector":
        arrow_arr = vec.to_arrow()
        if _pa.types.is_string(arrow_arr.type) or _pa.types.is_binary(arrow_arr.type):
            arrow_mask = _true_for_nulls(_pc.or_(arrow_mask, _pc.is_null(arrow_arr)))
        else:
            arrow_mask = _true_for_nulls(_pc.or_(arrow_mask, _pc.is_nan(arrow_arr)))
    return _vfa(arrow_mask)
