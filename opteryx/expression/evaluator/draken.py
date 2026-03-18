"""Draken-native comparison and expression evaluation helpers."""

import datetime
import decimal

import numpy
import pyarrow as _pa

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .function_execution import _is_draken_vector
from .function_execution import apply_bounded_function

_EPOCH_DATE = datetime.date(1970, 1, 1)

_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


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

    from opteryx.draken.interop.arrow import vector_from_arrow

    arrow_arr = vec.to_arrow() if hasattr(vec, "to_arrow") else vec
    if isinstance(arrow_arr, _pa.ChunkedArray):
        if arrow_arr.num_chunks != 1:
            raise NotImplementedError(
                "Dictionary compare path does not support multi-chunk dictionary arrays."
            )
        arrow_arr = arrow_arr.chunk(0)

    return vector_from_arrow(arrow_arr)


def _coerce_str(value) -> bytes:
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


def _coerce_int64(value) -> int:
    if value.__class__.__name__ == "ConstantVector":
        value = value.scalar_value()
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
    if value.__class__.__name__ == "ConstantVector":
        value = value.scalar_value()
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
    if value.__class__.__name__ == "ConstantVector":
        value = value.scalar_value()
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
        return int(value.timestamp() * 1_000_000)
    if isinstance(value, datetime.date):
        return int(datetime.datetime(value.year, value.month, value.day).timestamp() * 1_000_000)
    if value.__class__.__name__ == "ArrowVector":
        scalar = value._arr[0].as_py()
        if scalar is None:
            return None
        return _coerce_timestamp(scalar)
    return int(value)


def _coerce_timestamp_set(values) -> frozenset:
    return frozenset(_coerce_timestamp(v) for v in values)


def _coerce_interval(value) -> tuple:
    if isinstance(value, (tuple, list)) and len(value) == 2:
        return (int(value[0]), int(value[1]))
    raise TypeError(f"Cannot coerce {type(value)!r} to interval literal")


def _coerce_temporal_scalar_for_arrow(value, target_type):
    from orso.types import OrsoTypes

    from opteryx.expression.casts import parse_timestamp_value

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

    from opteryx.compiled.vector_ops.function_definitions import bool_vector_all_true
    from opteryx.compiled.vector_ops.function_definitions import bool_vector_from_int8_mask
    from opteryx.compiled.vector_ops.function_definitions import (
        bool_vector_from_inverted_null_bitmap,
    )
    from opteryx.draken.vectors.bool_vector import BoolVector

    cls_name = vec.__class__.__name__
    n = len(vec)

    if _is_dictionary_encoded_vector(vec):
        if hasattr(vec, "is_null_boolvector"):
            return vec.is_null_boolvector()

        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

        arrow_mask = _pc.is_null(vec.to_arrow())
        if _pa.types.is_floating(vec.to_arrow().type):
            arrow_mask = _pc.or_(arrow_mask, _pc.is_nan(vec.to_arrow()))
        return _vfa(arrow_mask)

    if cls_name in _FIXED_BUFFER_VECTOR_CLASSES:
        if cls_name == "Float64Vector":
            import pyarrow.compute as _pc

            from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

            arrow_arr = vec.to_arrow()
            return _vfa(_pc.or_(_pc.is_null(arrow_arr), _pc.is_nan(arrow_arr)))
        return bool_vector_from_int8_mask(vec.is_null(), n)

    if cls_name == "ConstantVector":
        if vec.scalar_value() is None:
            return bool_vector_all_true(n)
        return BoolVector(n)

    nb = vec.null_bitmap()
    if nb is not None:
        return bool_vector_from_inverted_null_bitmap(nb, n)
    if getattr(vec, "null_count", 0) == 0:
        return BoolVector(n)

    from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

    arrow_mask = _pc.is_null(vec.to_arrow())
    if _pa.types.is_floating(vec.to_arrow().type):
        arrow_mask = _pc.or_(arrow_mask, _pc.is_nan(vec.to_arrow()))
    return _vfa(arrow_mask)


def _string_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_str_set(right)
    elif right.__class__.__name__ == "StringVector":
        raise NotImplementedError("StringVector column-column comparisons not yet supported")
    else:
        value_bytes = _coerce_str(right)

    if op == "Eq":
        return vec.equals(value_bytes)
    if op == "Lt":
        return vec.less_than(value_bytes)
    if op == "Gt":
        return vec.greater_than(value_bytes)
    if op == "LtEq":
        return vec.less_than_or_equals(value_bytes)
    if op == "GtEq":
        return vec.greater_than_or_equals(value_bytes)
    if op == "InList":
        return vec.in_list(value_set)
    if op == "Like":
        return vec.like(value_bytes, False)
    if op == "ILike":
        return vec.like(value_bytes, True)
    if op == "RLike":
        return vec.rlike(value_bytes)
    if op == "InStr":
        return vec.contains(value_bytes, False)
    if op == "IInStr":
        return vec.contains(value_bytes, True)
    raise NotImplementedError(f"StringVector: unsupported op {op!r}")


def _int64_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_int64_set(right)
    elif right.__class__.__name__ == "Int64Vector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Int64Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "Float64Vector":
        import pyarrow as pa

        from opteryx.draken.interop.arrow import vector_from_arrow

        float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
        return _float64_compare(op, float_vec, right)
    else:
        value = _coerce_int64(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vec.in_list(value_set)
    raise NotImplementedError(f"Int64Vector: unsupported op {op!r}")


def _float64_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_float_set(right)
    elif right.__class__.__name__ == "Float64Vector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Float64Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    else:
        value = _coerce_float(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vec.in_list(value_set)
    raise NotImplementedError(f"Float64Vector: unsupported op {op!r}")


def _timestamp_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_timestamp_set(right)
    elif right.__class__.__name__ == "TimestampVector":
        import pyarrow.compute as _pac

        arrow_ops = {
            "Eq": _pac.equal,
            "NotEq": _pac.not_equal,
            "Lt": _pac.less,
            "Gt": _pac.greater,
            "LtEq": _pac.less_equal,
            "GtEq": _pac.greater_equal,
        }
        fn = arrow_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"TimestampVector vector-vector: unsupported op {op!r}")
        result_arr = fn(vec.to_arrow(), right.to_arrow())
        return BoolVector.from_arrow(result_arr)
    elif right.__class__.__name__ == "Date32Vector":
        import pyarrow as _pa_local

        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

        ts_right = _vfa(right.to_arrow().cast(_pa_local.timestamp("us")))
        return _timestamp_compare(op, vec, ts_right)
    else:
        value = _coerce_timestamp(right)
        if value is None:
            return BoolVector(len(vec))

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vec.in_list(value_set)
    raise NotImplementedError(f"TimestampVector: unsupported op {op!r}")


def _date32_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_date32_set(right)
    elif right.__class__.__name__ == "Date32Vector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Date32Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "TimestampVector":
        import pyarrow as _pa_local

        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

        ts_left = _vfa(vec.to_arrow().cast(_pa_local.timestamp("us")))
        return _timestamp_compare(op, ts_left, right)
    else:
        value = _coerce_date32(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vec.in_list(value_set)
    raise NotImplementedError(f"Date32Vector: unsupported op {op!r}")


def _interval_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    literal = _coerce_interval(right)
    if op == "Eq":
        return vec.equals(literal)
    if op == "Lt":
        return vec.less_than(literal)
    if op == "Gt":
        return vec.greater_than(literal)
    if op == "LtEq":
        return vec.less_than_or_equals(literal)
    if op == "GtEq":
        return vec.greater_than_or_equals(literal)
    raise NotImplementedError(f"IntervalVector: unsupported op {op!r}")


def _dict_compare(op: str, vec, right):
    import pyarrow as pa
    import pyarrow.compute as pc

    from opteryx.draken.vectors.bool_vector import BoolVector

    vec = _dictionary_compare_vector(vec)
    if vec is None:
        raise NotImplementedError("Dictionary compare path requires a dictionary-encoded vector.")

    if right is None:
        return BoolVector(len(vec))

    if right.__class__.__name__ == "ConstantVector":
        right = right.scalar_value()
    elif right.__class__.__name__ == "ArrowVector":
        arr = right.to_arrow()
        right = arr[0].as_py() if len(arr) == 1 else arr
    elif hasattr(right, "to_arrow") and right.__class__.__name__ != "ConstantVector":
        arrow_ops = {
            "Eq": pc.equal,
            "NotEq": pc.not_equal,
            "Lt": pc.less,
            "Gt": pc.greater,
            "LtEq": pc.less_equal,
            "GtEq": pc.greater_equal,
        }
        fn = arrow_ops.get(op)
        if fn is None:
            raise NotImplementedError(
                f"dictionary-encoded vector column-to-column: unsupported op {op!r}"
            )
        left_arr = vec.to_arrow()
        right_arr = right.to_arrow()
        if pa.types.is_dictionary(left_arr.type):
            left_arr = left_arr.dictionary_decode()
        if pa.types.is_dictionary(right_arr.type):
            right_arr = right_arr.dictionary_decode()
        return BoolVector.from_arrow(fn(left_arr, right_arr))

    if isinstance(right, numpy.generic):
        right = right.item()

    value_list = list(right) if isinstance(right, (list, tuple, set, frozenset)) else right

    if isinstance(right, (datetime.datetime, datetime.date, numpy.datetime64)):
        arr = vec.to_arrow()
        if pa.types.is_dictionary(arr.type):
            arr = arr.dictionary_decode()

        if pa.types.is_date32(arr.type):
            if isinstance(right, datetime.datetime):
                arr = arr.cast(pa.timestamp("us"))
                scalar = pa.scalar(right, type=pa.timestamp("us"))
            else:
                day_value = right
                if isinstance(day_value, numpy.datetime64):
                    day_value = day_value.astype("datetime64[D]").astype(datetime.date)
                if isinstance(day_value, datetime.datetime):
                    day_value = day_value.date()
                scalar = pa.scalar(day_value, type=pa.date32())
        else:
            if isinstance(right, datetime.date) and not isinstance(right, datetime.datetime):
                right = datetime.datetime(right.year, right.month, right.day)
            if isinstance(right, numpy.datetime64):
                right = right.astype("datetime64[us]").astype(datetime.datetime)
            arr = arr if pa.types.is_timestamp(arr.type) else arr.cast(pa.timestamp("us"))
            scalar = pa.scalar(right, type=pa.timestamp("us"))

        arrow_ops = {
            "Eq": pc.equal,
            "NotEq": pc.not_equal,
            "Lt": pc.less,
            "Gt": pc.greater,
            "LtEq": pc.less_equal,
            "GtEq": pc.greater_equal,
        }
        fn = arrow_ops.get(op)
        if fn is None:
            raise NotImplementedError(
                f"dictionary-encoded vector temporal compare: unsupported op {op!r}"
            )
        return BoolVector.from_arrow(fn(arr, scalar))

    if op == "Eq":
        return vec.equals(right)
    if op == "Lt":
        return vec.less_than(right)
    if op == "Gt":
        return vec.greater_than(right)
    if op == "LtEq":
        return vec.less_than_or_equals(right)
    if op == "GtEq":
        return vec.greater_than_or_equals(right)
    if op == "InList":
        return vec.in_list(value_list)
    if op == "Like":
        return vec.like(right, False)
    if op == "ILike":
        return vec.like(right, True)
    if op == "RLike":
        return vec.rlike(right)
    if op == "InStr":
        return vec.contains(right, False)
    if op == "IInStr":
        return vec.contains(right, True)
    raise NotImplementedError(f"dictionary-encoded vector: unsupported op {op!r}")


def _constant_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector
    from opteryx.expression.ops import _coerce_in_list_values

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        right = _coerce_in_list_values(right)

    if op == "Eq":
        return vec.equals(right)
    if op == "Lt":
        return vec.less_than(right)
    if op == "Gt":
        return vec.greater_than(right)
    if op == "LtEq":
        return vec.less_than_or_equals(right)
    if op == "GtEq":
        return vec.greater_than_or_equals(right)
    if op == "InList":
        return vec.in_list(right)
    raise NotImplementedError(f"ConstantVector: unsupported op {op!r}")


_ARROW_COMPARE_OPS = {
    "Eq": "equal",
    "NotEq": "not_equal",
    "Gt": "greater",
    "GtEq": "greater_equal",
    "Lt": "less",
    "LtEq": "less_equal",
}


def _arrow_vector_compare(op: str, vec, right):
    import pyarrow as pa
    import pyarrow.compute as pc

    from opteryx.draken.vectors.bool_vector import BoolVector

    pc_op = _ARROW_COMPARE_OPS.get(op)
    if pc_op is None:
        raise NotImplementedError(f"ArrowVector: unsupported op {op!r}")
    arr = vec.to_arrow() if not isinstance(vec._arr, pa.Array) else vec._arr
    if hasattr(right, "to_arrow"):
        right = right.to_arrow()
        if isinstance(right, pa.ChunkedArray):
            right = right.combine_chunks() if right.num_chunks > 1 else right.chunk(0)
    if not isinstance(right, (pa.Array, pa.ChunkedArray)) and (
        pa.types.is_date32(arr.type)
        or pa.types.is_date64(arr.type)
        or pa.types.is_timestamp(arr.type)
    ):
        from orso.types import OrsoTypes

        target_type = OrsoTypes.TIMESTAMP if pa.types.is_timestamp(arr.type) else OrsoTypes.DATE
        scalar_value = _coerce_temporal_scalar_for_arrow(right, target_type)
        if pa.types.is_date32(arr.type) or pa.types.is_date64(arr.type):
            if isinstance(scalar_value, datetime.datetime):
                scalar_value = scalar_value.date()
            scalar = pa.scalar(scalar_value, type=arr.type)
        else:
            scalar = pa.scalar(scalar_value, type=arr.type)
        right = scalar
    bool_arr = getattr(pc, pc_op)(arr, right)
    return BoolVector.from_arrow(bool_arr)


def _ensure_array_vector(val):
    if val.__class__.__name__ == "ArrowVector":
        from opteryx.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(val.to_arrow())
    return val


def _string_anyop_like(vec, patterns, ignore_case: bool):
    pat_list = patterns if isinstance(patterns, (list, tuple)) else [patterns]
    result = None
    for p in pat_list:
        if p is None:
            continue
        pat_bytes = p if isinstance(p, bytes) else str(p).encode()
        mask = vec.like(pat_bytes, ignore_case)
        result = mask if result is None else result.or_vector(mask)
    if result is None:
        from opteryx.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(vec))
    return result


def draken_compare(op: str, left, right):
    if op == "AnyOpEq":
        from opteryx.compiled.vector_ops import vector_anyop_eq

        return vector_anyop_eq(literal=left, column=right)
    if op == "AnyOpNotEq":
        from opteryx.compiled.vector_ops import vector_anyop_neq

        return vector_anyop_neq(literal=left, column=right)
    if op == "AnyOpGt":
        from opteryx.compiled.vector_ops import vector_anyop_gt

        return vector_anyop_gt(left, right)
    if op == "AnyOpLt":
        from opteryx.compiled.vector_ops import vector_anyop_lt

        return vector_anyop_lt(left, right)
    if op == "AnyOpGtEq":
        from opteryx.compiled.vector_ops import vector_anyop_gte

        return vector_anyop_gte(left, right)
    if op == "AnyOpLtEq":
        from opteryx.compiled.vector_ops import vector_anyop_lte

        return vector_anyop_lte(left, right)
    if op == "AllOpEq":
        from opteryx.compiled.vector_ops import vector_allop_eq

        return vector_allop_eq(left, right)
    if op == "AllOpNotEq":
        from opteryx.compiled.vector_ops import vector_allop_neq

        return vector_allop_neq(left, right)
    if op == "AtArrow":
        from opteryx.compiled.vector_ops import vector_contains_any

        items = set(right) if right is not None else set()
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_any(left, items)
    if op == "ArrayContainsAll":
        from opteryx.compiled.vector_ops import vector_contains_all

        items = set(right) if right is not None else set()
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_all(left, items)
    if op == "AnyOpLike":
        from opteryx.compiled.vector_ops import vector_anyop_like
        from opteryx.draken.vectors.string_vector import StringVector

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False)
        return vector_anyop_like(right, _ensure_array_vector(left))
    if op == "AnyOpNotLike":
        from opteryx.compiled.vector_ops import vector_anyop_like
        from opteryx.draken.vectors.string_vector import StringVector

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False).not_vector()
        return vector_anyop_like(right, _ensure_array_vector(left)).not_vector()
    if op == "AnyOpILike":
        from opteryx.compiled.vector_ops import vector_anyop_ilike
        from opteryx.draken.vectors.string_vector import StringVector

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True)
        return vector_anyop_ilike(right, _ensure_array_vector(left))
    if op == "AnyOpNotILike":
        from opteryx.compiled.vector_ops import vector_anyop_ilike
        from opteryx.draken.vectors.string_vector import StringVector

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True).not_vector()
        return vector_anyop_ilike(right, _ensure_array_vector(left)).not_vector()
    if op == "AtQuestion":
        import pyarrow as pa

        from opteryx.draken.interop.arrow import vector_from_arrow
        from opteryx.third_party.tktech import csimdjson as simdjson

        docs = left.to_pylist()
        path = right
        parser = simdjson.Parser()

        if not path.startswith("$."):
            result = [None if doc is None else path in parser.parse(doc) for doc in docs]
        else:

            def _pointer(jsonpath: str) -> str:
                ptr = jsonpath[1:].replace(".", "/").replace("[", "/").replace("]", "")
                return ptr

            json_pointer = _pointer(path)

            def _check(doc):
                if doc is None:
                    return None
                try:
                    parser.parse(doc).at_pointer(json_pointer)
                    return True
                except Exception:
                    return False

            result = [_check(doc) for doc in docs]

        return vector_from_arrow(pa.array(result, type=pa.bool_()))

    negate = op in _NEGATED_OPS
    if negate:
        op = _NEGATED_OPS[op]

    if (
        isinstance(
            left,
            (
                str,
                int,
                float,
                bytes,
                bool,
                tuple,
                list,
                type(None),
                datetime.date,
                datetime.datetime,
            ),
        )
        and hasattr(right, "null_count")
        or isinstance(left, (numpy.generic, numpy.datetime64))
        and hasattr(right, "null_count")
    ):
        flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
        op = flip_ops.get(op, op)
        left, right = right, left

    if right is None and not isinstance(left, (str, int, float, bytes, bool, type(None))):
        from opteryx.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(left))

    cls = left.__class__.__name__

    if cls == "StringVector":
        result = _string_compare(op, left, right)
    elif cls == "Int64Vector" or cls == "IntegerVector":
        if cls == "IntegerVector":
            import pyarrow as pa

            from opteryx.draken.interop.arrow import vector_from_arrow

            arrow_arr = left.to_arrow().cast(pa.int64())
            left = vector_from_arrow(arrow_arr)
        result = _int64_compare(op, left, right)
    elif cls == "Float64Vector":
        result = _float64_compare(op, left, right)
    elif cls == "TimestampVector":
        result = _timestamp_compare(op, left, right)
    elif cls == "Date32Vector":
        result = _date32_compare(op, left, right)
    elif cls == "IntervalVector":
        result = _interval_compare(op, left, right)
    elif _is_dictionary_encoded_vector(left):
        result = _dict_compare(op, left, right)
    elif cls == "ConstantVector":
        result = _constant_compare(op, left, right)
    elif cls == "ArrowVector":
        result = _arrow_vector_compare(op, left, right)
    elif cls == "BoolVector":
        if op == "Eq":
            result = left.equals(bool(right))
        elif op == "NotEq":
            result = left.not_equals(bool(right))
        elif op == "InList":
            import pyarrow as _pa_local
            import pyarrow.compute as _pac

            from opteryx.draken.vectors.bool_vector import BoolVector as _BoolVec

            bool_set = {bool(v) for v in right if v is not None}
            result_arr = _pac.is_in(
                left.to_arrow(), _pa_local.array(list(bool_set), type=_pa_local.bool_())
            )
            result = _BoolVec.from_arrow(result_arr)
        else:
            import pyarrow.compute as _pac

            from opteryx.draken.vectors.bool_vector import BoolVector as _BoolVec

            bool_arrow_ops = {
                "Lt": _pac.less,
                "Gt": _pac.greater,
                "LtEq": _pac.less_equal,
                "GtEq": _pac.greater_equal,
            }
            fn = bool_arrow_ops.get(op)
            if fn is None:
                raise NotImplementedError(f"BoolVector: unsupported op {op!r}")
            result_arr = fn(left.to_arrow(), bool(right))
            result = _BoolVec.from_arrow(result_arr)
    else:
        raise NotImplementedError(f"draken_compare: unsupported vector type {cls!r}")

    return result.not_vector() if negate else result


_DATE_TYPES = frozenset(("Date32Vector", "TimestampVector"))
_INTERVAL_TYPES = frozenset(("IntervalVector",))


def _date_minus_date_draken(left_vec, right_vec):
    import pyarrow as pa
    import pyarrow.compute as pc

    from opteryx.draken.interop.arrow import vector_from_arrow
    from opteryx.expression.intervals import MICROSECONDS_PER_DAY
    from opteryx.expression.intervals import _intervals_to_month_day_nano

    left_arr = left_vec.to_arrow()
    right_arr = right_vec.to_arrow()

    if pa.types.is_date32(left_arr.type):
        left_us = pc.multiply(
            left_arr.cast(pa.int32()).cast(pa.int64()), pa.scalar(MICROSECONDS_PER_DAY, pa.int64())
        )
    else:
        left_us = left_arr.cast(pa.timestamp("us")).cast(pa.int64())

    if pa.types.is_date32(right_arr.type):
        right_us = pc.multiply(
            right_arr.cast(pa.int32()).cast(pa.int64()), pa.scalar(MICROSECONDS_PER_DAY, pa.int64())
        )
    else:
        right_us = right_arr.cast(pa.timestamp("us")).cast(pa.int64())

    diff_us = pc.subtract(left_us, right_us)

    rows = [None if not d.is_valid else (0, d.as_py()) for d in diff_us]
    return vector_from_arrow(_intervals_to_month_day_nano(rows))


def _date_interval_op_draken(left_vec, right_vec, op):
    from opteryx.expression.intervals import _as_interval_vector

    signum = 1 if op == "Plus" else -1

    if right_vec.__class__.__name__ in _INTERVAL_TYPES:
        date_vec, interval_vec = left_vec, _as_interval_vector(right_vec)
    else:
        date_vec, interval_vec = right_vec, _as_interval_vector(left_vec)

    return interval_vec.apply_to_temporal(date_vec, signum)


def _eval_binary_op_draken(node, morsel):
    op = node.value
    left = _eval_value(node.left, morsel)
    right = _eval_value(node.right, morsel)

    from orso.types import OrsoTypes

    if not hasattr(left, "null_count") and node.left.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.draken.interop.arrow import vector_from_arrow

        arrow_type = (
            _pa.date32() if node.left.schema_column.type == OrsoTypes.DATE else _pa.timestamp("us")
        )
        scalar = _coerce_temporal_scalar_for_arrow(left, node.left.schema_column.type)
        left = vector_from_arrow(_pa.array([scalar] * morsel.num_rows, type=arrow_type))
    if not hasattr(right, "null_count") and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.draken.interop.arrow import vector_from_arrow

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

    from opteryx.draken.interop.arrow import vector_from_arrow
    from opteryx.draken.interop.arrow import vector_from_sequence
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


def _eval_value(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.LITERAL:
        return node.value

    if node_type == NodeType.IDENTIFIER:
        vec = morsel.column(node.schema_column.identity.encode())
        if vec.__class__.__name__ == "ArrowVector":
            from opteryx.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(vec.to_arrow())
        return vec

    if node_type in (NodeType.EVALUATED, NodeType.AGGREGATOR):
        try:
            vec = morsel.column(node.schema_column.identity.encode())
        except KeyError:
            raise ColumnReferencedBeforeEvaluationError(column=node.schema_column.name)
        if vec.__class__.__name__ == "ArrowVector":
            from opteryx.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(vec.to_arrow())
        return vec

    if node_type == NodeType.NESTED:
        return _eval_value(node.centre, morsel)

    if node_type == NodeType.EXPRESSION_LIST:
        return [_eval_value(parameter, morsel) for parameter in node.parameters]

    if node_type == NodeType.EXTRACTION_OPERATOR:
        left_vec = _eval_value(node.left, morsel)
        right_val = node.right.value
        op = node.value

        if op == "MapAccess":
            from opteryx.draken.interop.arrow import vector_from_arrow
            from opteryx.draken.interop.arrow import vector_from_sequence
            from opteryx.expression.binary_operators import MapAccessOp

            source = left_vec.to_arrow() if hasattr(left_vec, "to_arrow") else left_vec
            result = MapAccessOp(source, [right_val])
            if isinstance(result, _pa.Array):
                return vector_from_arrow(result)
            return vector_from_sequence(result)

        if op in ("Arrow", "LongArrow"):
            from opteryx.draken.interop.arrow import vector_from_arrow
            from opteryx.expression.binary_operators import ArrowOp
            from opteryx.expression.binary_operators import LongArrowOp

            docs = left_vec.to_pylist()
            result = ArrowOp(docs, [right_val]) if op == "Arrow" else LongArrowOp(docs, [right_val])
            return vector_from_arrow(result)

        raise NotImplementedError(
            f"_eval_value: EXTRACTION_OPERATOR {op!r} not supported in Draken path"
        )

    from opteryx.expression import NodeType as _NT

    if node.node_type == _NT.BINARY_OPERATOR:
        result = _eval_binary_op_draken(node, morsel)
        if result is not None:
            return result

    if node.node_type in (_NT.BINARY_OPERATOR, _NT.CAST, _NT.FUNCTION):
        identity = getattr(getattr(node, "schema_column", None), "identity", None)
        if identity is not None:
            try:
                vec = morsel.column(identity if isinstance(identity, bytes) else identity.encode())
            except KeyError:
                vec = None
            if vec is not None:
                if vec.__class__.__name__ == "ArrowVector":
                    from opteryx.draken.interop.arrow import vector_from_arrow

                    return vector_from_arrow(vec.to_arrow())
                return vec

        from opteryx.draken.interop.arrow import vector_from_arrow
        from opteryx.draken.interop.arrow import vector_from_sequence
        from opteryx.expression import _inner_evaluate

        arrow_table = morsel.to_arrow()
        result = _inner_evaluate(node, arrow_table)
        if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
            return vector_from_arrow(result)
        if result is not None and result.__class__.__name__.endswith("Vector"):
            return result
        if not hasattr(result, "__iter__") or isinstance(result, (str, bytes, numpy.generic)):
            from opteryx.draken.vectors.constant_vector import from_scalar as _const_scalar

            vec = _const_scalar(result, morsel.num_rows)
            if vec is not None:
                return vec
            return vector_from_arrow(_pa.array([result] * morsel.num_rows))
        return vector_from_sequence(result)

    return evaluate_draken(node, morsel)


def _unary_draken(op: str, centre_node, morsel):
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        return _is_null_as_boolvector(vec).not_vector()
    if op in ("IsTrue", "IsNotFalse", "IsFalse", "IsNotTrue"):
        bv = vec if vec.__class__.__name__ == "BoolVector" else None
        if bv is None:
            raise TypeError(
                f"IS TRUE/IS FALSE requires a boolean expression; got {vec.__class__.__name__!r}"
            )
        if op == "IsTrue":
            return bv.equals(True)
        if op == "IsNotFalse":
            return bv.not_equals(False)
        if op == "IsFalse":
            return bv.equals(False)
        if op == "IsNotTrue":
            return bv.not_equals(True)
    raise NotImplementedError(f"evaluate_draken: unsupported unary op {op!r}")


def evaluate_draken(node, morsel):
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.NESTED:
        return evaluate_draken(node.centre, morsel)

    if node_type == NodeType.AND:
        left = evaluate_draken(node.left, morsel)
        if not left.any():
            return left
        right = evaluate_draken(node.right, morsel)
        return left.and_vector(right)

    if node_type == NodeType.OR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.or_vector(right)

    if node_type == NodeType.NOT:
        return evaluate_draken(node.centre, morsel).not_vector()

    if node_type == NodeType.XOR:
        left = evaluate_draken(node.left, morsel)
        right = evaluate_draken(node.right, morsel)
        return left.xor_vector(right)

    if node_type == NodeType.DNF:
        result = evaluate_draken(node.parameters[0], morsel)
        for sub in node.parameters[1:]:
            if not result.any():
                return result
            result = result.and_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NodeType.LITERAL:
        import pyarrow as pa

        from opteryx.draken.vectors.bool_vector import BoolVector

        val = node.value
        scalar = bool(val) if val is not None else False
        return BoolVector.from_arrow(pa.array([scalar] * morsel.num_rows, type=pa.bool_()))

    if node_type == NodeType.COMPARISON_OPERATOR:
        left = _eval_value(node.left, morsel)
        right = _eval_value(node.right, morsel)
        from orso.types import OrsoTypes

        temporal_types = {OrsoTypes.DATE, OrsoTypes.TIMESTAMP}
        left_schema_type = getattr(getattr(node.left, "schema_column", None), "type", None)
        right_schema_type = getattr(getattr(node.right, "schema_column", None), "type", None)
        if left_schema_type in temporal_types or right_schema_type in temporal_types:
            if not hasattr(left, "null_count") and left_schema_type in temporal_types:
                left = _coerce_temporal_scalar_for_arrow(left, left_schema_type)
            if not hasattr(right, "null_count") and right_schema_type in temporal_types:
                right = _coerce_temporal_scalar_for_arrow(right, right_schema_type)

        if not hasattr(left, "null_count") and not hasattr(right, "null_count"):
            import pyarrow as pa

            from opteryx.draken.vectors.bool_vector import BoolVector
            from opteryx.expression.ops import filter_operations

            scalar_result = filter_operations(
                pa.array([left]),
                left_schema_type,
                node.value,
                pa.array([right]),
                right_schema_type,
            )[0].as_py()
            scalar_result = False if scalar_result is None else bool(scalar_result)
            return BoolVector.from_arrow(
                pa.array([scalar_result] * morsel.num_rows, type=pa.bool_())
            )
        return draken_compare(node.value, left, right)

    if node_type == NodeType.UNARY_OPERATOR:
        return _unary_draken(node.value, node.centre, morsel)

    if node_type == NodeType.FUNCTION:
        if node.value == "_PASSTHRU":
            return evaluate_draken(node.parameters[0], morsel)
        parameters = [_eval_value(param, morsel) for param in node.parameters]
        if len(parameters) == 0:
            parameters = [morsel.num_rows]
        result = apply_bounded_function(node, *parameters)
        if isinstance(result, list):
            result = numpy.array(result, dtype=object)
        if isinstance(result, numpy.ndarray):
            if result.ndim != 1:
                raise TypeError(
                    f"evaluate_draken: FUNCTION node returned ndarray with rank {result.ndim}, expected 1-dimensional boolean results"
                )
            if result.dtype.kind in ("b", "O", "f", "i", "u"):
                import pyarrow as pa

                from opteryx.draken.vectors.bool_vector import BoolVector

                try:
                    result = BoolVector.from_arrow(pa.array(result, type=pa.bool_()))
                except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError):
                    pass
        elif isinstance(result, _pa.Array) and _pa.types.is_boolean(result.type):
            from opteryx.draken.vectors.bool_vector import BoolVector

            result = BoolVector.from_arrow(result)
        if result.__class__.__name__ != "BoolVector":
            raise TypeError(
                f"evaluate_draken: FUNCTION node returned {result.__class__.__name__!r}, expected BoolVector"
            )
        return result

    if node_type == NodeType.BINARY_OPERATOR:
        from opteryx.draken.vectors.bool_vector import BoolVector

        result = _eval_value(node, morsel)
        if result.__class__.__name__ == "BoolVector":
            return result
        if isinstance(result, _pa.Array) and _pa.types.is_boolean(result.type):
            return BoolVector.from_arrow(result)
        if isinstance(result, numpy.ndarray) and result.dtype.kind == "b":
            return BoolVector.from_arrow(_pa.array(result, type=_pa.bool_()))
        raise TypeError(
            f"evaluate_draken: BINARY_OPERATOR '{node.value!r}' returned non-boolean {result.__class__.__name__!r}"
        )

    raise NotImplementedError(
        f"evaluate_draken: unsupported node type {node_type!r} (value={node.value!r})"
    )


def evaluate_and_append_draken(nodes, morsel):
    from opteryx.draken.interop.arrow import vector_from_sequence
    from opteryx.draken.morsels.morsel import Morsel
    from opteryx.expression import NodeType

    col_names = list(morsel.column_names)
    col_vecs = [morsel.column(n if isinstance(n, bytes) else n.encode()) for n in col_names]
    existing = {n.decode() if isinstance(n, bytes) else n for n in col_names}

    for node in nodes:
        if node.value == "_PASSTHRU":
            continue
        identity = node.schema_column.identity
        if identity in existing:
            continue
        if node.node_type == NodeType.FUNCTION:
            from opteryx.expression import NodeType as _NT
            from opteryx.expression import _inner_evaluate

            arrow_table = None
            parameters = []
            for param in node.parameters:
                if param.node_type == _NT.LITERAL:
                    if arrow_table is None:
                        arrow_table = morsel.to_arrow()
                    parameters.append(_inner_evaluate(param, arrow_table))
                else:
                    parameters.append(_eval_value(param, morsel))
            if len(parameters) == 0:
                parameters = [morsel.num_rows]
            result = apply_bounded_function(node, *parameters)
        else:
            result = _eval_value(node, morsel)
        if not _is_draken_vector(result):
            import pyarrow as _pa_local

            from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

            if isinstance(result, (_pa_local.Array, _pa_local.ChunkedArray)):
                result = _vfa(result)
            elif not hasattr(result, "__iter__") or isinstance(result, (str, bytes)):
                from opteryx.draken.vectors.constant_vector import from_scalar as _const_scalar

                vec = _const_scalar(result, morsel.num_rows)
                result = _vfa(_pa_local.array([result] * morsel.num_rows)) if vec is None else vec
            else:
                result = vector_from_sequence(result)
        col_names.append(identity)
        col_vecs.append(result)
        existing.add(identity)

    return Morsel.from_vectors(col_names, col_vecs)


__all__ = ["draken_compare", "evaluate_and_append_draken", "evaluate_draken"]
