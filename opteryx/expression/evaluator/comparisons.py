"""Draken comparison operations."""

import datetime

import numpy
import pyarrow as _pa

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
    vector_contains,
    vector_in_list,
)

from .function_execution import _is_draken_vector, apply_bounded_function
from .string_ops import _string_compare
from .type_coercion import (
    _coerce_date32,
    _coerce_date32_set,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_interval,
    _coerce_str,
    _coerce_str_set,
    _coerce_temporal_scalar_for_arrow,
    _coerce_timestamp,
    _coerce_timestamp_set,
    _constant_scalar_value,
    _dictionary_arrow_type,
    _dictionary_compare_vector,
    _is_constant_vector_like,
    _is_dictionary_encoded_vector,
    _is_null_as_boolvector,
    _is_typed_constant_encoded_vector,
)

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


def _int64_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

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
        return vector_in_list(vec,value_set)

    # Fallback for edge cases like Float64Vector comparison (not in hot path for ClickBench)
    if right.__class__.__name__ == "Float64Vector":
        import pyarrow as pa

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
        return _float64_compare(op, float_vec, right)

    raise NotImplementedError(f"Int64Vector: unsupported op {op!r}")


def _float64_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

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
        return vector_in_list(vec,value_set)
    raise NotImplementedError(f"Float64Vector: unsupported op {op!r}")


def _dict_compare(op: str, vec, right):
    import pyarrow as pa
    import pyarrow.compute as pc

    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    vec = _dictionary_compare_vector(vec)
    if vec is None:
        raise NotImplementedError("Dictionary compare path requires a dictionary-encoded vector.")

    if right is None:
        return BoolVector(len(vec))

    if _is_constant_vector_like(right):
        right = _constant_scalar_value(right)
    elif right.__class__.__name__ == "ArrowVector":
        arr = right.to_arrow()
        right = arr[0].as_py() if len(arr) == 1 else arr
    elif hasattr(right, "to_arrow") and not _is_constant_vector_like(right):
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
        return vector_in_list(vec,value_list)
    if op in ("Like", "ILike", "RLike", "InStr", "IInStr"):
        right = _coerce_str(right)
    if op == "Like":
        return vector_like(vec,right, False)
    if op == "ILike":
        return vector_like(vec,right, True)
    if op == "RLike":
        return vector_rlike(vec,right)
    if op == "InStr":
        return vector_contains(vec,right, False)
    if op == "IInStr":
        return vector_contains(vec,right, True)
    raise NotImplementedError(f"dictionary-encoded vector: unsupported op {op!r}")


def _constant_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values

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
        return vector_in_list(vec,right)
    raise NotImplementedError(f"constant-encoded vector: unsupported op {op!r}")


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

    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

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


def draken_compare(op: str, left, right, left_schema_type=None, right_schema_type=None):
    from orso.types import OrsoTypes

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
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False)
        return vector_anyop_like(right, _ensure_array_vector(left))
    if op == "AnyOpNotLike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False).not_vector()
        return vector_anyop_like(right, _ensure_array_vector(left)).not_vector()
    if op == "AnyOpILike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True)
        return vector_anyop_ilike(right, _ensure_array_vector(left))
    if op == "AnyOpNotILike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True).not_vector()
        return vector_anyop_ilike(right, _ensure_array_vector(left)).not_vector()
    if op == "AtQuestion":
        import pyarrow as pa

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        from opteryx.third_party.tktech import csimdjson as simdjson

        docs = left.to_pylist()
        path = right
        parser = simdjson.Parser()

        if path.startswith("$."):
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
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(left))

    cls = left.__class__.__name__

    if cls == "StringVector":
        result = _string_compare(op, left, right)
    elif cls == "Int64Vector" or cls == "IntegerVector":
        if left_schema_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
            result = _int64_temporal_compare(op, left, right, left_schema_type)
        else:
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
    elif _is_typed_constant_encoded_vector(left):
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

            from opteryx.compiled.draken.vectors.bool_vector import BoolVector as _BoolVec

            bool_set = {bool(v) for v in right if v is not None}
            result_arr = _pac.is_in(
                left.to_arrow(), _pa_local.array(list(bool_set), type=_pa_local.bool_())
            )
            result = _BoolVec.from_arrow(result_arr)
        else:
            import pyarrow.compute as _pac

            from opteryx.compiled.draken.vectors.bool_vector import BoolVector as _BoolVec

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
