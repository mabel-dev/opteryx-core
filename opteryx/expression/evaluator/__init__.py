"""Expression evaluator: hotpath for function execution.

The evaluator executes bound function expressions with minimal dispatch overhead.
The binder attaches a ResolvedFunction reference (node.function_ref) to each FUNCTION node;
the evaluator uses that to dispatch directly to the kernel, bypassing all name-based lookup.

Phase 1 additions: draken_compare, evaluate_draken, evaluate_and_append_draken.
These operate on Draken Morsel/Vector types without Arrow conversion.
"""

import datetime
import decimal
from typing import Any

import numpy
import pyarrow as _pa
import pyarrow.compute as compute

from opteryx.exceptions import FunctionExecutionError

_DRAKEN_VECTOR_NAMES = frozenset(
    {
        "StringVector",
        "Int64Vector",
        "IntegerVector",
        "Float64Vector",
        "TimestampVector",
        "Date32Vector",
        "TimeVector",
        "BoolVector",
        "DictionaryVector",
        "ConstantVector",
        "ArrayVector",
        "VectorVector",
        "ArrowVector",
        "IntervalVector",
    }
)


def _coerce_param_for_kernel(p, pa):
    """Convert a Draken vector to a PyArrow array for kernel dispatch.

    Python wrapper functions in implementations/ already accept StringVector.
    PyArrow compute functions (compute.sqrt, compute.utf8_reverse, …) do not.
    Converting all Draken vectors to Arrow is safe for both cases.
    """
    if p.__class__.__name__ not in _DRAKEN_VECTOR_NAMES:
        return p
    arr = p.to_arrow()
    # Decode dictionary-encoded arrays before any other type checks
    if pa.types.is_dictionary(arr.type):
        arr = arr.cast(arr.type.value_type)
    # StringVector (and binary-valued dicts) store UTF-8 bytes as Arrow binary;
    # cast to utf8 for string kernels like utf8_reverse, utf8_title, etc.
    if pa.types.is_binary(arr.type) or pa.types.is_large_binary(arr.type):
        arr = arr.cast(pa.utf8())
    return arr


def _coerce_param_for_draken(p):
    """Coerce inputs into native Draken vectors for draken kernels.

    Draken kernels should only receive Draken vectors (including ConstantVector).
    This function mirrors the intent of KernelSpec.engine="draken" by
    coercing scalars and lists into vector form before the kernel is invoked.

    This avoids letting non-Draken values (lists, numpy scalars, pyarrow scalars)
    reach the kernel and trigger type errors.
    """
    # Fast path: already a Draken vector.
    if p.__class__.__name__ in _DRAKEN_VECTOR_NAMES:
        return p

    # Arrow arrays → Draken vectors (existing behavior)
    if isinstance(p, (_pa.Array, _pa.ChunkedArray)):
        from opteryx.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(p)

    # PyArrow scalar → primitive → constant vector
    if hasattr(p, "as_py") and not isinstance(p, (bytes, str)):
        try:
            p = p.as_py()
        except Exception:
            pass

    # Numpy scalars / arrays
    try:
        import numpy as np

        if isinstance(p, np.generic):
            p = p.item()
        elif isinstance(p, np.ndarray):
            if p.ndim == 0:
                p = p.item()
            else:
                p = p.tolist()
    except Exception:
        pass

    # Lists/tuples → vector_from_sequence
    if isinstance(p, (list, tuple)):
        from opteryx.draken.interop.arrow import vector_from_sequence

        try:
            return vector_from_sequence(p)
        except Exception as e:
            raise FunctionExecutionError(
                message=(
                    "Failed to coerce list/tuple to Draken vector for draken kernel. "
                    f"Inner error: {e}"
                ),
                function=None,
            )

    # Scalars → ConstantVector (length 1, broadcastable)
    from opteryx.draken.vectors.constant_vector import from_scalar as _const_scalar

    if isinstance(p, (bool, int, float, str, bytes, type(None))):
        vec = _const_scalar(p, 1)
        if vec is not None:
            return vec

    # Fall back to returning as-is, so downstream type errors are visible.
    return p


def _normalize_null_policy(null_policy: str) -> str:
    """Normalize old null_policy labels to their new semantic equivalents."""
    if null_policy == "strict":
        return "compress"
    if null_policy == "custom":
        return "bypass"
    if null_policy == "passthrough":
        return "passthru"
    return null_policy


def apply_bounded_function(node, *parameters) -> Any:
    """Apply a bound FUNCTION node to its already-evaluated parameters.

    Uses node.function_ref (set by binder) for kernel dispatch and null policy.

    Null policy (kernel.null_policy):
        "compress"    — strip null rows before calling the kernel and fill them back after.
                        Fast path for functions that return NULL on any NULL input.
        "passthru"    — pass all rows including nulls; the kernel handles nulls itself.
                        Required for COALESCE, CASE, IFNULL, CONCAT, SUBSTRING, etc.
        "bypass"      — do not perform any null-special handling; the kernel is
                        responsible for all null handling.
        (legacy values "strict" and "custom" are still accepted and mapped.)
    """
    func_ref = node.function_ref
    if func_ref is None:
        raise FunctionExecutionError(
            message=f"Function '{node.value}' was not bound — function_ref is None.",
            function=node.value,
        )

    kernel = func_ref.selected_overload.kernel
    engine = kernel.engine

    if engine is None:
        raise FunctionExecutionError(
            message=("KernelSpec.engine is required; please specify one of: 'arrow', 'draken'."),
            function=node.value,
        )

    null_policy = _normalize_null_policy(kernel.null_policy)

    compressed = False
    if (
        null_policy == "compress"
        and len(parameters) > 0
        and not isinstance(parameters[0], int)
        and all(isinstance(arr, numpy.ndarray) for arr in parameters)
        and all(arr.ndim == 1 for arr in parameters)
    ):
        morsel_size = len(parameters[0])
        null_positions = numpy.zeros(morsel_size, dtype=numpy.bool_)

        for arr in parameters:
            if arr.dtype.kind == "f":
                null_positions = numpy.logical_or(
                    null_positions, compute.is_null(arr, nan_is_null=True)
                )
            else:
                null_positions = numpy.logical_or(null_positions, compute.is_null(arr))

        if null_positions.all():
            return numpy.full(morsel_size, None, dtype=object)

        if null_positions.any():
            valid_positions = ~null_positions
            parameters = [arr.compress(valid_positions) for arr in parameters]
            compressed = True

    # Convert inputs based on engine. In the draken path we must ensure the
    # kernel always sees Draken vectors (including ConstantVector) so it can
    # execute at native speed without handling Python types.
    if engine == "arrow":
        # Arrow kernels expect arrow arrays, so coerce any known Draken vectors.
        import pyarrow as _pa_abf

        parameters = tuple(_coerce_param_for_kernel(p, _pa_abf) for p in parameters)
    elif engine == "draken":
        # Draken kernels expect native Draken vectors. Convert everything we can.
        parameters = tuple(_coerce_param_for_draken(p) for p in parameters)
        # ArrowVector is a fallback wrapper for types without native Draken support
        # (e.g. decimal128, float32). Detect these before the kernel call so we can
        # report the SQL type name rather than an opaque Cython crash.
        for p in parameters:
            if p.__class__.__name__ == "ArrowVector":
                arr = getattr(p, "_arr", None)
                type_name = str(arr.type) if arr is not None else "unknown"
                raise FunctionExecutionError(
                    message=(
                        f"Function '{node.value}' does not natively support column type "
                        f"'{type_name}'. Consider casting the column: "
                        f"CAST(column AS DOUBLE)."
                    ),
                    function=node.value,
                )
    elif engine == "python":
        # No conversion needed — kernel expects native Python objects.
        pass
    else:
        raise FunctionExecutionError(
            message=(
                f"Unknown kernel engine '{engine}' for function '{node.value}'. "
                "Expected one of: 'arrow', 'draken', 'python'."
            ),
            function=node.value,
        )

    try:
        result = kernel.callable_ref(*parameters)
    except FunctionExecutionError as e:
        raise e
    except Exception as e:
        raise FunctionExecutionError(message=e, function=node.value) from e

    if isinstance(result, list):
        result = numpy.array(result)

    if compressed:
        out = numpy.full(morsel_size, None, dtype=object)
        numpy.place(out, valid_positions, result)
        return out

    return result


# ---------------------------------------------------------------------------
# Phase 1 — Draken-native expression evaluator
# ---------------------------------------------------------------------------

_EPOCH_DATE = datetime.date(1970, 1, 1)

# Negation map: operator → positive counterpart applied before .not_vector()
_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


# --- Type coercions (at dispatch time, not in kernels) ---


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


# --- IS NULL helper ---

# Vector class names whose is_null() returns int8_t[::1] (1=null, 0=valid).
# These are all fixed-buffer types backed by DrakenFixedBuffer.
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
    """Return BoolVector where True = SQL NULL position.

    Dispatch order:
      1. DictionaryVector  → native is_null_boolvector() (handles NaN-encoded nulls)
      2. Fixed-buffer types (Int64, Float64, Date32, Timestamp, Time, Interval,
         Bool) → is_null() returns int8_t[::1]; packed to BoolVector via Cython
      3. ConstantVector    → O(1) scalar_value() check; all-true or all-false
      4. StringVector / ArrayVector → null_bitmap() memoryview (inverted) path
      5. ArrowVector and other unrecognized types → Arrow round-trip via pc.is_null
    """
    from opteryx.compiled.vector_ops.function_definitions import bool_vector_all_true
    from opteryx.compiled.vector_ops.function_definitions import bool_vector_from_int8_mask
    from opteryx.compiled.vector_ops.function_definitions import (
        bool_vector_from_inverted_null_bitmap,
    )
    from opteryx.draken.vectors.bool_vector import BoolVector

    cls_name = vec.__class__.__name__
    n = len(vec)

    if cls_name == "DictionaryVector":
        return vec.is_null_boolvector()

    if cls_name in _FIXED_BUFFER_VECTOR_CLASSES:
        return bool_vector_from_int8_mask(vec.is_null(), n)

    if cls_name == "ConstantVector":
        if vec.scalar_value() is None:
            return bool_vector_all_true(n)
        return BoolVector(n)  # all-false: non-null constant has no nulls

    # StringVector and ArrayVector expose null information via null_bitmap()
    nb = vec.null_bitmap()
    if nb is not None:
        return bool_vector_from_inverted_null_bitmap(nb, n)
    # null_bitmap() == None can mean no nulls OR that the type doesn't implement it
    # For types that don't override null_bitmap() (e.g. ArrowVector, StringVector
    # when all rows are valid), check null_count first to avoid Arrow round-trip
    if getattr(vec, "null_count", 0) == 0:
        return BoolVector(n)  # confirmed no nulls

    # Arrow fallback: for ArrowVector and any other unrecognized type that wraps
    # a PyArrow array with nulls but doesn't expose a native null bitmap
    import pyarrow.compute as _pc

    from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

    return _vfa(_pc.is_null(vec.to_arrow()))


# --- Per-type comparison dispatchers ---


def _string_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = frozenset(int(v) for v in right)
    elif right.__class__.__name__ == "Int64Vector":
        _VEC_OPS_INT64 = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = _VEC_OPS_INT64.get(op)
        if fn is None:
            raise NotImplementedError(f"Int64Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "Float64Vector":
        # Promote Int64Vector to Float64Vector for mixed-precision comparison
        import pyarrow as pa

        from opteryx.draken.interop.arrow import vector_from_arrow

        float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
        return _float64_compare(op, float_vec, right)
    else:
        value = int(right)

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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_float_set(right)
    elif right.__class__.__name__ == "Float64Vector":
        _VEC_OPS = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = _VEC_OPS.get(op)
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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_timestamp_set(right)
    elif right.__class__.__name__ == "TimestampVector":
        # TimestampVector has no vector-vs-vector compare methods; use Arrow compute.
        import pyarrow as _pa
        import pyarrow.compute as _pac

        _ARROW_OPS = {
            "Eq": _pac.equal,
            "NotEq": _pac.not_equal,
            "Lt": _pac.less,
            "Gt": _pac.greater,
            "LtEq": _pac.less_equal,
            "GtEq": _pac.greater_equal,
        }
        fn = _ARROW_OPS.get(op)
        if fn is None:
            raise NotImplementedError(f"TimestampVector vector-vector: unsupported op {op!r}")
        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa
        from opteryx.draken.vectors.bool_vector import BoolVector

        result_arr = fn(vec.to_arrow(), right.to_arrow())
        return BoolVector.from_arrow(result_arr)
    elif right.__class__.__name__ == "Date32Vector":
        # Cross-type: upcast the Date32Vector to Timestamp and delegate.
        import pyarrow as _pa

        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

        ts_right = _vfa(right.to_arrow().cast(_pa.timestamp("us")))
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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_date32_set(right)
    elif right.__class__.__name__ == "Date32Vector":
        _VEC_OPS = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = _VEC_OPS.get(op)
        if fn is None:
            raise NotImplementedError(f"Date32Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "TimestampVector":
        # Cross-type: upcast the Date32Vector to Timestamp and delegate.
        import pyarrow as _pa

        from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

        ts_left = _vfa(vec.to_arrow().cast(_pa.timestamp("us")))
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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
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

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
    if right is None:
        return BoolVector(len(vec))

    if right.__class__.__name__ == "ConstantVector":
        right = right.scalar_value()
    elif right.__class__.__name__ == "ArrowVector":
        arr = right.to_arrow()
        right = arr[0].as_py() if len(arr) == 1 else arr

    if isinstance(right, numpy.generic):
        right = right.item()

    # keep scalar as-is for non-list ops
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

        _ARROW_OPS = {
            "Eq": pc.equal,
            "NotEq": pc.not_equal,
            "Lt": pc.less,
            "Gt": pc.greater,
            "LtEq": pc.less_equal,
            "GtEq": pc.greater_equal,
        }
        fn = _ARROW_OPS.get(op)
        if fn is None:
            raise NotImplementedError(f"DictionaryVector temporal compare: unsupported op {op!r}")
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
    raise NotImplementedError(f"DictionaryVector: unsupported op {op!r}")


def _constant_compare(op: str, vec, right):
    from opteryx.draken.vectors.bool_vector import BoolVector
    from opteryx.expression.ops import _coerce_in_list_values

    # SQL NULL semantics: comparing anything with NULL returns NULL (treated as FALSE in WHERE)
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
    """Fallback comparison for ArrowVector (unrecognized Arrow types like decimal128).

    Delegates to ``pyarrow.compute`` and returns a BoolVector.  There is no coercion
    of the scalar *right* here — PyArrow handles the cast implicitly.
    """
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
    """Ensure val is an ArrayVector, converting from ArrowVector if needed."""
    if val.__class__.__name__ == "ArrowVector":
        from opteryx.draken.interop.arrow import vector_from_arrow

        return vector_from_arrow(val.to_arrow())
    return val


def _string_anyop_like(vec, patterns, ignore_case: bool):
    """LIKE ANY / ILIKE ANY for a StringVector: OR of per-pattern results."""
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
    """Dispatch a scalar or vector comparison to the appropriate Draken vector method.

    Args:
        op:    Opteryx operator string ("Eq", "Gt", "InList", "Like", etc.)
        left:  A Draken vector (the column being filtered), or a scalar for AnyOp/AllOp
        right: A Python scalar, Python collection (InList), or another Draken vector

    Returns:
        BoolVector with SQL three-valued-logic null semantics.
    """
    # --- AnyOp / AllOp: left is the scalar literal, right is the ArrayVector column ---
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

    # --- AtArrow / ArrayContainsAll: left is the ArrayVector column, right is a literal list ---
    if op == "AtArrow":
        from opteryx.compiled.vector_ops import vector_contains_any

        items = set(right) if right is not None else set()
        # ArrayVector stores strings as bytes; coerce str literals to bytes
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_any(left, items)
    if op == "ArrayContainsAll":
        from opteryx.compiled.vector_ops import vector_contains_all

        items = set(right) if right is not None else set()
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_all(left, items)

    # --- AnyOp LIKE: left is the column (ArrayVector or StringVector), right is pattern list ---
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

    # --- AtQuestion: left is StringVector (JSON docs), right is the literal path ---
    if op == "AtQuestion":
        import pyarrow as pa

        from opteryx.draken.interop.arrow import vector_from_arrow
        from opteryx.third_party.tktech import csimdjson as simdjson

        docs = left.to_pylist()  # list of bytes or None
        path = right  # JSON path string (e.g. "key" or "$.key.sub")
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

    # Normalize: if left is a Python scalar and right is a Draken vector, swap them
    # and invert the directional operator so semantics remain correct.
    # e.g. 'Earth' = g.name  →  g.name = 'Earth' (Eq is symmetric, no flip needed)
    # e.g. 5 > g.id          →  g.id < 5           (Gt → Lt)
    if isinstance(left, (str, int, float, bytes, bool, tuple, list, type(None))) and hasattr(
        right, "null_count"
    ):
        _FLIP_OPS = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
        op = _FLIP_OPS.get(op, op)
        left, right = right, left
    elif isinstance(left, numpy.generic) and hasattr(right, "null_count"):
        # numpy scalar (numpy.int64, numpy.datetime64, etc.) on left, vector on right
        _FLIP_OPS = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
        op = _FLIP_OPS.get(op, op)
        left, right = right, left

    # SQL three-valued logic: comparing anything with NULL always yields NULL
    # (treated as FALSE in WHERE).  This must be checked AFTER the scalar/vector
    # flip so that "null = col" is also caught.  Crucially it must apply before
    # we could ever negate the result (NOT(NULL) = NULL, not TRUE).
    if right is None and not isinstance(left, (str, int, float, bytes, bool, type(None))):
        from opteryx.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(left))

    cls = left.__class__.__name__

    if cls == "StringVector":
        result = _string_compare(op, left, right)
    elif cls == "Int64Vector" or cls == "IntegerVector":
        # ``IntegerVector`` is the fixed-width integer type for int8/int16/int32
        # widths; it does not implement the same scalar comparison methods that
        # _int64_compare expects.  Promote to a true 64-bit integer vector by
        # casting through Arrow so the kernels are available.
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
    elif cls == "DictionaryVector":
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
            import pyarrow as _pa
            import pyarrow.compute as _pac

            from opteryx.draken.vectors.bool_vector import BoolVector as _BoolVec

            bool_set = {bool(v) for v in right if v is not None}
            result_arr = _pac.is_in(left.to_arrow(), _pa.array(list(bool_set), type=_pa.bool_()))
            result = _BoolVec.from_arrow(result_arr)
        else:
            import pyarrow.compute as _pac

            from opteryx.draken.vectors.bool_vector import BoolVector as _BoolVec

            _BOOL_ARROW_OPS = {
                "Lt": _pac.less,
                "Gt": _pac.greater,
                "LtEq": _pac.less_equal,
                "GtEq": _pac.greater_equal,
            }
            fn = _BOOL_ARROW_OPS.get(op)
            if fn is None:
                raise NotImplementedError(f"BoolVector: unsupported op {op!r}")
            result_arr = fn(left.to_arrow(), bool(right))
            result = _BoolVec.from_arrow(result_arr)
    else:
        raise NotImplementedError(f"draken_compare: unsupported vector type {cls!r}")

    return result.not_vector() if negate else result


# --- Native Draken binary operator handler ---


_DATE_TYPES = frozenset(("Date32Vector", "TimestampVector"))
_INTERVAL_TYPES = frozenset(("IntervalVector",))
_TEMPORAL_TYPES = _DATE_TYPES | _INTERVAL_TYPES
_EPOCH_DATE_DAYS = 0  # days-since-epoch base is 1970-01-01


def _date_minus_date_draken(left_vec, right_vec):
    """Subtract two date/timestamp vectors → IntervalVector (no numpy)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    from opteryx.draken.interop.arrow import vector_from_arrow
    from opteryx.expression.intervals import MICROSECONDS_PER_DAY
    from opteryx.expression.intervals import _intervals_to_month_day_nano

    left_arr = left_vec.to_arrow()
    right_arr = right_vec.to_arrow()

    # Unify to microsecond integers for the subtraction.
    if pa.types.is_date32(left_arr.type):
        # date32 days → int32; multiply to microseconds via int64
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
    """Add/subtract an IntervalVector to/from a date/timestamp vector → TimestampVector."""
    from opteryx.expression.intervals import _as_interval_vector

    signum = 1 if op == "Plus" else -1

    if right_vec.__class__.__name__ in _INTERVAL_TYPES:
        date_vec, interval_vec = left_vec, _as_interval_vector(right_vec)
    else:
        date_vec, interval_vec = right_vec, _as_interval_vector(left_vec)

    return interval_vec.apply_to_temporal(date_vec, signum)


def _eval_binary_op_draken(node, morsel):
    """Handle BINARY_OPERATOR natively over Draken vectors for temporal arithmetic.

    Returns a Draken vector, or None if the operation is not handled here
    (caller falls through to the Arrow path).
    """
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

    # DATE/TIMESTAMP - DATE/TIMESTAMP  → IntervalVector
    if op == "Minus" and left_cls in _DATE_TYPES and right_cls in _DATE_TYPES:
        return _date_minus_date_draken(left, right)

    # DATE/TIMESTAMP +/- INTERVAL  →  TimestampVector
    # INTERVAL +/- DATE/TIMESTAMP  →  TimestampVector
    if op in ("Plus", "Minus"):
        if left_cls in _DATE_TYPES and right_cls in _INTERVAL_TYPES:
            return _date_interval_op_draken(left, right, op)
        if left_cls in _INTERVAL_TYPES and right_cls in _DATE_TYPES:
            return _date_interval_op_draken(right, left, op)

    # Non-temporal binary ops are still evaluated inside the Draken-native
    # evaluator boundary. We operate on vector/scalar operands directly and
    # only convert the individual operands/results as needed, rather than
    # bouncing the whole morsel through Arrow expression evaluation.
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


# --- Expression tree walker helpers ---


def _eval_value(node, morsel):
    """Extract a raw Python scalar, Python list, or Draken vector from a tree node.

    Used to resolve the operands of a COMPARISON_OPERATOR node.
    """
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.LITERAL:
        return node.value

    if node_type == NodeType.IDENTIFIER:
        vec = morsel.column(node.schema_column.identity.encode())
        # RUGO stores some types (e.g. date32) as ArrowVector; unwrap to native.
        if vec.__class__.__name__ == "ArrowVector":
            from opteryx.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(vec.to_arrow())
        return vec

    if node_type in (NodeType.EVALUATED, NodeType.AGGREGATOR):
        # AGGREGATOR nodes in HAVING expressions refer to columns that have
        # already been materialised by the preceding GROUP BY node.
        vec = morsel.column(node.schema_column.identity.encode())
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
        right_val = node.right.value  # scalar literal key (str) or index (int)
        op = node.value  # "Arrow", "LongArrow", or "MapAccess"

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

    # --- Arrow-path fallback for node types not yet natively supported ---
    # BINARY_OPERATOR (date/interval arithmetic), CAST, and FUNCTION nodes all
    from opteryx.expression import NodeType as _NT

    if node.node_type == _NT.BINARY_OPERATOR:
        result = _eval_binary_op_draken(node, morsel)
        if result is not None:
            return result
        # Fall through to Arrow path for ops not yet handled natively.

    if node.node_type in (_NT.BINARY_OPERATOR, _NT.CAST, _NT.FUNCTION):
        from opteryx.draken.interop.arrow import vector_from_arrow
        from opteryx.draken.interop.arrow import vector_from_sequence
        from opteryx.expression import _inner_evaluate

        arrow_table = morsel.to_arrow()
        result = _inner_evaluate(node, arrow_table)
        if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
            return vector_from_arrow(result)
        if not hasattr(result, "__iter__") or isinstance(result, (str, bytes, numpy.generic)):
            from opteryx.draken.vectors.constant_vector import from_scalar as _const_scalar

            vec = _const_scalar(result, morsel.num_rows)
            if vec is not None:
                return vec
            return vector_from_arrow(_pa.array([result] * morsel.num_rows))
        return vector_from_sequence(result)

    # Predicate sub-expressions (AND/OR/NOT/COMPARISON_OPERATOR inside a value
    # context, e.g. CASE conditions): evaluate as a BoolVector via the predicate
    # walker.  Everything else raises immediately so gaps are visible.
    return evaluate_draken(node, morsel)


def _unary_draken(op: str, centre_node, morsel):
    """Evaluate a UNARY_OPERATOR node, returning a BoolVector."""
    vec = _eval_value(centre_node, morsel)

    if op == "IsNull":
        return _is_null_as_boolvector(vec)
    if op == "IsNotNull":
        return _is_null_as_boolvector(vec).not_vector()
    if op in ("IsTrue", "IsNotFalse", "IsFalse", "IsNotTrue"):
        # vec is expected to be a BoolVector from a sub-expression
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
    """Evaluate an expression tree over a Draken Morsel.

    Returns a BoolVector for predicate nodes (WHERE/HAVING evaluation).
    NOT safe to call for non-predicate sub-expressions directly; use _eval_value
    for column/literal extraction.

    Args:
        node:   Expression tree node (uses NodeType from managers.expression)
        morsel: Draken Morsel containing the column data

    Returns:
        BoolVector — SQL three-valued-logic null semantics throughout.
    """
    from opteryx.expression import NodeType

    node_type = node.node_type

    if node_type == NodeType.NESTED:
        return evaluate_draken(node.centre, morsel)

    if node_type == NodeType.AND:
        left = evaluate_draken(node.left, morsel)
        if not left.any():
            # Short-circuit: all false, skip right evaluation
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
                return result  # short-circuit: already all-false
            result = result.and_vector(evaluate_draken(sub, morsel))
        return result

    if node_type == NodeType.LITERAL:
        # Scalar boolean literal in WHERE (e.g. WHERE False, WHERE True).
        # NULL literal → all-false (NULL is not truthy in SQL WHERE).
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
        if (
            node.left.schema_column.type in temporal_types
            or node.right.schema_column.type in temporal_types
        ):
            if not hasattr(left, "null_count") and node.left.schema_column.type in temporal_types:
                left = _coerce_temporal_scalar_for_arrow(left, node.left.schema_column.type)
            if not hasattr(right, "null_count") and node.right.schema_column.type in temporal_types:
                right = _coerce_temporal_scalar_for_arrow(right, node.right.schema_column.type)

        if not hasattr(left, "null_count") and not hasattr(right, "null_count"):
            import pyarrow as pa

            from opteryx.draken.vectors.bool_vector import BoolVector
            from opteryx.expression.ops import filter_operations

            scalar_result = filter_operations(
                pa.array([left]),
                node.left.schema_column.type,
                node.value,
                pa.array([right]),
                node.right.schema_column.type,
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
            # The optimizer creates unbound PASSTHRU wrappers post-binding
            # (e.g. collapsing LIKE OR LIKE → RLIKE wrapped in PASSTHRU).
            # PASSTHRU is identity: just evaluate the inner predicate.
            return evaluate_draken(node.parameters[0], morsel)
        parameters = [_eval_value(param, morsel) for param in node.parameters]
        if len(parameters) == 0:
            parameters = [morsel.num_rows]
        result = apply_bounded_function(node, *parameters)
        # Function results must be BoolVector for predicate use
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
        # BINARY_OPERATOR in a predicate context (e.g. address | '54.0.0.0/8' for IP/CIDR).
        # Evaluate via _eval_value (which handles Arrow fallback) and coerce to BoolVector.
        from opteryx.draken.vectors.bool_vector import BoolVector

        result = _eval_value(node, morsel)
        if result.__class__.__name__ == "BoolVector":
            return result
        if isinstance(result, _pa.Array) and _pa.types.is_boolean(result.type):
            return BoolVector.from_arrow(result)
        # numpy bool array
        if isinstance(result, numpy.ndarray) and result.dtype.kind == "b":
            return BoolVector.from_arrow(_pa.array(result, type=_pa.bool_()))
        raise TypeError(
            f"evaluate_draken: BINARY_OPERATOR '{node.value!r}' returned non-boolean {result.__class__.__name__!r}"
        )

    raise NotImplementedError(
        f"evaluate_draken: unsupported node type {node_type!r} (value={node.value!r})"
    )


def evaluate_and_append_draken(nodes, morsel):
    """Evaluate expressions and append their results as new columns.

    Used to pre-evaluate expressions referenced by Draken-native operators
    before the main execution step. Supports the same evaluatable node families
    used by aggregate/group and filter planning:

    - FUNCTION
    - CAST
    - BINARY_OPERATOR
    - EXTRACTION_OPERATOR
    - COMPARISON_OPERATOR
    - LITERAL

    Args:
        nodes:  Iterable of expression nodes
        morsel: Draken Morsel to evaluate against and extend

    Returns:
        New Morsel with appended columns for each evaluated node.
    """
    from opteryx.draken.interop.arrow import vector_from_sequence
    from opteryx.draken.morsels.morsel import Morsel
    from opteryx.expression import NodeType

    col_names = list(morsel.column_names)
    col_vecs = [morsel.column(n if isinstance(n, bytes) else n.encode()) for n in col_names]
    existing = {n.decode() if isinstance(n, bytes) else n for n in col_names}

    for node in nodes:
        if node.value == "_PASSTHRU":
            # PASSTHRU is an optimizer-created predicate wrapper, not a column
            # producer. evaluate_draken handles it inline; skip pre-evaluation.
            continue
        identity = node.schema_column.identity
        if identity in existing:
            continue
        if node.node_type == NodeType.FUNCTION:
            parameters = [_eval_value(param, morsel) for param in node.parameters]
            if len(parameters) == 0:
                parameters = [morsel.num_rows]
            result = apply_bounded_function(node, *parameters)
        else:
            result = _eval_value(node, morsel)
        if result.__class__.__name__ not in (
            "BoolVector",
            "Int64Vector",
            "IntegerVector",
            "Float64Vector",
            "StringVector",
            "TimestampVector",
            "Date32Vector",
            "IntervalVector",
            "TimeVector",
            "DictionaryVector",
            "ConstantVector",
            "ArrayVector",
            "ArrowVector",
        ):
            import pyarrow as _pa

            from opteryx.draken.interop.arrow import vector_from_arrow as _vfa

            if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
                result = _vfa(result)
            elif not hasattr(result, "__iter__") or isinstance(result, (str, bytes)):
                # Scalar result (numpy scalar, Python int/float/str/datetime, etc.) —
                # broadcast to a ConstantVector of the morsel's length.
                from opteryx.draken.vectors.constant_vector import from_scalar as _const_scalar

                vec = _const_scalar(result, morsel.num_rows)
                # from_scalar doesn't handle this type — fall back via PyArrow broadcast
                result = _vfa(_pa.array([result] * morsel.num_rows)) if vec is None else vec
            else:
                result = vector_from_sequence(result)
        col_names.append(identity)
        col_vecs.append(result)
        existing.add(identity)

    return Morsel.from_vectors(col_names, col_vecs)
