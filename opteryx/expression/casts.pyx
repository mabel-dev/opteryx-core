"""Cast operation kernels.

Cython migration of the former casts.py. Contains the parse/coerce kernels
used by both the legacy function-table cast path and the new vectorised
CAST evaluation in the operators.

Architectural contract (Phase 5.3.2):
- Expression-layer functions are Draken-native: Draken vectors or Python
  scalars in, Draken vectors or Python scalars out.
- PyArrow / NumPy are never accepted on the hot path — fail fast.
- Reader-side conversion (PyArrow → Draken) happens at IO boundaries.
"""

import datetime
import decimal as _decimal_mod
import logging
import math

import draken.draken_native as _draken_native_casts

from opteryx.types import OrsoTypes
from opteryx.types._datetime_conversion import timestamp_to_int64_us
from opteryx.utils.vector_types import (
    VectorType,
    get_vector_type,
    is_draken_vector as is_draken_vector_fn,
)


cpdef bint _is_nullish(value):
    """True if value is None or float NaN."""
    return value is None or (isinstance(value, float) and math.isnan(value))


cpdef parse_timestamp_value(value, unit=None):
    """Parse a value into a Python `datetime.datetime`.

    Numeric inputs require an explicit `unit` — ambiguous timestamps are a
    correctness hazard, so fail fast.
    """
    cdef double numeric
    cdef double seconds

    if _is_nullish(value):
        return None

    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time()).replace(tzinfo=None)

    if isinstance(value, (int, float)):
        if unit is None:
            raise TypeError(
                "Ambiguous cast: TIMESTAMP requires a unit. "
                "Use `::TIMESTAMP[ns]`, `::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, "
                "`::TIMESTAMP[us]`, or `::TIMESTAMP[d]`."
            )

        numeric = <double>value
        if unit == "ns":
            seconds = numeric / 1_000_000_000.0
        elif unit == "ms":
            seconds = numeric / 1_000.0
        elif unit == "s":
            seconds = numeric
        elif unit == "us":
            seconds = numeric / 1_000_000.0
        elif unit == "days":
            seconds = numeric * 86_400.0
        else:
            raise ValueError(
                f"Unsupported timestamp unit: {unit!r}. "
                "Use 'ns', 'ms', 's', 'us', or 'days'."
            )

        return datetime.datetime.fromtimestamp(
            seconds, tz=datetime.timezone.utc
        ).replace(tzinfo=None)

    return OrsoTypes.TIMESTAMP.parse(value)


def _parse_array_value(value, element_type, bint safe_cast=False):
    """Parse array values with element-type coercion."""
    if _is_nullish(value):
        return None

    # Duck-typed: any array-like exposing to_pylist (Arrow array, Draken
    # vector). isinstance(bytes/str) intentionally short-circuits — bytes
    # does NOT have to_pylist but we want to treat raw bytes as a single
    # value, not a sequence.
    if not isinstance(value, (bytes, str)):
        pylist_fn = getattr(value, "to_pylist", None)
        if pylist_fn is not None:
            value = pylist_fn()

    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", errors="ignore")

    cdef str stripped
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if safe_cast:
                return safe(OrsoTypes.ARRAY.parse, value, element_type=element_type)
            return OrsoTypes.ARRAY.parse(value, element_type=element_type)
        value = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        value = list(value)
    else:
        value = [value]

    caster = OrsoTypes[element_type.name].parse
    return [caster(item) if item is not None else None for item in value]


def cast_to_double(arr, *args):
    """Cast `arr` to FLOAT64.

    Primary: Draken vectors (FLOAT64 / INT64 / string-family).
    Fallback: Python scalar or list.
    Fails on PyArrow/NumPy arrays per the architectural contract.
    """
    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.FLOAT64:
            return arr
        if v_type == VectorType.INT64:
            return _draken_native_casts.vector_float64_from_sequence(
                [float(v) if v is not None else None for v in arr.to_pylist()]
            )
        if v_type == VectorType.STRING:
            return _draken_native_casts.vector_cast_string_to_float64(arr)

    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.DOUBLE.parse
        return [caster(i) if i is not None else None for i in arr]

    if isinstance(arr, (int, float)):
        return OrsoTypes.DOUBLE.parse(arr)

    raise TypeError(f"Unsupported type for cast_to_double: {type(arr).__name__}")


def cast_to_int(arr, *args):
    """Cast `arr` to INT64."""
    from draken.vectors.integer64_vector import from_sequence
    from opteryx.compiled.nanobind.vector_casts import vector_cast_string_to_int as vector_cast_ascii_to_int
    from opteryx.expression.evaluator.type_coercion import (
        _coerce_date32,
        _coerce_timestamp,
    )

    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.INT64:
            return arr
        if v_type == VectorType.STRING:
            return vector_cast_ascii_to_int(arr)
        if v_type == VectorType.TIMESTAMP:
            return from_sequence(
                [_coerce_timestamp(v) if v is not None else None for v in arr.to_pylist()]
            )
        if v_type == VectorType.DATE32:
            return from_sequence(
                [_coerce_date32(v) if v is not None else None for v in arr.to_pylist()]
            )

    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.INTEGER.parse
        return [caster(i) if i is not None else None for i in arr]

    if isinstance(arr, int):
        return arr

    raise TypeError(f"Unsupported type for cast_to_int: {type(arr).__name__}")


cdef str _array_row_to_json(object row):
    """Encode a list row (from ArrayVector.to_pylist) as a JSON array string.

    Elements are strings or bytes (needing UTF-8 decode) or None (→ null).
    Only `\\` and `"` are escaped — control characters in Parquet string data
    are uncommon and the standard library handles the general case if needed.
    """
    cdef list parts = []
    cdef str s
    for elem in row:
        if elem is None:
            parts.append("null")
        else:
            if isinstance(elem, bytes):
                s = elem.decode("utf-8")
            else:
                s = str(elem)
            s = s.replace("\\", "\\\\").replace('"', '\\"')
            parts.append('"' + s + '"')
    return "[" + ", ".join(parts) + "]"


def cast_to_varchar(arr, *args):
    """Cast `arr` to VARCHAR / StringVector."""
    cdef object v_type
    cdef object row
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.STRING:
            return arr
        if v_type == VectorType.FLOAT64:
            return _draken_native_casts.vector_cast_float64_to_string(arr)
        rows = arr.to_pylist()
        if v_type == VectorType.ARRAY:
            result = [_array_row_to_json(row) if row is not None else None for row in rows]
        else:
            result = [v.decode("utf-8") if isinstance(v, bytes) else (str(v) if v is not None else None) for v in rows]
        return _draken_native_casts.vector_from_string_sequence(result)

    if isinstance(arr, (list, tuple)):
        result = [v.decode("utf-8") if isinstance(v, bytes) else (str(v) if v is not None else None) for v in arr]
        return _draken_native_casts.vector_from_string_sequence(result)

    if isinstance(arr, str):
        return arr

    raise TypeError(f"Unsupported type for cast_to_varchar: {type(arr).__name__}")


def cast_to_boolean(arr, *args):
    """Cast `arr` to BOOL / BoolVector."""
    from draken.vectors.bool_vector import BoolVector

    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.BOOL:
            return arr
        return BoolVector.from_list(
            [bool(v) if v is not None else None for v in arr.to_pylist()]
        )

    if isinstance(arr, (list, tuple)):
        return BoolVector.from_list(
            [bool(v) if v is not None else None for v in arr]
        )

    if isinstance(arr, bool):
        return arr

    raise TypeError(f"Unsupported type for cast_to_boolean: {type(arr).__name__}")


def cast_to_date(arr, *args):
    """Cast `arr` to DATE32. Note: returns a Python list when input is not
    already a Date32Vector — callers materialise via vector_from_sequence
    when they need a vector back. Matches the legacy behaviour."""
    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.DATE32:
            return arr
        caster = OrsoTypes.DATE.parse
        return [caster(v) if v is not None else None for v in arr.to_pylist()]

    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.DATE.parse
        return [caster(v) if v is not None else None for v in arr]

    if isinstance(arr, datetime.date):
        return arr

    raise TypeError(f"Unsupported type for cast_to_date: {type(arr).__name__}")


def safe(func, value, **kwargs):
    """Call `func(value, **kwargs)` and swallow exceptions, returning None.

    Used by the array cast path so a single malformed row doesn't poison
    the rest of the vector. Logs at debug for visibility.
    """
    try:
        return func(value, **kwargs)
    except Exception as err:
        logging.getLogger(__name__).debug(
            f"Cast function {func.__name__} failed on value {value!r}: {err}"
        )
        return None


def _to_int_arg(a):
    """Unwrap a length-1 vector argument or coerce a scalar to int."""
    pylist_fn = getattr(a, "to_pylist", None)
    if pylist_fn is not None:
        pl = pylist_fn()
        return int(pl[0]) if pl else 0
    return int(a)


def _cast_result_to_draken(result, resolved_type, args=()):
    """Dispatch a Python list `result` to the appropriate Draken vector constructor.

    `resolved_type` is an OrsoTypes name string (e.g. "INTEGER", "DOUBLE").
    `args` is the original CAST argument tuple (used for DECIMAL precision/scale).
    Raises TypeError for unrecognised types — fail fast.
    """
    from draken.vectors.bool_vector import BoolVector as _BoolVector_casts
    if resolved_type in ("VARCHAR", "BLOB", "VARBINARY"):
        return _draken_native_casts.vector_from_string_sequence(
            [v.decode("utf-8") if isinstance(v, bytes) else (str(v) if v is not None else None) for v in result]
        )
    if resolved_type in ("INTEGER", "BIGINT"):
        return _draken_native_casts.vector_from_sequence(result)
    if resolved_type == "DOUBLE":
        return _draken_native_casts.vector_float64_from_sequence(result)
    if resolved_type == "BOOLEAN":
        return _BoolVector_casts.from_list(result)
    if resolved_type == "DATE":
        import datetime as _dt
        int_vals = [
            (v - _dt.date(1970, 1, 1)).days if v is not None else None
            for v in result
        ]
        int_vec = _draken_native_casts.vector_from_sequence(int_vals)
        return _draken_native_casts.vector_reinterpret_as_date32(int_vec)
    if resolved_type == "TIMESTAMP":
        from opteryx.types._datetime_conversion import timestamp_to_int64_us as _ts_to_int
        int_vals = [_ts_to_int(v) if v is not None else None for v in result]
        int_vec = _draken_native_casts.vector_from_sequence(int_vals)
        return _draken_native_casts.vector_reinterpret_as_timestamp64(int_vec)
    if resolved_type == "DECIMAL":
        # Infer scale from args if available; default to 6 precision 38 if not.
        precision = int(_to_int_arg(args[0])) if len(args) >= 1 else 38
        scale = int(_to_int_arg(args[1])) if len(args) >= 2 else 6
        return _draken_native_casts.vector_decimal_from_sequence(result, precision, scale)
    if resolved_type == "INTERVAL":
        return _draken_native_casts.vector_interval_from_sequence(result)
    raise TypeError(
        f"_cast_result_to_draken: no Draken constructor for resolved type {resolved_type!r}"
    )


def cast(arr, _type, args=(), unit=None):
    """Factory: return a callable that casts a vector to the requested type.

    Called once per CAST expression at bind time; the returned `_inner`
    runs once per evaluated morsel. Kept as a Python factory so the closure
    over args / unit / _type works naturally — Cython closure rules inside
    cpdef fight this pattern.
    """

    def _inner(arr):
        kwargs = {}

        # VARBINARY isn't a canonical OrsoType — map to BLOB.
        _resolved_type = "VARBINARY" if _type == "VARBINARY" else _type
        caster = OrsoTypes[_resolved_type].parse

        decimal_quantizer = None
        if _type == "DECIMAL":
            # DECIMAL.parse doesn't accept precision/scale; we capture scale
            # here and quantise the parsed Decimal so CAST(x AS DECIMAL(p,s))
            # actually rounds rather than silently no-op'ing.
            if len(args) >= 2:
                _scale = _to_int_arg(args[1])
            elif len(args) == 1:
                _scale = 0
            else:
                _scale = None
            if _scale is not None:
                _quant_exp = _decimal_mod.Decimal(1).scaleb(-_scale)

                def decimal_quantizer(d):  # noqa: E306
                    if d is None:
                        return None
                    if not isinstance(d, _decimal_mod.Decimal):
                        return d
                    try:
                        return d.quantize(_quant_exp)
                    except _decimal_mod.InvalidOperation:
                        return d

        elif _type in ("VARCHAR", "BLOB", "VARBINARY") and len(args) == 1:
            kwargs["length"] = args[0]
        elif _type == "ARRAY" and len(args) == 1:
            kwargs["element_type"] = args[0]

        if _type == "TIMESTAMP":
            if is_draken_vector_fn(arr) and unit is not None:
                v_type = get_vector_type(arr)
                if v_type == VectorType.INT64:
                    from opteryx.compiled.nanobind.vector_casts import (
                        vector_cast_int64_to_timestamp,
                    )
                    return vector_cast_int64_to_timestamp(arr._nb if hasattr(arr, "_nb") else arr, unit=unit)
                if v_type == VectorType.TIMESTAMP:
                    return arr
                if v_type == VectorType.DATE32:
                    from opteryx.compiled.nanobind.vector_temporal_convert import (
                        vector_date32_to_timestamp,
                    )
                    return vector_date32_to_timestamp(arr)

            result = [parse_timestamp_value(i, unit=unit) for i in arr]
            int64_values = [
                timestamp_to_int64_us(dt) if dt is not None else None for dt in result
            ]
            int_vec = _draken_native_casts.vector_from_sequence(int64_values)
            return _draken_native_casts.vector_reinterpret_as_timestamp64(int_vec)

        if _type == "DATE":
            if is_draken_vector_fn(arr):
                v_type = get_vector_type(arr)
                if v_type == VectorType.DATE32:
                    return arr
                if v_type == VectorType.TIMESTAMP:
                    from opteryx.compiled.nanobind.vector_temporal_convert import vector_timestamp_to_date32
                    return vector_timestamp_to_date32(arr)

        if _type == "ARRAY":
            result = [_parse_array_value(i, args[0], safe_cast=False) for i in arr]
            return _draken_native_casts.vector_array_from_sequence(result)

        if _type == "VECTOR":
            result = [caster(i, **kwargs) for i in arr]
            return _draken_native_casts.vector_fp16_from_sequence(result)

        if _type == "VARCHAR" and is_draken_vector_fn(arr):
            return cast_to_varchar(arr)

        result = [caster(i, **kwargs) for i in arr]
        if decimal_quantizer is not None:
            result = [decimal_quantizer(d) for d in result]
        resolved = "BLOB" if _type == "VARBINARY" else _type
        return _cast_result_to_draken(result, resolved, args)

    return _inner
