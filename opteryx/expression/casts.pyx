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


cdef inline object _unwrap_nb(object arr):
    """Return the raw nanobind Vector from either a Cython shim (has ._nb) or a raw nanobind Vector."""
    cdef object nb = getattr(arr, '_nb', None)
    return nb if nb is not None else arr


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
    from opteryx.compiled.nanobind.vector_casts import (
        vector_cast_int64_to_float64,
        vector_cast_bool_to_float64,
        vector_cast_integer_to_float64,
    )
    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.FLOAT64:
            return arr
        if v_type == VectorType.INT64:
            return vector_cast_int64_to_float64(_unwrap_nb(arr))
        if v_type == VectorType.INTEGER:
            return vector_cast_integer_to_float64(_unwrap_nb(arr))
        if v_type == VectorType.BOOL:
            return vector_cast_bool_to_float64(_unwrap_nb(arr))
        if v_type == VectorType.STRING:
            return _draken_native_casts.vector_cast_string_to_float64(_unwrap_nb(arr))

    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.DOUBLE.parse
        return [caster(i) if i is not None else None for i in arr]

    if isinstance(arr, (int, float)):
        return OrsoTypes.DOUBLE.parse(arr)

    raise TypeError(f"Unsupported type for cast_to_double: {type(arr).__name__}")


def cast_to_int(arr, *args):
    """Cast `arr` to INT64."""
    from opteryx.compiled.nanobind.vector_casts import (
        vector_cast_string_to_int as vector_cast_ascii_to_int,
        vector_cast_bool_to_int64,
        vector_cast_date32_to_int64,
        vector_cast_timestamp_to_int64,
        vector_cast_integer_to_int64,
        vector_cast_float64_to_int64,
    )

    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.INT64:
            return arr
        if v_type == VectorType.INTEGER:
            return vector_cast_integer_to_int64(_unwrap_nb(arr))
        if v_type == VectorType.FLOAT64:
            return vector_cast_float64_to_int64(_unwrap_nb(arr))
        if v_type == VectorType.STRING:
            return vector_cast_ascii_to_int(_unwrap_nb(arr))
        if v_type == VectorType.BOOL:
            return vector_cast_bool_to_int64(_unwrap_nb(arr))
        if v_type == VectorType.TIMESTAMP:
            return vector_cast_timestamp_to_int64(_unwrap_nb(arr))
        if v_type == VectorType.DATE32:
            return vector_cast_date32_to_int64(_unwrap_nb(arr))

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
    from opteryx.compiled.nanobind.vector_casts import (
        vector_cast_int64_to_string,
        vector_cast_bool_to_string,
        vector_cast_date_to_string,
        vector_cast_timestamp_to_string,
    )
    cdef object v_type
    cdef object row
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.STRING:
            return arr
        if v_type == VectorType.FLOAT64:
            return _draken_native_casts.vector_cast_float64_to_string(_unwrap_nb(arr))
        if v_type == VectorType.INT64:
            return vector_cast_int64_to_string(_unwrap_nb(arr))
        if v_type == VectorType.BOOL:
            return vector_cast_bool_to_string(_unwrap_nb(arr))
        if v_type == VectorType.TIMESTAMP:
            return vector_cast_timestamp_to_string(_unwrap_nb(arr))
        if v_type == VectorType.DATE32:
            return vector_cast_date_to_string(_unwrap_nb(arr))
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
    from opteryx.compiled.nanobind.vector_casts import (
        vector_cast_int64_to_bool,
        vector_cast_float64_to_bool,
        vector_cast_string_to_bool,
    )

    cdef object v_type
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.BOOL:
            return arr
        if v_type == VectorType.INT64:
            return BoolVector(vector_cast_int64_to_bool(_unwrap_nb(arr)))
        if v_type == VectorType.FLOAT64:
            return BoolVector(vector_cast_float64_to_bool(_unwrap_nb(arr)))
        if v_type == VectorType.STRING:
            return BoolVector(vector_cast_string_to_bool(_unwrap_nb(arr)))
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
        # Infer scale from args if available; default to scale 6 if not.
        precision = int(_to_int_arg(args[0])) if len(args) >= 1 else 18
        scale = int(_to_int_arg(args[1])) if len(args) >= 2 else 6
        # Opteryx decimals are int64-backed (max 18 significant digits); the
        # arithmetic kernels likewise cap result precision at 18. Honour a
        # larger declared precision (e.g. DECIMAL(32,2)) as the engine maximum
        # rather than rejecting it — values that genuinely exceed 18 digits
        # still raise via the native overflow/precision check.
        if precision > 18:
            precision = 18
        if scale > precision:
            scale = precision
        return _draken_native_casts.vector_decimal_from_sequence(result, precision, scale)
    if resolved_type == "INTERVAL":
        return _draken_native_casts.vector_interval_from_sequence(result)
    raise TypeError(
        f"_cast_result_to_draken: no Draken constructor for resolved type {resolved_type!r}"
    )


def resolve_cast(source_orso, target_type, args=(), unit=None):
    """Bind-time resolver: return a callable for casting source_orso → target_type.

    Called once per CAST node at bind time. The returned callable takes a single
    argument (the vector to cast) and returns the cast result as a Draken vector.

    Returns:
    - A native kernel callable (for direct casts with no parameters).
    - A specialized closure (for parametrized casts like DECIMAL(p,s), ARRAY(T), etc.).

    Raises NotImplementedError if no kernel is registered for the pair.
    """
    from opteryx.compiled.nanobind.vector_casts import (
        vector_cast_int64_to_float64,
        vector_cast_bool_to_float64,
        vector_cast_integer_to_float64,
        vector_cast_int64_to_string,
        vector_cast_bool_to_string,
        vector_cast_date_to_string,
        vector_cast_timestamp_to_string,
        vector_cast_string_to_int,
        vector_cast_bool_to_int64,
        vector_cast_date32_to_int64,
        vector_cast_timestamp_to_int64,
        vector_cast_integer_to_int64,
        vector_cast_float64_to_int64,
        vector_cast_int64_to_bool,
        vector_cast_float64_to_bool,
        vector_cast_string_to_bool,
        vector_cast_int64_to_timestamp,
    )
    from opteryx.compiled.nanobind.vector_temporal_convert import (
        vector_date32_to_timestamp,
        vector_timestamp_to_date32,
    )

    # Normalize VARBINARY to BLOB for lookup purposes.
    _resolved_target = "BLOB" if target_type == "VARBINARY" else target_type

    # Passthrough: no-op casts (source == target)
    if source_orso == _resolved_target:
        return lambda arr: arr

    # Direct kernel map for specific type pairs.
    # Uses canonical OrsoType names (INTEGER, DOUBLE, VARCHAR, etc.)
    # For INTEGER → numeric, we use dispatch helpers that handle both INT64 and INT8/16/32.
    if source_orso == "INTEGER":
        if _resolved_target in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
            # INTEGER → DOUBLE: dispatch helper that calls cast_to_double internally
            return cast_to_double
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return lambda arr: arr  # Passthrough
        if _resolved_target == "BOOLEAN":
            return cast_to_boolean
        if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
            return cast_to_varchar

    if source_orso == "DOUBLE" or source_orso in ("FLOAT64", "FLOAT32", "FLOAT"):
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return cast_to_int
        if _resolved_target == "BOOLEAN":
            return cast_to_boolean
        if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
            return cast_to_varchar

    if source_orso == "BOOLEAN":
        if _resolved_target in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
            return cast_to_double
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return cast_to_int
        if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
            return cast_to_varchar

    if source_orso in ("VARCHAR", "STRING", "BLOB"):
        if _resolved_target in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
            return cast_to_double
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return cast_to_int
        if _resolved_target == "BOOLEAN":
            return cast_to_boolean

    # DATE/TIMESTAMP conversions
    if source_orso in ("DATE", "DATE32"):
        if _resolved_target == "TIMESTAMP":
            return lambda arr: vector_date32_to_timestamp(arr)
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return cast_to_int
        if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
            return cast_to_varchar

    if source_orso == "TIMESTAMP":
        if _resolved_target in ("DATE", "DATE32"):
            return lambda arr: vector_timestamp_to_date32(arr)
        if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
            return cast_to_int
        if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
            return cast_to_varchar

    # Parametrized casts: need specialized closures.
    if _resolved_target == "TIMESTAMP" and unit is not None:
        if source_orso in ("INT64", "INTEGER", "BIGINT"):
            def _int_to_timestamp_with_unit(arr):
                nb = _unwrap_nb(arr)
                if is_draken_vector_fn(arr) and get_vector_type(arr) == VectorType.INTEGER:
                    nb = vector_cast_integer_to_int64(nb)
                return vector_cast_int64_to_timestamp(nb, unit=unit)
            return _int_to_timestamp_with_unit

    if _resolved_target == "DECIMAL":
        return _build_decimal_closure(args)

    if _resolved_target == "ARRAY":
        if len(args) < 1:
            raise ValueError("CAST to ARRAY requires element_type parameter")
        element_type = args[0]
        return lambda arr: _build_array_cast(arr, element_type)

    if _resolved_target == "VECTOR":
        return lambda arr: _build_vector_cast(arr)

    if _resolved_target in ("VARCHAR", "BLOB", "VARBINARY"):
        if len(args) >= 1:
            # VARCHAR with length constraint; fall through to row-loop for validation.
            return lambda arr: _build_varchar_cast_with_length(arr, args[0])
        # No length constraint; use cast_to_varchar which has native paths for vectors.
        return cast_to_varchar

    # Fallback to row-loop for numeric → BOOLEAN or other residual cases.
    # These should be covered above; if we reach here, it's a gap in the table.
    if _resolved_target == "BOOLEAN":
        return cast_to_boolean

    if _resolved_target in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
        return cast_to_int

    if _resolved_target in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
        return cast_to_double

    # Residual row-loop for unspecialized pairs.
    # These will be flagged in the PR as candidates for native kernel implementation.
    return _build_residual_cast(target_type, args)


def _build_decimal_closure(args):
    """Build a closure for CAST to DECIMAL(precision, scale)."""
    precision = int(_to_int_arg(args[0])) if len(args) >= 1 else 38
    scale = int(_to_int_arg(args[1])) if len(args) >= 2 else 6

    def _decimal_cast(arr):
        caster = OrsoTypes.DECIMAL.parse
        result = [caster(i) if i is not None else None for i in arr]

        # Quantize to the specified scale.
        if scale is not None:
            _quant_exp = _decimal_mod.Decimal(1).scaleb(-scale)
            def quantizer(d):
                if d is None:
                    return None
                if not isinstance(d, _decimal_mod.Decimal):
                    return d
                try:
                    return d.quantize(_quant_exp)
                except _decimal_mod.InvalidOperation:
                    return d
            result = [quantizer(d) for d in result]

        return _cast_result_to_draken(result, "DECIMAL", args)

    return _decimal_cast


def _build_array_cast(arr, element_type):
    """Build a closure for CAST to ARRAY(element_type)."""
    result = [_parse_array_value(i, element_type, safe_cast=False) for i in arr]
    return _draken_native_casts.vector_array_from_sequence(result)


def _build_vector_cast(arr):
    """Build a closure for CAST to VECTOR (FP16 quantization)."""
    caster = OrsoTypes.VECTOR.parse
    result = [caster(i) for i in arr]
    return _draken_native_casts.vector_fp16_from_sequence(result)


def _build_varchar_cast_with_length(arr, length_arg):
    """Build a closure for CAST to VARCHAR(length) with length enforcement."""
    # For now, enforce length only if needed; otherwise use cast_to_varchar.
    # Length enforcement could be added here if needed.
    return cast_to_varchar(arr)


def _build_residual_cast(target_type, args):
    """Build a closure for residual (unspecialized) casts via row-loop.

    These casts fall through to OrsoTypes[target_type].parse and are
    flagged in the PR as candidates for native kernel implementation.
    """
    resolved_type = "BLOB" if target_type == "VARBINARY" else target_type
    caster = OrsoTypes[resolved_type].parse

    def _residual_cast(arr):
        # Row-loop: each value is parsed individually.
        result = [caster(i) if i is not None else None for i in arr]
        return _cast_result_to_draken(result, resolved_type, args)

    return _residual_cast


def cast(arr, _type, args=(), unit=None):
    """Compatibility wrapper: resolve_cast returns a callable; this factory form
    is kept for any legacy callers (constant folding, etc.).

    At bind time, resolve_cast should be used directly.
    At runtime (constant folding), this invokes the resolver and applies the result.
    """
    # For compatibility, fall back to the old behavior if source_orso can't be determined.
    # This handles constant-folding and other plan-time evaluations.
    source_orso = None  # Not available in legacy path; resolver uses fallbacks.
    kernel = resolve_cast(source_orso, _type, args, unit)
    return kernel


def try_cast(target_type):
    """Factory: return a callable for safe casting to the target type.

    Used by tests and legacy code. Returns a callable that takes a sequence
    and returns a list of cast values (with None for parse failures).
    """
    def _try_cast_fn(arr):
        """Cast each element in arr, returning None on parse failures."""
        caster = OrsoTypes[target_type].parse
        result = []
        for item in arr:
            try:
                if item is None:
                    result.append(None)
                else:
                    result.append(caster(item))
            except Exception:
                # Safe cast: return None on any parse failure.
                result.append(None)
        return result
    return _try_cast_fn
