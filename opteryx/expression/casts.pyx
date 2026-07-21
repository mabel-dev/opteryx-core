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

from opteryx.types.logical_type import LogicalCategory
from opteryx.types.scalars.value_parsing import parse_value, parser_for
from opteryx.types.timestamps._datetime_conversion import timestamp_to_int64_us


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

    return parse_value(LogicalCategory.TIMESTAMP, value)


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
                return safe(parser_for(LogicalCategory.ARRAY), value)
            return parse_value(LogicalCategory.ARRAY, value)
        value = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        value = list(value)
    else:
        value = [value]

    caster = parser_for(LogicalCategory[element_type.name])
    return [caster(item) if item is not None else None for item in value]


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


def _build_array_to_json(arr):
    """CAST(array AS VARCHAR): encode each ARRAY row as a JSON string. ARRAY is the
    one cast path excused from zero-Python (CLAUDE.md carve-out)."""
    cdef object row
    rows = arr.to_pylist()
    # vector_from_string_sequence is bytes-only — encode each JSON string to bytes.
    result = [
        _array_row_to_json(row).encode("utf-8") if row is not None else None
        for row in rows
    ]
    return _draken_native_casts.vector_from_string_sequence(result)


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

    `resolved_type` is an LogicalCategory name string (e.g. "INTEGER", "DOUBLE").
    `args` is the original CAST argument tuple (used for DECIMAL precision/scale).
    Raises TypeError for unrecognised types — fail fast.
    """
    from draken.vectors.bool_vector import BoolVector as _BoolVector_casts
    if resolved_type in ("VARCHAR", "BLOB", "VARBINARY"):
        # vector_from_string_sequence is bytes-only — normalize each element to
        # bytes (str must not reach the Draken edge).
        return _draken_native_casts.vector_from_string_sequence(
            [v if isinstance(v, bytes) else (str(v).encode("utf-8") if v is not None else None) for v in result]
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
        from opteryx.types.timestamps._datetime_conversion import timestamp_to_int64_us as _ts_to_int
        int_vals = [_ts_to_int(v) if v is not None else None for v in result]
        int_vec = _draken_native_casts.vector_from_sequence(int_vals)
        return _draken_native_casts.vector_reinterpret_as_timestamp64(int_vec)
    if resolved_type == "DECIMAL":
        # Infer precision/scale from args; default to DECIMAL(18, 6) (Decision F).
        precision = int(_to_int_arg(args[0])) if len(args) >= 1 else 18
        scale = int(_to_int_arg(args[1])) if len(args) >= 2 else 6
        # 38 significant digits is the int128 (DECIMAL128) maximum. Honour a larger
        # declared precision as the engine maximum rather than rejecting it; values
        # that genuinely exceed 38 digits still raise via the native check.
        if precision > 38:
            precision = 38
        if scale > precision:
            scale = precision
        # p ≤ 18 → int64-backed DECIMAL (fast tier); 19 ≤ p ≤ 38 → int128-backed
        # DECIMAL128 (the correct-but-scalar tier, doc 06).
        if precision > 18:
            return _draken_native_casts.vector_decimal128_from_sequence(result, precision, scale)
        return _draken_native_casts.vector_decimal_from_sequence(result, precision, scale)
    if resolved_type == "INTERVAL":
        return _draken_native_casts.vector_interval_from_sequence(result)
    raise TypeError(
        f"_cast_result_to_draken: no Draken constructor for resolved type {resolved_type!r}"
    )


# String-family and narrow-integer physical-type sets (DrakenType.name tokens).
_CAST_STRINGS = ("VARCHAR", "NVARCHAR", "VARBINARY")
_CAST_NARROW_INT = ("INT8", "INT16", "INT32")
_CAST_UNSIGNED_INT = ("UINT8", "UINT16", "UINT32", "UINT64")  # E33


def _c_native_cast(source_physical, target_type, bint safe=False):
    """Return (c_kernel_name, ctx_unit_code) for casts that have a REAL, REGISTERED
    C-ABI kernel the executor can dispatch zero-Python via BC_INSTR_C_NATIVE, or
    None (those fall back to the resolve_cast closure).

    Y, increment 1: only kernels producing a FIXED-WIDTH result (INT64/FLOAT64/
    BOOL) — these fold into the frame arena as a dense DV* with no Python object.
    String- and timestamp-producing kernels (and parametrized/closure casts) are
    NOT listed yet; they stay on the closure path until later increments wire
    their result handling. TRY_CAST stays on the closure path (conservative).

    The set grows as kernels become real+registered. This table is the single
    source of truth for which casts run C-native.
    """
    cdef str s = source_physical
    cdef str t = "BLOB" if target_type == "VARBINARY" else target_type
    if s is None or safe:
        return None
    if t in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
        if s == "INT64":
            return ("draken_cast_int64_to_float64", 0)
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_float64", 0)
        if s == "BOOL":
            return ("draken_cast_bool_to_float64", 0)
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_float64", 0)
        return None
    if t in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_int64", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return ("draken_cast_float64_to_int64", 0)
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_int64", 0)
        if s == "BOOL":
            return ("draken_cast_bool_to_int64", 0)
        if s == "TIMESTAMP64":
            return ("draken_cast_timestamp_to_int64", 0)
        if s == "DATE32":
            return ("draken_cast_date32_to_int64", 0)
        # E33 — reverse direction: any unsigned source -> INT64, range-checked
        # (a UINT64 value > INT64_MAX raises rather than wrapping negative).
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_int64", 0)
        return None
    # E33 — UINT8/16/32/64 target. Range-checked in the kernel itself (negative,
    # NaN, or out-of-range magnitude raises — never silently truncates/wraps).
    if t in ("UINT8", "UINT16", "UINT32", "UINT64"):
        if s in _CAST_NARROW_INT or s == "INT64":
            return (f"draken_cast_integer_to_{t.lower()}", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return (f"draken_cast_float_to_{t.lower()}", 0)
        if s == "BOOL":
            return (f"draken_cast_bool_to_{t.lower()}", 0)
        if s in _CAST_STRINGS:
            return (f"draken_cast_string_to_{t.lower()}", 0)
        return None
    if t in ("DATE", "DATE32"):
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_date32", 0)
        return None
    if t == "BOOLEAN":
        if s == "INT64":
            return ("draken_cast_int64_to_bool", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return ("draken_cast_float64_to_bool", 0)
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_bool", 0)
        return None
    # → TIMESTAMP (fixed-width int64; unit rides in cast_timestamp_ctx). The
    # date32 kernel emits a DENSE gathered result (uniform selection read) after
    # a dict-shape wrong-answer bug in its first shape-preserving version.
    # int64→timestamp stays OFF (unverified against a value oracle).
    if t in ("TIMESTAMP", "TIMESTAMP64"):
        if s in _CAST_STRINGS:
            # Default ISO-8601 parse or CAST ... FORMAT (compiled_expression.pyx
            # allocates the format_ctx from the CAST node's literal FORMAT pattern,
            # or a zero-fmt_len ctx for the default — see the FORMAT_TIMESTAMP-style
            # ctx-building chain there).
            return ("draken_cast_string_to_timestamp", 0)
        if s == "DATE32":
            return ("draken_cast_date32_to_timestamp", 0)
        if s == "INT64":
            # INT64 and TIMESTAMP64 share the 8-byte payload, but a verbatim
            # retag is only correct when the int is ALREADY at the declared
            # result unit. `x::TIMESTAMP[s]` means "interpret x as SECONDS",
            # and the binder declares the RESULT at canonical us — so the
            # values must SCALE (x1e6 for [s]). Route through the rescale
            # kernel (suffix = source unit, declared ct = target unit); it
            # degrades to a copy when the units happen to match.
            return ("draken_cast_timestamp_rescale", 0)
        if s == "TIMESTAMP64":
            # Rescales source-unit -> the BINDER-DECLARED result unit (always
            # canonical us today; the `[s]` suffix re-types the SOURCE's
            # interpretation, not the target). The earlier "1970 garbage" was a
            # value/descriptor mismatch from targeting the SQL-suffix unit while
            # ExprProject re-attached the plan-declared (us) descriptor — the ctx
            # plumbing was never at fault (values survived verbatim, unit lied).
            return ("draken_cast_timestamp_rescale", 0)
        return None
    # → DECIMAL (both tiers; the target (precision, scale) rides in a
    # binary_op_ctx the lowering allocates from the cast params).
    if t == "DECIMAL":
        if s in ("FLOAT64", "FLOAT32"):
            return ("draken_cast_float_to_decimal", 0)
        return None
    # → VARCHAR / BLOB (string result; executor owns it as a Vector). NVARCHAR as
    # a TARGET is handled separately below (validate+retag, a different kernel);
    # NVARCHAR as a SOURCE is fine here (its bytes are always valid VARCHAR/
    # VARBINARY bytes, no validation needed — see string_retag_core).
    if t in ("VARCHAR", "BLOB"):
        if s in _CAST_STRINGS:
            # VARCHAR/NVARCHAR/VARBINARY share the exact DrakenStringArena byte
            # layout (buffers.h §11) — this is a retag, not a reformat.
            return ("draken_cast_string_to_blob" if t == "BLOB"
                    else "draken_cast_string_to_varchar", 0)
        # Below: numeric/bool/decimal/temporal sources format to the identical
        # ASCII bytes for either target — the `_to_blob` twin (t == "BLOB") just
        # retags the `_to_string` kernel's result rather than reformatting (see
        # the DRAKEN_CAST_TO_BLOB doc comment in cast_numeric.cpp). Returning the
        # plain `_to_string` name here for a BLOB target would silently mistag
        # the result VARCHAR — that was the bug.
        if s == "INT64":
            return ("draken_cast_int64_to_blob" if t == "BLOB" else "draken_cast_int64_to_string", 0)
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_blob" if t == "BLOB" else "draken_cast_integer_to_string", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return ("draken_cast_float64_to_blob" if t == "BLOB" else "draken_cast_float64_to_string", 0)
        if s == "BOOL":
            return ("draken_cast_bool_to_blob" if t == "BLOB" else "draken_cast_bool_to_string", 0)
        if s == "TIMESTAMP64":
            return ("draken_cast_timestamp_to_blob" if t == "BLOB" else "draken_cast_timestamp_to_string", 0)
        if s == "DATE32":
            return ("draken_cast_date_to_blob" if t == "BLOB" else "draken_cast_date_to_string", 0)
        if s == "INTERVAL":
            # Default ISO-8601 duration or CAST ... FORMAT (same ctx mechanism as
            # the DATE32/TIMESTAMP64 arms above, tokens reinterpreted as duration
            # magnitudes — see interval_to_sql_fields in sql_temporal_format.h).
            return ("draken_cast_interval_to_blob" if t == "BLOB" else "draken_cast_interval_to_string", 0)
        # DECIMAL → VARCHAR/BLOB. The source scale (LogicalType, not on the vector)
        # is loaded into a binary_op_ctx by the lowering (see the decimal-source
        # `_to_string`/`_to_blob` arm in compiled_expression.pyx). Two physical tiers.
        if s == "DECIMAL":
            return ("draken_cast_decimal_to_blob" if t == "BLOB" else "draken_cast_decimal_to_string", 0)
        if s == "DECIMAL128":
            return ("draken_cast_decimal128_to_blob" if t == "BLOB" else "draken_cast_decimal128_to_string", 0)
        if s == "TIME64":
            # No `_to_blob` twin kernel exists yet — VARCHAR only (falls to the
            # resolve_cast closure path for a BLOB target rather than silently
            # mistagging the VARCHAR-formatted result, per the DECIMAL/DATE/
            # TIMESTAMP comment above).
            return ("draken_cast_time_to_string", 0) if t == "VARCHAR" else None
        return None
    # → TIME (fixed-width TIME64; only string sources are reachable from SQL —
    # TIME() always resolves to TIME64/microseconds, see logical_type.TIME()).
    if t == "TIME":
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_time64", 0)
        return None
    # → NVARCHAR: validates UTF-8 per row (raises on the first invalid row) then
    # retags — see draken_cast_string_to_nvarchar in cast_string.cpp. Only
    # string-family sources are native; a non-string source (INT64 etc.) first
    # needs VARCHAR formatting, which stays on the resolve_cast closure path
    # (matches this function's "no chained native kernels" posture elsewhere).
    if t == "NVARCHAR":
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_nvarchar", 0)
        return None
    return None


def _late_bound_cast(target_type, args, unit, bint safe):
    """Escape hatch for CAST whose source type the binder left unresolved.

    The binder does not attach a column_type to every cast source operand (ARRAY
    columns are the known case — ARRAY is the cast path excused from zero-Python).
    When that happens we cannot pick the kernel at bind time, so we resolve it from
    the runtime vector's *physical* type on first use. This is the only type
    dispatch left in the cast path; it fires solely for binder-untyped sources.
    """
    from draken.vectors.vector import Vector
    from draken.vectors.bool_vector import BoolVector

    def _lb(arr):
        fn, needs_nb, returns_raw = resolve_cast(arr.type.name, target_type, args, unit, safe)
        inp = arr._nb if (needs_nb and isinstance(arr, Vector)) else arr
        res = fn(inp)
        if not returns_raw:
            return res
        if target_type == "BOOLEAN":
            return BoolVector(res)
        return Vector(res)

    return _lb


def resolve_cast(source_physical, target_type, args=(), unit=None, bint safe=False):
    """Bind-time resolver: (source physical type, target category) → cast kernel.

    Called once per CAST node at bind time. `source_physical` is the source
    ColumnType.physical DrakenType name string (e.g. "INT64", "FLOAT64",
    "VARCHAR"); None when the source type is unknown.

    Returns a 3-tuple ``(kernel, needs_nb_input, returns_raw_nb)``:
      kernel          — callable taking ONE vector arg, returning the cast result.
      needs_nb_input  — True if the kernel expects a raw nanobind Vector (the
                        executor unwraps the Cython shim to ``._nb`` before the
                        call); False if it takes the Cython shim / iterates it.
      returns_raw_nb  — True if the kernel returns a raw nanobind Vector (the
                        executor wraps it in Vector()/BoolVector()); False if it
                        returns an already-wrapped Cython Vector / passthrough.

    There is no per-morsel type dispatch: the exact native kernel is chosen here,
    at bind time, from the physical source type. Raises NotImplementedError for
    unsupported pairs — no row-loop fallback.
    """
    from opteryx.compiled.nanobind.vectors import (
        vector_cast_int64_to_float64,
        vector_cast_bool_to_float64,
        vector_cast_integer_to_float64,
        vector_cast_int64_to_string,
        vector_cast_integer_to_string,
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
        vector_cast_string_to_date32,
        vector_cast_string_to_nvarchar,
        vector_cast_int64_to_timestamp,
        vector_cast_string_to_time64,
        vector_cast_time_to_string,
        vector_cast_interval_to_string,
        vector_cast_string_to_timestamp,
    )
    from opteryx.compiled.nanobind.vectors import (
        vector_date32_to_timestamp,
        vector_timestamp_to_date32,
    )

    cdef str s = source_physical
    cdef str t = "BLOB" if target_type == "VARBINARY" else target_type

    # Binder left the source untyped (e.g. ARRAY columns) — resolve from the runtime
    # vector's physical type on first use. Only dispatch left in the cast path.
    if s is None:
        return _late_bound_cast(target_type, args, unit, safe), False, False

    # ---- Parametrized / non-native targets (separate-track + excused closures) ----
    if t == "DECIMAL":
        # DECIMAL has no native kernel yet — Python row-loop (separate track).
        return _build_decimal_closure(args, safe), False, True
    if t == "ARRAY":
        if len(args) < 1:
            raise ValueError("CAST to ARRAY requires element_type parameter")
        element_type = args[0]
        return (lambda arr: _build_array_cast(arr, element_type)), False, True
    if t == "VECTOR":
        # VECTOR (FP16) has no native kernel yet — Python row-loop (separate track).
        return (lambda arr: _build_vector_cast(arr)), False, True

    # ---- NVARCHAR: validate UTF-8 per row + retag (non-string sources stringified) ----
    if t == "NVARCHAR":
        if s in _CAST_STRINGS:
            return (lambda nb: vector_cast_string_to_nvarchar(nb, safe)), True, True
        vfn, _vni, _vrr = resolve_cast(s, "VARCHAR", (), None)
        return (lambda nb: vector_cast_string_to_nvarchar(vfn(nb), safe)), True, True

    # ---- TIMESTAMP target (parametrized unit for integer sources) ----
    if t == "TIMESTAMP":
        if s == "TIMESTAMP64":
            return (lambda arr: arr), False, False
        if s in _CAST_STRINGS:
            # FORMAT (when present) only compiles through the C-native ctx path
            # (compiled_expression.pyx) — this closure covers the no-FORMAT,
            # default-ISO-8601 case only.
            return vector_cast_string_to_timestamp, True, True
        if s == "DATE32":
            return vector_date32_to_timestamp, True, True
        if s == "INT64" or s in _CAST_NARROW_INT:
            if unit is None:
                raise NotImplementedError(
                    "CAST to TIMESTAMP from an integer requires a unit (e.g. ::TIMESTAMP[us])"
                )
            _narrow = s in _CAST_NARROW_INT
            def _int_to_timestamp_with_unit(nb):
                if _narrow:
                    nb = vector_cast_integer_to_int64(nb)
                return vector_cast_int64_to_timestamp(nb, unit=unit)
            return _int_to_timestamp_with_unit, True, True
        raise NotImplementedError(f"No native CAST {source_physical} → TIMESTAMP")

    # ---- DATE target ----
    if t in ("DATE", "DATE32"):
        if s == "DATE32":
            return (lambda arr: arr), False, False
        if s in _CAST_STRINGS:
            return vector_cast_string_to_date32, True, True
        if s == "TIMESTAMP64":
            return vector_timestamp_to_date32, True, True
        raise NotImplementedError(f"No native CAST {source_physical} → DATE")

    # ---- TIME target (string parse; only TIME64/microseconds is reachable
    # from SQL — TIME() always resolves to TIME64, see logical_type.TIME()) ----
    if t == "TIME":
        if s == "TIME64":
            return (lambda arr: arr), False, False
        if s in _CAST_STRINGS:
            return vector_cast_string_to_time64, True, True
        raise NotImplementedError(f"No native CAST {source_physical} → TIME")

    # ---- DOUBLE / FLOAT target (→ float64) ----
    if t in ("DOUBLE", "FLOAT", "FLOAT64", "FLOAT32"):
        if s in ("FLOAT64", "FLOAT32"):
            return (lambda arr: arr), False, False
        if s == "INT64":
            return vector_cast_int64_to_float64, True, True
        if s in _CAST_NARROW_INT:
            return vector_cast_integer_to_float64, True, True
        if s == "BOOL":
            return vector_cast_bool_to_float64, True, True
        if s in _CAST_STRINGS:
            return _draken_native_casts.vector_cast_string_to_float64, True, True
        raise NotImplementedError(f"No native CAST {source_physical} → DOUBLE")

    # ---- INTEGER target (→ int64) ----
    if t in ("INTEGER", "BIGINT", "INT64", "INT32", "INT16", "INT8"):
        if s == "INT64":
            return (lambda arr: arr), False, False
        if s in _CAST_NARROW_INT:
            return vector_cast_integer_to_int64, True, True
        if s in ("FLOAT64", "FLOAT32"):
            return vector_cast_float64_to_int64, True, True
        if s in _CAST_STRINGS:
            return vector_cast_string_to_int, True, True
        if s == "BOOL":
            return vector_cast_bool_to_int64, True, True
        if s == "TIMESTAMP64":
            return vector_cast_timestamp_to_int64, True, True
        if s == "DATE32":
            return vector_cast_date32_to_int64, True, True
        # E33 — reverse direction fallback (the C-native path above,
        # draken_cast_uint_to_int64, handles this at bind time when the source
        # type is known; this closure only runs for late-bound sources).
        # Range-checked: a UINT64 value > INT64_MAX raises, never wraps.
        if s in _CAST_UNSIGNED_INT:
            def _uint_to_int64_cast(arr):
                result = [int(v) if v is not None else None for v in arr]
                return _draken_native_casts.vector_from_sequence(result)
            return _uint_to_int64_cast, False, True
        raise NotImplementedError(f"No native CAST {source_physical} → INTEGER")

    # ---- UINT8/16/32/64 target (E33) ----
    # No native zero-Python kernel yet (matches DECIMAL/ARRAY/VECTOR above) — a
    # row-loop closure that parses each value as an integer, then hands the
    # Python list to the width-specific native constructor, which performs the
    # actual range check (negative values and out-of-range magnitude both raise
    # OverflowError there — fail loud, never silently wrap/truncate).
    if t in ("UINT8", "UINT16", "UINT32", "UINT64"):
        def _uint_cast(arr):
            caster = parser_for(LogicalCategory.INTEGER)
            result = [caster(i) if i is not None else None for i in arr]
            if t == "UINT8":
                return _draken_native_casts.vector_uint8_from_sequence(result)
            if t == "UINT16":
                return _draken_native_casts.vector_uint16_from_sequence(result)
            if t == "UINT32":
                return _draken_native_casts.vector_uint32_from_sequence(result)
            return _draken_native_casts.vector_uint64_from_sequence(result)
        return _uint_cast, False, True

    # ---- BOOLEAN target ----
    if t == "BOOLEAN":
        if s == "BOOL":
            return (lambda arr: arr), False, False
        if s == "INT64":
            return vector_cast_int64_to_bool, True, True
        if s in ("FLOAT64", "FLOAT32"):
            return vector_cast_float64_to_bool, True, True
        if s in _CAST_STRINGS:
            return vector_cast_string_to_bool, True, True
        raise NotImplementedError(f"No native CAST {source_physical} → BOOLEAN")

    # ---- VARCHAR / BLOB target (→ string) ----
    if t in ("VARCHAR", "BLOB"):
        if s in _CAST_STRINGS:
            return (lambda arr: arr), False, False
        if s == "INT64":
            return vector_cast_int64_to_string, True, True
        if s in _CAST_NARROW_INT:
            # Single pass at the source width — the kernel reads INT8/16/32 at its
            # native stride (no widen-to-int64 detour).
            return vector_cast_integer_to_string, True, True
        if s in ("FLOAT64", "FLOAT32"):
            return _draken_native_casts.vector_cast_float64_to_string, True, True
        if s == "BOOL":
            return vector_cast_bool_to_string, True, True
        if s == "TIMESTAMP64":
            return vector_cast_timestamp_to_string, True, True
        if s == "DATE32":
            return vector_cast_date_to_string, True, True
        if s == "INTERVAL":
            # FORMAT (when present) only compiles through the C-native ctx path —
            # this closure covers the no-FORMAT, default-ISO-8601-duration case.
            if t == "BLOB":
                raise NotImplementedError("No native CAST INTERVAL → VARBINARY")
            return vector_cast_interval_to_string, True, True
        if s == "TIME64":
            if t == "BLOB":
                raise NotImplementedError("No native CAST TIME64 → VARBINARY")
            return vector_cast_time_to_string, True, True
        if s == "ARRAY":
            return _build_array_to_json, False, True
        if s in ("DECIMAL", "DECIMAL128"):
            # DECIMAL → VARCHAR is native-only: correct text needs the source
            # scale, which lives on the bind-time ColumnType, NOT the runtime
            # vector — so no correct scale-less Python closure can exist. The
            # C-native kernel (draken_cast_decimal{,128}_to_string) always handles
            # this pair via BC_INSTR_C_NATIVE; this callable_ref is dead there. It
            # fails loud rather than silently emit an unscaled integer if the
            # native kernel is ever absent.
            return _decimal_to_string_native_only, False, False
        raise NotImplementedError(f"No native CAST {source_physical} → VARCHAR")

    raise NotImplementedError(
        f"No native CAST kernel for {source_physical} → {target_type}"
    )


def _decimal_to_string_native_only(arr):
    """callable_ref for DECIMAL → VARCHAR: native-only, no Python fallback.

    The C-native kernel always services this cast (the source scale is threaded
    into a binary_op_ctx at bind time). This is only reached if that kernel is
    somehow absent from the registry — in which case fail loud, never silently
    emit an unscaled integer.
    """
    raise NotImplementedError(
        "CAST DECIMAL → VARCHAR requires the native draken_cast_decimal_to_string "
        "kernel; there is no Python fallback (the source scale is not carried on "
        "the runtime vector)."
    )


def _build_decimal_closure(args, bint safe=False):
    """Build a closure for CAST to DECIMAL(precision, scale).

    A declared scale is a contract, not a hint: `CAST(x AS DECIMAL(10,2))` means
    the caller has explicitly said "2 fractional digits" — a value with MORE
    precision than that must not be silently rounded away. Plain CAST fails
    loud (matches the native decimal_to_unscaled kernel's own "value has more
    decimal places than the declared scale" check); TRY_CAST maps that row to
    NULL, consistent with every other TRY_CAST parse-failure path in this file.
    """
    # Only `scale` is needed here (for quantization); the precision/scale used for the
    # actual native construction are re-read (and capped at 18) inside
    # _cast_result_to_draken. Bare DECIMAL → scale 6 (Decision F: DECIMAL(18,6)).
    scale = int(_to_int_arg(args[1])) if len(args) >= 2 else 6

    def _decimal_cast(arr):
        caster = parser_for(LogicalCategory.DECIMAL)
        result = [caster(i) if i is not None else None for i in arr]

        # Quantize to the specified scale — exactly, never lossily.
        if scale is not None:
            _quant_exp = _decimal_mod.Decimal(1).scaleb(-scale)
            def quantizer(d):
                if d is None:
                    return None
                if not isinstance(d, _decimal_mod.Decimal):
                    return d
                try:
                    q = d.quantize(_quant_exp)
                except _decimal_mod.InvalidOperation:
                    if safe:
                        return None
                    raise ValueError(
                        f"Cannot CAST {d} to DECIMAL(scale={scale}): value cannot be "
                        f"represented at the declared scale."
                    )
                if q != d:
                    # Numeric equality ignores trailing zeros (Decimal('1.230000') ==
                    # Decimal('1.23')) — a mismatch here means genuine digits were
                    # dropped, not just re-padded.
                    if safe:
                        return None
                    raise ValueError(
                        f"Cannot CAST {d} to DECIMAL(scale={scale}): value has more "
                        f"decimal places than the declared scale."
                    )
                return q
            result = [quantizer(d) for d in result]

        return _cast_result_to_draken(result, "DECIMAL", args)

    return _decimal_cast


def _build_array_cast(arr, element_type):
    """Build a closure for CAST to ARRAY(element_type)."""
    result = [_parse_array_value(i, element_type, safe_cast=False) for i in arr]
    return _draken_native_casts.vector_array_from_sequence(result)


def _build_vector_cast(arr):
    """Build a closure for CAST to VECTOR (FP16 quantization)."""
    caster = parser_for(LogicalCategory.VECTOR)
    result = [caster(i) for i in arr]
    return _draken_native_casts.vector_fp16_from_sequence(result)


def try_cast(target_type):
    """Factory: return a callable for safe casting to the target type.

    Used by tests and legacy code. Returns a callable that takes a sequence
    and returns a list of cast values (with None for parse failures).
    """
    def _try_cast_fn(arr):
        """Cast each element in arr, returning None on parse failures."""
        caster = parser_for(LogicalCategory[target_type])
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
