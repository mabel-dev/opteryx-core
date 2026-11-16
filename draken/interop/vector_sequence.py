"""Producer-side dispatcher: Python sequence → typed Draken Vector.

`vector_from_sequence(values, dtype=None)` is the single ingestion entry
point for Python-list-shaped data. It dispatches by `dtype` to the
appropriate typed constructor in `draken.draken_native`.

`dtype` may be:
- `None` — defaults to int64 (backward-compatible with the no-dtype call
  form used by callers that don't care about type, or for which int64
  is correct).
- a `str` naming the logical type (e.g. `"INTEGER"`, `"VARCHAR"`,
  `"DOUBLE"`).
- any object exposing a `.name` attribute (e.g. a `DrakenType` enum
  member). The `.name` is read as a string; the canonical caller form
  is `DrakenType.VARCHAR` from `draken.draken_native`.

The dispatch table maps type-name strings to nanobind constructors. It
is the canonical surface for "Python list → Vector" — direct callers of
the typed constructors are fine; this dispatcher exists for callers
working from a runtime type tag.

Names supported (case-insensitive, both common SQL-style and
draken-native spellings):
- INTEGER / INT64 / INT
- INT32, INT16, INT8
- DOUBLE / FLOAT64
- FLOAT / FLOAT32
- VARCHAR / STRING
- BOOLEAN / BOOL
- DATE / DATE32
- TIMESTAMP / TIMESTAMP64
- TIME / TIME32
- INTERVAL
- DECIMAL — uses default precision=18, scale=6 unless the caller passes
  precision/scale explicitly via `vector_decimal_from_sequence` directly.
- ARRAY

Type names not in the table raise `ValueError`. Per-type quirks
(decimal precision/scale, fp16 dimension, etc.) require calling the
typed `vector_*_from_sequence` function in `draken.draken_native`
directly — this dispatcher chooses defaults for the common path only.
"""

from draken import draken_native as _draken_native

# Each entry maps a logical type name → constructor function from
# draken_native. Names are stored uppercase; lookup normalises input.
_DTYPE_DISPATCH = {
    # Integers — INT64 is also what the bare no-dtype call form resolves to.
    "INT64": _draken_native.vector_int64_from_sequence,
    "INTEGER": _draken_native.vector_int64_from_sequence,
    "INT": _draken_native.vector_int64_from_sequence,
    "INT32": _draken_native.vector_int32_from_sequence,
    "INT16": _draken_native.vector_int16_from_sequence,
    "INT8": _draken_native.vector_int8_from_sequence,
    # Floats
    "FLOAT64": _draken_native.vector_float64_from_sequence,
    "DOUBLE": _draken_native.vector_float64_from_sequence,
    "FLOAT32": _draken_native.vector_float32_from_sequence,
    "FLOAT": _draken_native.vector_float32_from_sequence,
    # Strings
    "VARCHAR": _draken_native.vector_from_string_sequence,
    "STRING": _draken_native.vector_from_string_sequence,
    # Bytes
    "VARBINARY": _draken_native.vector_from_bytes_sequence,
    "BLOB": _draken_native.vector_from_bytes_sequence,
    # Booleans
    "BOOLEAN": _draken_native.vector_from_bool_sequence,
    "BOOL": _draken_native.vector_from_bool_sequence,
    # Temporals
    "DATE": _draken_native.vector_date32_from_sequence,
    "DATE32": _draken_native.vector_date32_from_sequence,
    "TIMESTAMP": _draken_native.vector_timestamp_from_sequence,
    "TIMESTAMP64": _draken_native.vector_timestamp_from_sequence,
    "TIME": _draken_native.vector_time32_from_sequence,
    "TIME32": _draken_native.vector_time32_from_sequence,
    "INTERVAL": _draken_native.vector_interval_from_sequence,
    # Unicode strings
    "NVARCHAR": _draken_native.vector_from_nvarchar_sequence,
    # Containers
    "ARRAY": _draken_native.vector_array_from_sequence,
}


def _encode_varchar_elements(values):
    """Encode str elements to UTF-8 bytes for the bytes-only VARCHAR edge.

    The VARCHAR sequence builder (`vector_from_string_sequence`) is bytes-only;
    a Python str must not reach the Draken edge. This is the single ingestion
    encode point so list-of-str callers keep working. None passes through.
    """
    out = []
    for v in values:
        if v is None or isinstance(v, bytes):
            out.append(v)
        elif isinstance(v, str):
            out.append(v.encode("utf-8"))
        else:
            raise TypeError(
                f"VARCHAR sequence element must be str, bytes, or None; "
                f"got {type(v).__name__}"
            )
    return out


def _resolve_dtype_name(dtype):
    """Coerce `dtype` to an uppercase string type name, or None for default."""
    if dtype is None:
        return None
    if isinstance(dtype, str):
        return dtype.upper()
    # Enum-like with .name attribute (e.g. DrakenType member).
    name = getattr(dtype, "name", None)
    if name is None:
        raise TypeError(
            f"vector_from_sequence: dtype must be None, str, or an object with "
            f"a .name attribute; got {type(dtype).__name__}"
        )
    return str(name).upper()


# DECIMAL and DECIMAL128 have no type without their precision and scale, so they are
# built only from the caller's descriptor (`logical`). There is no default: one used to
# build every DECIMAL as DECIMAL(18, 6), whatever the column was declared as.
_DECIMAL_DISPATCH = {
    "DECIMAL": _draken_native.vector_decimal_from_sequence,
    "DECIMAL128": _draken_native.vector_decimal128_from_sequence,
}


def vector_from_sequence(values, dtype=None, logical=None):
    """Build a Draken Vector from a Python sequence.

    Parameters
    ----------
    values : iterable
        A Python list (or list-coerceable iterable) of element values.
        `None` elements become null rows. The required Python element
        type depends on `dtype`; see `draken.draken_native` typed
        constructors for the per-type contract (e.g. `Decimal` for
        DECIMAL, `datetime.date` for DATE).
    dtype : None | str | DrakenType
        Physical type. `None` defaults to INT64. Pass a `DrakenType` enum
        member (from `draken.draken_native`) or a type-name string.
    logical : None | LogicalType
        The column's logical descriptor. REQUIRED for DECIMAL / DECIMAL128,
        whose precision and scale it carries; ignored for every other type.

    Returns
    -------
    A `draken.draken_native.Vector` of the requested type.

    Raises
    ------
    ValueError
        If `dtype` resolves to an unsupported type name, or is DECIMAL /
        DECIMAL128 with no `logical` descriptor.
    TypeError
        If `dtype` is non-None and not str/.name-bearing.

    Notes
    -----
    FP16 requires `dimension` and is not available through this dispatcher.
    """
    # Materialise iterables to a list — draken constructors expect a list.
    if not isinstance(values, list):
        values = list(values)

    type_name = _resolve_dtype_name(dtype)

    if type_name is None:
        return _draken_native.vector_int64_from_sequence(values)

    decimal_ctor = _DECIMAL_DISPATCH.get(type_name)
    if decimal_ctor is not None:
        if logical is None:
            raise ValueError(
                f"vector_from_sequence: a {type_name} vector needs its precision and "
                "scale - pass the column's descriptor as `logical`"
            )
        return decimal_ctor(values, int(logical.precision), int(logical.scale))

    # The VARCHAR/NVARCHAR sequence edge is bytes-only — encode str elements once
    # here (the NVARCHAR builder shares the bytes-only string-from-sequence path).
    if type_name in ("VARCHAR", "STRING", "NVARCHAR"):
        values = _encode_varchar_elements(values)

    ctor = _DTYPE_DISPATCH.get(type_name)
    if ctor is None:
        raise ValueError(
            f"vector_from_sequence: unsupported dtype name {type_name!r}. "
            f"Supported: {sorted(_DTYPE_DISPATCH.keys()) + sorted(_DECIMAL_DISPATCH)}"
        )
    return ctor(values)
