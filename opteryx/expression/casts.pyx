"""CAST resolution: which native kernel runs a (source, target) pair.

`_c_native_cast` is the single source of truth; a pair it does not name has no
kernel and is refused when the expression is lowered. The Python cast closures
(resolve_cast and its builders) went with the Python evaluator.
"""

import datetime
import math

from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import parse_column_type
from opteryx.types.scalars.value_parsing import parse_value, parser_for


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


_CAST_STRINGS = ("VARCHAR", "NVARCHAR", "VARBINARY")
_CAST_NARROW_INT = ("INT8", "INT16", "INT32")
_CAST_UNSIGNED_INT = ("UINT8", "UINT16", "UINT32", "UINT64")  # E33


def _c_native_cast(source_physical, target_type, bint safe=False, bint source_is_ipv4=False):
    """Return (c_kernel_name, ctx_unit_code) for casts that have a REAL, REGISTERED
    C-ABI kernel the executor can dispatch zero-Python via BC_INSTR_C_NATIVE, or
    None — no kernel, so the cast is refused.

    The set grows as kernels become real+registered. This table is the single
    source of truth for which casts run C-native.

    `safe` (TRY_CAST) NO LONGER changes what this table returns. It used to force
    None, on the theory that TRY_CAST would fall back to a closure — but this
    engine has no closure fallback, so that made TRY_CAST unrunnable on a column
    for EVERY target. The disposition now rides in the kernel's ctx
    (binary_op_ctx.safe, or format_ctx.safe for the two pattern-parsing kernels,
    or cast_array_ctx.safe for ARRAY), so ONE kernel serves both dispositions and
    a raise and a NULL can never disagree about what "converts" means.

    A cast that CANNOT fail (a widening, a bool source, a retag) ignores the flag
    entirely — TRY_CAST over one is just a cast, which is correct.

    `source_is_ipv4` is the IPv4 SOURCE discriminant, and it is NOT derivable from
    `source_physical`: an IPv4 column is DRAKEN_UINT32 refined by a
    LogicalKind.IPV4 descriptor, so it and a plain unsigned column arrive here
    under the same name, "UINT32". The caller reads the descriptor off the bound
    source ColumnType and passes it explicitly. It is a separate argument rather
    than a synthetic "IPV4" source name because every other pairing (IPV4 →
    INTEGER, → UINT32, → BOOLEAN) is correct on the raw uint32 and must keep
    matching the unsigned arms.
    """
    cdef str s = source_physical
    cdef str t = "BLOB" if target_type == "VARBINARY" else target_type
    # ---- ARRAY target ----
    # The first pair to take its disposition through the ctx, and the model the
    # rest now follow: `safe` rides in cast_array_ctx.
    if t == "ARRAY":
        if s in _CAST_STRINGS or s == "VARIANT":
            return ("draken_cast_to_array", 0)
        # Every other source (a number, a bool, a temporal) is NOT castable to
        # ARRAY. Returning None here is a plan-time refusal, not a fallback.
        return None
    if s is None:
        return None
    # FLOAT32 is NOT in this tuple: it is a narrower type with its own kernels.
    # Routing it here would declare FLOAT32 and produce FLOAT64.
    if t in ("DOUBLE", "FLOAT", "FLOAT64"):
        # FLOAT32 → FLOAT64 is a WIDENING, not a retag: the payloads are 4 and 8
        # bytes wide, so it must run a kernel. It was previously listed as an
        # identity passthrough in resolve_cast, which the gate then refused —
        # the refusal was the only thing standing between that entry and a
        # 4-byte buffer read at an 8-byte stride.
        if s == "FLOAT32":
            return ("draken_cast_float_to_float64", 0)
        if s == "INT64":
            return ("draken_cast_int64_to_float64", 0)
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_float64", 0)
        if s == "BOOL":
            return ("draken_cast_bool_to_float64", 0)
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_float64", 0)
        # The source SCALE rides in binary_op_ctx.left_scale — a decimal vector
        # does not carry it, so this pair is ctx-bearing like decimal→string.
        if s in ("DECIMAL", "DECIMAL128"):
            return (f"draken_cast_{s.lower()}_to_float64", 0)
        # Unsigned → float, direct. NOT via INT64: that route raises above
        # 2^63-1, which left the top half of the UINT64 range with no way into
        # float arithmetic at all.
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_float64", 0)
        return None
    # INT8/INT16/INT32 are NOT in this tuple. They used to be, which is exactly
    # why they could not be cast TARGETS: every one of them would have dispatched
    # an int64-PRODUCING kernel, declaring INT32 and producing INT64.
    if t in ("INTEGER", "BIGINT", "INT64"):
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
        # Truncates toward zero (draken_cast_float64_to_int64's convention), and
        # takes the source scale in binary_op_ctx.left_scale.
        if s in ("DECIMAL", "DECIMAL128"):
            return (f"draken_cast_{s.lower()}_to_int64", 0)
        return None
    # Narrow signed target (INT8/INT16/INT32). Source families mirror the unsigned
    # arm below exactly; every narrowing is range-checked in the kernel. The
    # same-width case (INT32 → INT32) is a copy through the same kernel rather
    # than a special case — its range check is trivially satisfied.
    if t in ("INT8", "INT16", "INT32"):
        if s in _CAST_NARROW_INT or s == "INT64":
            return (f"draken_cast_integer_to_{t.lower()}", 0)
        if s in _CAST_UNSIGNED_INT:
            return (f"draken_cast_uint_to_{t.lower()}", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return (f"draken_cast_float_to_{t.lower()}", 0)
        if s == "BOOL":
            return (f"draken_cast_bool_to_{t.lower()}", 0)
        if s in _CAST_STRINGS:
            return (f"draken_cast_string_to_{t.lower()}", 0)
        if s in ("DECIMAL", "DECIMAL128"):
            return (f"draken_cast_{s.lower()}_to_{t.lower()}", 0)
        return None
    # FLOAT32 target. Precision loss is the type's contract; only a finite value
    # with no float32 representation at all raises (see the kernel).
    if t == "FLOAT32":
        if s in _CAST_NARROW_INT or s == "INT64":
            return ("draken_cast_integer_to_float32", 0)
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_float32", 0)
        if s in ("FLOAT64", "FLOAT32"):
            return ("draken_cast_float_to_float32", 0)
        if s == "BOOL":
            return ("draken_cast_bool_to_float32", 0)
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_float32", 0)
        if s in ("DECIMAL", "DECIMAL128"):
            return (f"draken_cast_{s.lower()}_to_float32", 0)
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
        # Unsigned → unsigned, every width pairing. Widenings and the same-width
        # copy cannot fail; narrowings are range-checked. Without these an
        # unsigned column could not change width at all — the signed family
        # above rejects an unsigned source outright.
        if s in _CAST_UNSIGNED_INT:
            return (f"draken_cast_uint_to_{t.lower()}", 0)
        # DECIMAL → unsigned: truncates toward zero like the INTEGER target,
        # then range-checks. Ctx-bearing (source scale), as every decimal-source
        # kernel is.
        if s in ("DECIMAL", "DECIMAL128"):
            return (f"draken_cast_{s.lower()}_to_{t.lower()}", 0)
        return None
    if t in ("DATE", "DATE32"):
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_date32", 0)
        # <integer> → DATE: the integer already holds days-since-epoch, which is
        # exactly DATE32's own storage, so this is the int32 narrowing with the
        # temporal tag (range-checked in the kernel, never wrapped). It is the
        # same reading the planner's CAST-predicate rewrite asserts
        # (_try_normalize_cast_predicate), and the two MUST agree — otherwise a
        # `col::DATE >= <date>` that the optimizer rewrote and one it left alone
        # would return different rows.
        if s in _CAST_NARROW_INT or s == "INT64":
            return ("draken_cast_integer_to_date32", 0)
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_date32", 0)
        # TIMESTAMP64 → DATE32: truncates the time component. The source's
        # TimestampUnit rides in binary_op_ctx.left_unit (compiled_expression.pyx
        # allocates it), since the raw int64 payload's scale depends on it.
        if s == "TIMESTAMP64":
            return ("draken_cast_timestamp_to_date32", 0)
        return None
    # IPv4. The kernel yields UINT32; the IPV4 descriptor is re-attached from the
    # bound output type via add_expr_project's `logical` tuple, not from the
    # kernel result — VecResult has no descriptor channel and does not need one.
    if t == "IPV4":
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_ipv4", 0)
        # An IPv4 address IS a uint32, so every integer width reaches it through
        # the range-checked narrowing to UINT32 — the descriptor is then attached
        # from the bound output type. `192.168.1.1` and `3232235777` are the same
        # address, and refusing the integer spelling made the address arithmetic
        # people actually do (store as INT64, render as an address) impossible.
        #
        # Range-checked, never wrapped: a negative INT64 or a value above
        # 2^32-1 is not an address and raises. UINT32 → IPV4 goes through the
        # unsigned kernel too — it is a width-preserving copy there, which costs
        # one pass and keeps this a normal, gate-admissible instruction rather
        # than a special "no kernel runs" case the compiler had no way to honour
        # (it refused the query outright).
        if s in _CAST_NARROW_INT or s == "INT64":
            return ("draken_cast_integer_to_uint32", 0)
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_uint32", 0)
        return None
    # UUID (docs/UUID_TYPE_DESIGN.md U6): text parses strictly, VARBINARY is the raw
    # 16 bytes. No numeric source.
    if t == "UUID":
        if s in ("VARCHAR", "NVARCHAR"):
            return ("draken_cast_string_to_uuid", 0)
        if s == "VARBINARY":
            return ("draken_cast_blob_to_uuid", 0)
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
        # DECIMAL → DECIMAL rescale. Named by the SOURCE tier; the TARGET tier is the
        # ctx precision (>18 → int128), so one name covers both destinations. The
        # source scale rides in binary_op_ctx.left_scale, exactly like the
        # `_to_string` decimal arm below — the vector carries no scale at all.
        if s == "DECIMAL":
            return ("draken_cast_decimal_to_decimal", 0)
        if s == "DECIMAL128":
            return ("draken_cast_decimal128_to_decimal", 0)
        # INTEGER → DECIMAL — an integer is a decimal at scale 0. Reached whenever a
        # DECIMAL blend has an integer COLUMN branch (CASE, UNION-leg coercion) as
        # well as by an explicit CAST; integer LITERALS never get here, the binder
        # retypes those in place.
        if s == "INT64":
            return ("draken_cast_int64_to_decimal", 0)
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_decimal", 0)
        if s in _CAST_UNSIGNED_INT:
            return ("draken_cast_uint_to_decimal", 0)
        # BOOL → DECIMAL — true is 1, false is 0, the same integer promotion BOOL
        # already has to INT64/FLOAT64/the narrow and unsigned families. DECIMAL was
        # the only hole in that row, and it surfaced through UNION leg coercion:
        # find_compatible_type promotes BOOL + DECIMAL to DECIMAL, so the bound CAST
        # had no kernel and the query died at the c-native admission gate.
        if s == "BOOL":
            return ("draken_cast_bool_to_decimal", 0)
        # STRING → DECIMAL parses the text EXACTLY (its own integer accumulation, not
        # a detour through draken_cast_string_to_float64 — a double cannot hold 18
        # significant digits, so composing the two would silently corrupt the low
        # digits of the values DECIMAL exists to keep).
        if s in _CAST_STRINGS:
            return ("draken_cast_string_to_decimal", 0)
        return None
    # → VARCHAR / BLOB (string result; executor owns it as a Vector). NVARCHAR as
    # a TARGET is handled separately below (validate+retag, a different kernel);
    # NVARCHAR as a SOURCE is fine here (its bytes are always valid VARCHAR/
    # VARBINARY bytes, no validation needed — see string_retag_core).
    if t in ("VARCHAR", "BLOB"):
        if s in _CAST_STRINGS or s == "VARIANT":
            # VARCHAR/NVARCHAR/VARBINARY/VARIANT share the exact DrakenStringArena
            # byte layout (buffers.h §11 / draken_type_is_string_storage) — this is
            # a retag, not a reformat.
            return ("draken_cast_string_to_blob" if t == "BLOB"
                    else "draken_cast_string_to_varchar", 0)
        # Below: numeric/bool/decimal/temporal sources format to the identical
        # ASCII bytes for either target — the `_to_blob` twin (t == "BLOB") just
        # retags the `_to_string` kernel's result rather than reformatting (see
        # the DRAKEN_CAST_TO_BLOB doc comment in cast_numeric.cpp). Returning the
        # plain `_to_string` name here for a BLOB target would silently mistag
        # the result VARCHAR — that was the bug.
        # IPv4 renders dotted-decimal ('192.168.1.1'), a plain unsigned renders its
        # integer ('3232235777'). Both sources are physically UINT32, so this arm
        # MUST sit ahead of the unsigned arm below and MUST key on the descriptor —
        # the kernels cannot tell the two apart (a DrakenVector carries no
        # descriptor), so getting the order or the key wrong here is a silent
        # wrong-answer bug rather than an error.
        if s == "UINT32" and source_is_ipv4:
            return ("draken_cast_ipv4_to_blob" if t == "BLOB" else "draken_cast_ipv4_to_string", 0)
        if s == "UUID":
            return ("draken_cast_uuid_to_blob" if t == "BLOB" else "draken_cast_uuid_to_string", 0)
        if s == "INT64":
            return ("draken_cast_int64_to_blob" if t == "BLOB" else "draken_cast_int64_to_string", 0)
        if s in _CAST_NARROW_INT:
            return ("draken_cast_integer_to_blob" if t == "BLOB" else "draken_cast_integer_to_string", 0)
        if s in _CAST_UNSIGNED_INT:
            # One kernel for all four widths — the stride comes from the vector's
            # type tag. Reached only for a descriptor-less unsigned column.
            return ("draken_cast_uint_to_blob" if t == "BLOB" else "draken_cast_uint_to_string", 0)
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
            # No `_to_blob` twin kernel exists yet — VARCHAR only (a BLOB target is
            # refused rather than silently mistagging the VARCHAR-formatted result,
            # per the DECIMAL/DATE/TIMESTAMP comment above).
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
    # string-family sources are native (including VARIANT, whose JSON text is
    # already-valid Unicode by the JSON spec); a non-string source (INT64 etc.)
    # first needs VARCHAR formatting, so it is refused (matches this function's
    # "no chained native kernels" posture elsewhere).
    if t == "NVARCHAR":
        if s in _CAST_STRINGS or s == "VARIANT":
            return ("draken_cast_string_to_nvarchar", 0)
        return None
    return None


def try_cast(target_type):
    """Factory: return a callable for safe casting to the target type.

    Used by tests and legacy code. Returns a callable that takes a sequence
    and returns a list of cast values (with None for parse failures).
    """
    def _try_cast_fn(arr):
        """Cast each element in arr, returning None on parse failures."""
        # `target_type` is a SQL type name, which is NOT the same vocabulary as
        # LogicalCategory's member names: indexing the enum directly meant DOUBLE
        # and STRUCT — both perfectly valid in a CAST — died with a bare
        # KeyError before a single value was looked at. Category names are still
        # accepted first (DECIMAL is a category but not a bare SQL type: it needs
        # precision/scale, so parse_column_type rejects it), then the name is
        # resolved through the same alias table the planner uses, so DOUBLE
        # resolves to FLOAT64 and STRUCT to NVARCHAR exactly as CAST does.
        if target_type in LogicalCategory.__members__:
            category = LogicalCategory[target_type]
        else:
            category = parse_column_type(target_type).category
        caster = parser_for(category)
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
