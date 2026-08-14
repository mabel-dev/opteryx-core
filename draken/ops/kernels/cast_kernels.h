#pragma once

/**
 * BC_CAST kernel ABI — Phase 9a.
 *
 * All cast kernels follow the Decision 3 signature:
 *   VecResult (*)(void* ctx, const DrakenVector* vector)
 *
 * Context (ctx) is NULL for simple casts; non-NULL for parameterized casts
 * (e.g., TIMESTAMP[unit], DECIMAL[p,s]). See kernel_context.h for struct definitions.
 *
 * Error handling: C++ exceptions are caught at the boundary. On error, return
 * a VecResult with data == nullptr (sentinel) and set thread-local error string
 * (same pattern as DV fast paths arena).
 */

#include "ops/vec_result.h"
#include "core/buffers.h"
#include "ops/kernels/kernel_context.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Numeric Casts (int64 ↔ float64, bool, string, etc.)
 * ========================================================================== */

VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_int64_to_bool(void* ctx, const DrakenVector* vector);
VecResult draken_cast_int64_to_timestamp(void* ctx, const DrakenVector* vector);

VecResult draken_cast_bool_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_int64(void* ctx, const DrakenVector* vector);

VecResult draken_cast_float64_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float64_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float64_to_bool(void* ctx, const DrakenVector* vector);

// DECIMAL → VARCHAR. Source scale (LogicalType, not on the vector) rides in a
// binary_op_ctx.left_scale supplied by the binder. Two physical tiers:
// draken_cast_decimal_to_string    — DRAKEN_DECIMAL    (int64 unscaled, p≤18)
// draken_cast_decimal128_to_string — DRAKEN_DECIMAL128 (int128 unscaled, p≤38)
VecResult draken_cast_decimal_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_string(void* ctx, const DrakenVector* vector);

// → VARBINARY (BLOB) thin retag wrappers: same formatted bytes as the `_to_string`
// twin above, with the result retagged VARBINARY (VARCHAR and VARBINARY share the
// identical DrakenStringArena layout — buffers.h §11). Fixes a prior mistagging
// bug where numeric/bool/decimal -> VARBINARY casts dispatched straight to the
// `_to_string` kernel and silently came back tagged VARCHAR.
VecResult draken_cast_int64_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float64_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_blob(void* ctx, const DrakenVector* vector);

VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_date32(void* ctx, const DrakenVector* vector);

/** CAST(<string> AS IPV4): dotted-decimal -> UINT32. Strict parse; invalid raises.
 *  Result carries NO descriptor — IPv4-ness is re-attached from the bound output
 *  type via add_expr_project's `logical` tuple. */
VecResult draken_cast_string_to_ipv4(void* ctx, const DrakenVector* vector);

/** CAST(<ipv4> AS VARCHAR / VARBINARY): UINT32 -> dotted-decimal, via the one
 *  renderer in draken/core/ipv4.h. Cannot fail.
 *
 *  The SOURCE discriminant is the bound ColumnType's LogicalKind, NOT the
 *  physical tag: a plain unsigned column is DRAKEN_UINT32 too and must render
 *  its integer through draken_cast_uint_to_string. A DrakenVector carries no
 *  descriptor, so the choice is made at bind time (opteryx/expression/casts.pyx)
 *  and picking the wrong one here is a silent wrong-answer bug, not an error. */
VecResult draken_cast_ipv4_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_ipv4_to_blob(void* ctx, const DrakenVector* vector);

// String-family retag: VARCHAR/NVARCHAR/VARBINARY -> VARCHAR or -> VARBINARY.
// All three share the exact DrakenStringArena byte layout (buffers.h), so this
// is a byte-identical copy that only changes the type tag — no validation, no
// reformatting (NVARCHAR source bytes are always valid arbitrary bytes; VARCHAR/
// VARBINARY source bytes are passed through unchecked, matching the documented
// "undefined behaviour for non-ASCII VARCHAR" contract).
VecResult draken_cast_string_to_varchar(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_blob(void* ctx, const DrakenVector* vector);

// -> NVARCHAR: validates UTF-8 per row (RAISES on the first invalid row), then
// retags via string_retag_core. Plain CAST only — TRY_CAST stays on the
// Python closure path (`_c_native_cast` returns None for safe=True).
VecResult draken_cast_string_to_nvarchar(void* ctx, const DrakenVector* vector);

VecResult draken_cast_integer_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_string(void* ctx, const DrakenVector* vector);

// Unsigned source (UINT8/16/32/64) -> VARCHAR / VARBINARY, read at the source's
// native stride. Separate from the signed entry points because a UINT64 above
// INT64_MAX would print negative through them.
VecResult draken_cast_uint_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_blob(void* ctx, const DrakenVector* vector);

// E33 — any signed integer source (INT8/16/32/64) -> the named unsigned target,
// range-checked (negative or out-of-range magnitude raises, never truncates/wraps).
VecResult draken_cast_integer_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_uint64(void* ctx, const DrakenVector* vector);

// E33 — any unsigned source (UINT8/16/32/64) -> INT64, range-checked (a UINT64
// value > INT64_MAX raises rather than wrapping to negative).
VecResult draken_cast_uint_to_int64(void* ctx, const DrakenVector* vector);

// E33 — FLOAT64/FLOAT32 -> the named unsigned target, range-checked (negative,
// NaN, or out-of-range magnitude raises; truncates toward zero otherwise).
VecResult draken_cast_float_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_uint64(void* ctx, const DrakenVector* vector);

// E33 — BOOL -> the named unsigned target (always 0/1, no range check needed).
VecResult draken_cast_bool_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_uint64(void* ctx, const DrakenVector* vector);

// E33 — VARCHAR/NVARCHAR/VARBINARY -> the named unsigned target (parse +
// range-check; malformed digits or out-of-range values raise).
VecResult draken_cast_string_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_uint64(void* ctx, const DrakenVector* vector);

VecResult draken_cast_date32_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_decimal(void* ctx, const DrakenVector* vector);

// DECIMAL -> DECIMAL, named by the SOURCE tier (int64-backed / int128-backed); the
// TARGET tier comes from binary_op_ctx.result_precision (>18 -> int128). ctx also
// carries the source scale in left_scale and the target scale in result_scale — the
// vector itself has neither. Widening overflow and inexact narrowing both raise.
VecResult draken_cast_decimal_to_decimal(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_decimal(void* ctx, const DrakenVector* vector);

// INTEGER -> DECIMAL, named by the SOURCE width (int64 / narrow signed / any
// unsigned); one core behind all three. An integer is a decimal at scale 0, so the
// payload is value * 10^result_scale. Target tier from binary_op_ctx.result_precision
// (>18 -> int128); an out-of-range value raises rather than wrapping.
VecResult draken_cast_int64_to_decimal(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_decimal(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_decimal(void* ctx, const DrakenVector* vector);

// BOOL -> DECIMAL: true is the decimal 1, false the decimal 0, i.e. the integer
// promotion above at scale 0. Own loop rather than an int_to_decimal_core arm
// because the BOOL payload is bit-packed. `true` overflows when the target scale
// reaches its precision (DECIMAL(1,1) cannot hold 1) and raises there.
VecResult draken_cast_bool_to_decimal(void* ctx, const DrakenVector* vector);

// STRING -> DECIMAL: exact text -> fixed-point (NOT via double, which would lose
// the low digits DECIMAL exists to keep). Accepts optional sign, a decimal point,
// and exponent notation, matching the literal path's decimal.Decimal() syntax.
// Fractional digits past the declared scale, and magnitudes past the declared
// precision, raise rather than round or wrap. Defined in cast_string.cpp.
VecResult draken_cast_string_to_decimal(void* ctx, const DrakenVector* vector);

// DECIMAL -> INT64 / FLOAT64, named by the SOURCE tier. ctx reads ONLY
// binary_op_ctx.left_scale (the source scale, which the vector does not carry).
// INT64 truncates toward zero, matching draken_cast_float64_to_int64; an
// out-of-INT64-range magnitude raises.
VecResult draken_cast_decimal_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_float64(void* ctx, const DrakenVector* vector);

// Unsigned (UINT8/16/32/64) -> the named unsigned width, range-checked. The
// unsigned counterpart of the draken_cast_integer_to_uint* family, which takes
// signed sources only — without these an unsigned column could not change width
// at all. The uint32 member also serves CAST(<unsigned> AS IPV4).
VecResult draken_cast_uint_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_uint64(void* ctx, const DrakenVector* vector);

// Unsigned -> FLOAT64. No range check (every uint64 is representable; >2^53 loses
// low bits, which is floating point, not an error). The only route to float for
// the top half of the UINT64 range — via INT64 it raises.
VecResult draken_cast_uint_to_float64(void* ctx, const DrakenVector* vector);

// ---- Narrow signed (INT8/INT16/INT32) and FLOAT32 targets ----------------------
// These widths were SOURCE-only until the cast targets were opened up; the source
// families mirror the unsigned family exactly (signed int, unsigned int, float,
// bool, string, decimal). Every narrowing is range-checked and raises.
VecResult draken_cast_integer_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_int32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_int32(void* ctx, const DrakenVector* vector);
// <integer> -> DATE32: the int32 narrowing with the temporal tag. A DATE32 is an
// int32 days-since-epoch, and the source integer is taken to already hold days
// (see the note at the instantiations in cast_numeric.cpp).
VecResult draken_cast_integer_to_date32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_date32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_int32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_int32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_int32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_int8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_int16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_int32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_int32(void* ctx, const DrakenVector* vector);

// -> FLOAT32. Precision loss is the type's contract, not an error; a finite value
// with no float32 representation at all (would become +-Inf) raises. An input that
// is already +-Inf/NaN passes through.
VecResult draken_cast_float_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_uint_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_float_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_bool_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_float32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_float32(void* ctx, const DrakenVector* vector);

// DECIMAL -> the named unsigned width, named by the SOURCE tier. Source scale in
// binary_op_ctx.left_scale; truncates toward zero like the INT64 twin, then
// range-checks (negative or over-width raises).
VecResult draken_cast_decimal_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_uint8(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_uint16(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_uint32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal_to_uint64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_decimal128_to_uint64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_date32_to_timestamp(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_rescale(void* ctx, const DrakenVector* vector);
// Implemented as draken_cast_date_to_string (the registered/forward-declared name).
VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* vector);

VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_to_date32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* vector);

// VARCHAR/NVARCHAR/VARBINARY -> TIMESTAMP64. ctx (format_ctx*) null/fmt_len==0 ->
// strict ISO-8601 parse; ctx->fmt_len>0 -> FORMAT-driven parse (CAST ... FORMAT).
// Always produces microsecond-unit TIMESTAMP64.
VecResult draken_cast_string_to_timestamp(void* ctx, const DrakenVector* vector);

// INTERVAL -> VARCHAR. ctx (format_ctx*) null/fmt_len==0 -> ISO-8601 duration
// default ("P1DT2H30M"); ctx->fmt_len>0 -> FORMAT tokens as duration magnitudes.
VecResult draken_cast_interval_to_string(void* ctx, const DrakenVector* vector);
VecResult draken_cast_interval_to_blob(void* ctx, const DrakenVector* vector);

// VARCHAR/NVARCHAR/VARBINARY -> TIME64 (int64 microseconds-since-midnight).
// Parses "HH:MM:SS[.ffffff]"; raises on malformed input.
VecResult draken_cast_string_to_time64(void* ctx, const DrakenVector* vector);
// TIME64 -> VARCHAR: "HH:MM:SS.ffffff" (15 chars, extern).
VecResult draken_cast_time_to_string(void* ctx, const DrakenVector* vector);

// → VARBINARY (BLOB) thin retag wrappers — see the matching comment near the
// numeric _to_blob declarations above.
VecResult draken_cast_date_to_blob(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_to_blob(void* ctx, const DrakenVector* vector);

/* ============================================================================
 * Dispatch Helpers — C implementations of cast_to_* closures.
 *
 * These kernels dispatch to the appropriate native cast based on input type.
 * They replace the Python row-loop closures in opteryx/expression/casts.pyx.
 * ========================================================================== */

/**
 * Cast any numeric/string type to FLOAT64.
 * Dispatches to the appropriate cast_*_to_float64 kernel based on input type.
 * Returns FLOAT64 vector or error.
 */
VecResult draken_cast_to_float64(void* ctx, const DrakenVector* vector);

/**
 * Cast any numeric/string type to INT64.
 * Dispatches to the appropriate cast_*_to_int64 kernel based on input type.
 * Returns INT64 vector or error.
 */
VecResult draken_cast_to_int64(void* ctx, const DrakenVector* vector);

/**
 * Cast any numeric/string/array type to VARCHAR.
 * Dispatches based on input type. Handles array → JSON string conversion.
 * Returns VARCHAR (StringVector) or error.
 */
VecResult draken_cast_to_varchar(void* ctx, const DrakenVector* vector);

/**
 * Cast any numeric/string type to BOOL.
 * Dispatches based on input type.
 * Returns BOOL (BoolVector) or error.
 */
VecResult draken_cast_to_bool(void* ctx, const DrakenVector* vector);

/**
 * Cast any numeric/string/temporal type to DATE32.
 * Dispatches based on input type.
 * Returns DATE32 vector or error.
 */
VecResult draken_cast_to_date(void* ctx, const DrakenVector* vector);

/* ============================================================================
 * Parameterized Casts — require context struct
 * ========================================================================== */

/**
 * Cast to DECIMAL(precision, scale).
 * ctx → cast_decimal_ctx with precision and scale.
 * Returns DECIMAL vector or error. (Likely row-loop in initial phase.)
 */
VecResult draken_cast_to_decimal(void* ctx, const DrakenVector* vector);

/**
 * Cast to ARRAY(element_type).
 * ctx → cast_array_ctx with element_type.
 * Returns ARRAY vector or error.
 */
VecResult draken_cast_to_array(void* ctx, const DrakenVector* vector);

/**
 * Cast to VECTOR (FP16 embedding).
 * Returns VECTOR vector or error.
 */
VecResult draken_cast_to_vector(void* ctx, const DrakenVector* vector);

/**
 * Cast to VARCHAR(length).
 * ctx → cast_varchar_ctx with max_length.
 * Returns VARCHAR (StringVector) with length enforcement, or error.
 */
VecResult draken_cast_to_varchar_with_length(void* ctx, const DrakenVector* vector);

/* ============================================================================
 * Passthrough / Identity Cast
 * ========================================================================== */

/**
 * No-op cast: source type == target type.
 * Returns the input vector unchanged.
 */
VecResult draken_cast_identity(void* ctx, const DrakenVector* vector);

/**
 * ARRAY -> VARCHAR rendering: ['a', 'b'] per row. TWO-vector signature — the
 * elements live in the parent's child vector (owned by the VectorOwner, not
 * reachable from the parent DrakenVector*), so the VM resolves and passes it
 * explicitly (BC_C_NATIVE_CHILD). Never dispatched through the one-vector
 * cast table.
 */
VecResult draken_cast_array_to_varchar(void* ctx, const DrakenVector* parent,
                                       const DrakenVector* child);

#ifdef __cplusplus
}
#endif
