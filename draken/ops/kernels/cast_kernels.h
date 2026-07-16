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

VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* vector);
VecResult draken_cast_string_to_date32(void* ctx, const DrakenVector* vector);

VecResult draken_cast_integer_to_float64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_integer_to_string(void* ctx, const DrakenVector* vector);

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
VecResult draken_cast_date32_to_timestamp(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_rescale(void* ctx, const DrakenVector* vector);
// Implemented as draken_cast_date_to_string (the registered/forward-declared name).
VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* vector);

VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_to_date32(void* ctx, const DrakenVector* vector);
VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* vector);

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
