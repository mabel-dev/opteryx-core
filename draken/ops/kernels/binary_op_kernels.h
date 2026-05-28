#pragma once

/**
 * BC_BINARY_OP kernel ABI — Phase 9a.
 *
 * All binary op kernels follow the Decision 3 signature:
 *   VecResult (*)(void* ctx, const DrakenVector* left, const DrakenVector* right)
 *
 * Context is used for:
 * - Arithmetic: ctx → binary_op_ctx{op_code} to dispatch to add/sub/mul/div/mod.
 * - Bitwise: ctx is NULL (operation determined at bind time, not exec time).
 * - Temporal: ctx → binary_op_ctx{BOP_PLUS or BOP_MINUS} for ± interval.
 * - Other: ctx is NULL.
 *
 * Error handling: On error, return VecResult with data == nullptr (sentinel).
 */

#include "ops/vec_result.h"
#include "core/buffers.h"
#include "ops/kernels/kernel_context.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Arithmetic Operators (vector op vector) — op_code in ctx
 *
 * Note: These call into draken's existing arithmetic fast path
 * (draken_arithmetic). Fallback path for mixed types, DECIMAL, etc.
 * ========================================================================== */

/**
 * Binary arithmetic dispatch: add, sub, mul, div, mod.
 * ctx → binary_op_ctx{op_code} where op_code is one of:
 *   BOP_PLUS (0), BOP_MINUS (1), BOP_MULTIPLY (2), BOP_DIVIDE (3), BOP_MODULO (4)
 * Dispatches internally to the right operation.
 *
 * Replaces the _build_arithmetic_closure that currently does
 * getattr(left, method_name)(right).
 *
 * Returns result vector or error sentinel.
 */
VecResult draken_binary_arith(void* ctx, const DrakenVector* left, const DrakenVector* right);

/* ============================================================================
 * Individual Arithmetic Operations — exposed separately for clarity
 * (optional, can be called directly if preferred over dispatcher)
 * ========================================================================== */

VecResult draken_add(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_subtract(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_multiply(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_divide(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_modulo(void* ctx, const DrakenVector* left, const DrakenVector* right);

/* ============================================================================
 * Bitwise Operators — ctx is NULL
 * ========================================================================== */

VecResult draken_bitwise_or(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_bitwise_and(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_bitwise_xor(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_bitwise_shift_left(void* ctx, const DrakenVector* left, const DrakenVector* right);
VecResult draken_bitwise_shift_right(void* ctx, const DrakenVector* left, const DrakenVector* right);

/* ============================================================================
 * String Operations
 * ========================================================================== */

/**
 * String concatenation: coerce both operands to VARCHAR, then concat.
 * ctx is NULL.
 * Returns VARCHAR (StringVector) or error.
 */
VecResult draken_string_concat(void* ctx, const DrakenVector* left, const DrakenVector* right);

/* ============================================================================
 * Temporal Operations — arithmetic with intervals
 *
 * DATE/TIMESTAMP ± INTERVAL, DATE - DATE, INTERVAL ± INTERVAL
 * ctx → binary_op_ctx{BOP_PLUS or BOP_MINUS}
 * ========================================================================== */

/**
 * Date/Timestamp ± Interval dispatch.
 * ctx → binary_op_ctx with op_code (BOP_PLUS or BOP_MINUS).
 * Handles: (DATE/TIMESTAMP ± INTERVAL) or (INTERVAL ± DATE/TIMESTAMP).
 * Returns result vector or error.
 */
VecResult draken_temporal_interval_op(void* ctx, const DrakenVector* left, const DrakenVector* right);

/**
 * Date - Date: compute interval between dates.
 * ctx → binary_op_ctx with op_code = BOP_MINUS.
 * Returns INTERVAL vector or error.
 */
VecResult draken_date_minus_date(void* ctx, const DrakenVector* left, const DrakenVector* right);

/**
 * Interval ± Interval.
 * ctx → binary_op_ctx with op_code (BOP_PLUS or BOP_MINUS).
 * Returns INTERVAL vector or error.
 */
VecResult draken_interval_interval_op(void* ctx, const DrakenVector* left, const DrakenVector* right);

/* ============================================================================
 * IP Address Operations
 * ========================================================================== */

/**
 * IP address in CIDR block check.
 * ctx is NULL.
 * Special case: BOP_BITWISE_OR on VARCHAR operands becomes IP-in-CIDR.
 * Returns BOOL (BoolVector) or error.
 */
VecResult draken_ip_in_cidr(void* ctx, const DrakenVector* left, const DrakenVector* right);

#ifdef __cplusplus
}
#endif
