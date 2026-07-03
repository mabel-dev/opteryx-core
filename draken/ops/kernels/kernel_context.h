#pragma once

/**
 * Kernel context structures for parameterized kernels in Phase 9.
 *
 * Context structs are passed as void* ctx to C ABI kernels. Each kernel family
 * that needs parameterization (cast unit, binary op code, etc.) has a context
 * struct defined here. Context lifetime ≥ CompiledBytecode; held in _held_refs.
 */

#include <cstdint>
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Context for BC_CAST with parameterized unit (TIMESTAMP[ns], TIMESTAMP[us], etc.).
 * Used by vector_cast_int64_to_timestamp when unit is specified.
 */
struct cast_timestamp_ctx {
    int unit;  // 0=none, 1=ns, 2=us, 3=ms, 4=s, 5=days
};

/**
 * Context for BC_CAST with DECIMAL(precision, scale) constraint.
 * Used by _decimal_cast kernels.
 */
struct cast_decimal_ctx {
    int32_t precision;  // e.g., 38
    int32_t scale;      // e.g., 6
};

/**
 * Context for BC_CAST to ARRAY(element_type).
 * Stores the target element type for array cast.
 */
struct cast_array_ctx {
    int element_type;  // DrakenType enum value
};

/**
 * Context for BC_CAST to VARCHAR(length).
 * Stores the max length constraint.
 */
struct cast_varchar_ctx {
    int32_t max_length;
};

/**
 * Context for BC_BINARY_OP arithmetic operations.
 * Stores the operation code (BOP_PLUS, BOP_MINUS, etc.) to dispatch.
 */
struct binary_op_ctx {
    int op_code;  // BOP_PLUS, BOP_MINUS, BOP_MULTIPLY, BOP_DIVIDE, BOP_MODULO
    // DECIMAL/DECIMAL128 only (P9.1b): operand + result scales, supplied by the
    // binder (DrakenVector carries no scale — it lives on the LogicalType
    // descriptor at bind time). Zero for non-decimal ops (aggregate init `{op}`).
    unsigned char left_scale;
    unsigned char right_scale;
    unsigned char result_scale;  // dec_div result scale = max(sa+6,6) capped 18
    unsigned char result_precision;  // DECIMAL/DECIMAL128 result precision (descriptor)
    // TIMESTAMP/TIME only (S-A.2): TimestampUnit (0=s,1=ms,2=us,3=ns) of each
    // temporal operand, supplied by the binder (the unit is a LogicalType detail,
    // not on the physical DrakenVector). Zero for non-temporal ops. date32 operands
    // ignore the unit (days); only the TIMESTAMP64 side's unit is read.
    unsigned char left_unit;
    unsigned char right_unit;
};

/**
 * Context for BC_EXTRACTION operations.
 * Stores the sub-operation code for map/array/JSON extraction variants.
 */
struct extraction_ctx {
    int sub_op_code;  // BC_EXTR_MAP_STRING, BC_EXTR_MAP_ARRAY, BC_EXTR_JSON_PTR, BC_EXTR_JSON_VALUE
};

/**
 * Context for BC_CASE (not in 9a scope, but defined for completeness).
 * Stores compiled bytecode branches and condition arrays for case evaluation.
 */
struct case_ctx {
    void** cond_bcs;       // Array of CompiledBytecode* for conditions
    uint32_t n_conds;      // Number of condition branches
    void** result_bcs;     // Array of CompiledBytecode* for results
    void* else_bc;         // CompiledBytecode* for else branch (may be NULL)
    int assemble_kind;     // AssembleKind enum (how to assemble result)
};

/**
 * Context for draken_in_list — bind-time membership set, allocated by copying
 * a Python-built blob whose first bytes ARE this header:
 *   [u32 count][u8 kind][u8 negate][u16 pad][payload...]
 * kind 0: count x int64 SORTED ASCENDING (int family raw values; DECIMAL raw
 *         quantized to the column's scale at bind time).
 * kind 1: count x (u32 len + bytes) — UTF-8/ASCII string entries.
 * The list never contains NULL (the plan compiler rejects those lists).
 */
struct in_list_ctx {
    uint32_t count;
    uint8_t  kind;
    uint8_t  negate;
    uint16_t _pad;
    /* payload bytes follow the struct inline */
};

struct in_list_ctx* kernel_alloc_in_list_ctx(const uint8_t* blob, size_t blob_len);

/**
 * Context for draken_substring — SUBSTRING(str, start, count). `start` is 1-based
 * (SQL); Python-slice semantics apply after `start -= 1` (when start > 0). When
 * has_count is 0 the substring runs to the end of the string.
 */
struct substring_ctx {
    int32_t start;
    int32_t count;
    uint8_t has_count;
};

struct substring_ctx* kernel_alloc_substring_ctx(int32_t start, int32_t count,
                                                 uint8_t has_count);

#ifdef __cplusplus
}
#endif
