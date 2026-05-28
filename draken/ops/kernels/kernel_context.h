#pragma once

/**
 * Kernel context structures for parameterized kernels in Phase 9.
 *
 * Context structs are passed as void* ctx to C ABI kernels. Each kernel family
 * that needs parameterization (cast unit, binary op code, etc.) has a context
 * struct defined here. Context lifetime ≥ CompiledBytecode; held in _held_refs.
 */

#include <cstdint>

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
    int element_type;  // OrsoType enum value
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

#ifdef __cplusplus
}
#endif
