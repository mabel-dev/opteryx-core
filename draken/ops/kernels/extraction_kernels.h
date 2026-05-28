#pragma once

/**
 * BC_EXTRACTION kernel ABI — Phase 9a.
 *
 * All extraction kernels follow the Decision 3 signature:
 *   VecResult (*)(void* ctx, const DrakenVector* vector, const DrakenVector* key)
 *
 * Four extraction sub-operations, identified by slot.op_code:
 *   BC_EXTR_MAP_STRING → vector_map_access_string
 *   BC_EXTR_MAP_ARRAY → vector_array_map_access
 *   BC_EXTR_JSON_PTR → vector_json_extract (Arrow →)
 *   BC_EXTR_JSON_VALUE → vector_json_extract (Arrow ->>)
 *
 * Context is NULL for all extraction kernels (operation determined at bind time).
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
 * Map/Dict Access — extract value from map/struct by string key
 * ========================================================================== */

/**
 * Access map/struct field by string key.
 * vector is a map/struct vector; key is a string vector.
 * ctx is NULL.
 * Returns result vector (type depends on field) or error.
 */
VecResult draken_map_access_string(void* ctx, const DrakenVector* vector, const DrakenVector* key);

/* ============================================================================
 * Array Index Access — extract element from array by index
 * ========================================================================== */

/**
 * Access array element by index.
 * vector is an array vector; key is an integer vector (indices).
 * ctx is NULL.
 * Returns result vector (element type) or error.
 */
VecResult draken_array_map_access(void* ctx, const DrakenVector* vector, const DrakenVector* key);

/* ============================================================================
 * JSON Path Extraction — extract JSON value by path
 *
 * Handles both Arrow → (returns text representation) and ->> (returns value).
 * These are distinguished at bind time and mapped to separate executors if needed,
 * or merged into one kernel with ctx discrimination (TBD per 9b).
 * ========================================================================== */

/**
 * JSON path extraction: object[key] or array[index] or nested path.
 * vector is a JSON column; key is a string vector (JSON path or index).
 * ctx is NULL (path discrimination happens at bind time).
 * Returns JSON text (→) or extracted value (->>) or error.
 */
VecResult draken_json_extract(void* ctx, const DrakenVector* vector, const DrakenVector* key);

/* ============================================================================
 * Pointer Extraction (Future / Optional)
 * ========================================================================== */

/**
 * Extract value via pointer/reference.
 * Not yet fully specified; placeholder for future extension.
 */
VecResult draken_pointer_extract(void* ctx, const DrakenVector* vector, const DrakenVector* key);

#ifdef __cplusplus
}
#endif
