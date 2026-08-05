#pragma once

/**
 * BC_EXTRACTION kernel ABI.
 *
 * All extraction kernels follow the Decision 3 signature:
 *   VecResult (*)(void* ctx, const DrakenVector* vector, const DrakenVector* key)
 *
 * Four sub-operations (BCExtractionOpCode in compiled_expression.pxd) map onto
 * three kernels; the binder resolves the pair at bind time:
 *   BC_EXTR_MAP_STRING (str[i])  → draken_map_access_string
 *   BC_EXTR_MAP_ARRAY  (arr[i])  → draken_array_map_access
 *   BC_EXTR_JSON_PTR   (`->`)    → draken_json_extract
 *   BC_EXTR_JSON_KEY   (`->>`)   → draken_json_extract  (text mode, same kernel)
 *
 * Context is NEVER NULL: every bind-time parameter — sub-op code, navigation
 * path (already an RFC 6901 pointer for the JSON sub-ops), subscript index —
 * arrives in extraction_ctx, so no kernel here pops a key operand. The ABI's
 * second slot is therefore free, and BC_EXTR_MAP_ARRAY reuses it to receive the
 * ARRAY's child vector (BC_C_NATIVE_CHILD); every other kernel is handed NULL
 * there and ignores it.
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
 * Character Subscript — str[i]
 * ========================================================================== */

/**
 * Take one character from each row of a string vector.
 * vector is VARCHAR/NVARCHAR/VARBINARY; the index is ctx->index; key is unused.
 * Returns a string vector of the same shape, or error.
 */
VecResult draken_map_access_string(void* ctx, const DrakenVector* vector, const DrakenVector* key);

/* ============================================================================
 * Array Index Access — arr[i]
 * ========================================================================== */

/**
 * Access array element by index.
 * vector is DRAKEN_ARRAY; the index is ctx->index. The `child` slot (the ABI's
 * key position) carries the ARRAY's element vector, which hangs off the
 * VectorOwner and is unreachable from the parent DrakenVector — the VM resolves
 * it per morsel and passes it here (BC_C_NATIVE_CHILD). It is required, not
 * optional: the kernel fails loud without it.
 * Returns a vector of the CHILD's type, or error.
 */
VecResult draken_array_map_access(void* ctx, const DrakenVector* vector, const DrakenVector* child);

/* ============================================================================
 * JSON Path Extraction — `->` and `->>`
 *
 * One kernel serves both: ctx->sub_op_code selects the output mode.
 *   BC_EXTR_JSON_PTR (`->`)  → JSON text, tagged VARIANT.
 *   BC_EXTR_JSON_KEY (`->>`) → text, JSON strings unquoted, tagged NVARCHAR.
 * ========================================================================== */

/**
 * JSON path extraction over a string-family or VARIANT column.
 * The path is ctx's nav bytes, already converted to an RFC 6901 pointer by
 * kernel_alloc_extraction_ctx; key is unused.
 * Returns the extracted value as VARIANT (`->`) or NVARCHAR (`->>`), or error.
 */
VecResult draken_json_extract(void* ctx, const DrakenVector* vector, const DrakenVector* key);

/**
 * N `->`/`->>` extractions over ONE parse per row (sibling-extraction fusion).
 *
 * NOT a VM kernel: it neither takes nor returns the kernel_fn_t shape and is not in
 * the registry. A VM instruction pops one operand and pushes one result; this
 * produces N columns from one operand, so it backs a physical operator
 * (JsonExtractMultiOperator) instead. Parsing dominates extraction cost, so N paths
 * over one parse cost barely more than one.
 *
 * `ctxs` are ordinary extraction_ctx blocks — the same ones a single extraction
 * binds, one per path. Every entry must be a `->` or `->>` sub-op.
 *
 * Returns nullptr on success, having filled out[0..n-1] (caller owns them). On
 * failure returns the error message and fills nothing; that pointer is the calling
 * thread's error buffer and is valid only until the next kernel call on this thread.
 */
const char* draken_json_extract_multi(const void* const* ctxs, uint32_t n,
                                      const DrakenVector* vector, VecResult* out);

#ifdef __cplusplus
}
#endif
