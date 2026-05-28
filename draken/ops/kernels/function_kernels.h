#pragma once

/**
 * BC_FUNCTION kernel ABI — Phase 9a.
 *
 * All function kernels follow the Decision 3 signature:
 *   VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 *
 * Context is NULL for built-in functions (no parameterization yet).
 *
 * Error handling: On error, return VecResult with data == nullptr (sentinel).
 *
 * This header defines:
 * 1. The function kernel typedef.
 * 2. Forward declarations for all ~90+ built-in function kernels.
 * 3. A kernel registry table (defined separately in .cpp).
 */

#include "ops/vec_result.h"
#include "core/buffers.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Function Kernel Signature Typedef
 * ========================================================================== */

/**
 * Signature for all BC_FUNCTION kernels.
 * Variadic: args is array of const DrakenVector*; nargs is count.
 */
typedef VecResult (*FunctionKernel)(
    void* ctx,
    const DrakenVector* const* args,
    uint32_t nargs
);

/* ============================================================================
 * Arithmetic / Numeric Functions (~10 kernels)
 * ========================================================================== */

VecResult vector_abs(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_sign(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_ceil(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_floor(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_round(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_sqrt(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_power(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_log(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_random(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_random_normal(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_random_strings(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * String / Text Functions (~30 kernels)
 * ========================================================================== */

VecResult vector_length(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_length(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_slice_left(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_slice_right(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_trim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_ltrim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_rtrim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_lowercase(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_uppercase(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_initcap(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_reverse(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_replace(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_position(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_starts_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_ends_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_ci_starts_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_ci_ends_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_regex_replace(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_levenshtein(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_soundex(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_is_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_string_is_not_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* String → scalar conversions (Python row-loops in initial phase; flag for C impl) */
VecResult vector_to_ascii(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_to_char(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_left_pad(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_right_pad(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Date / Time Functions (~10 kernels)
 * ========================================================================== */

VecResult vector_unixtime(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_from_unixtimestamp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_date_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_date_format(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_date_part(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_date_diff(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_time_diff(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_floor_temporal(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_date32_to_timestamp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_timestamp_to_date32(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Boolean / Logical Functions (~10 kernels)
 * ========================================================================== */

VecResult vector_coalesce(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_iif(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_nullif(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_allop_eq(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_allop_neq(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_eq(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_neq(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_lt(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_lte(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_gt(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_anyop_gte(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_in_list(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_bool_and_chain(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_bool_all_true(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Array Functions (~8 kernels)
 * ========================================================================== */

VecResult vector_array_concat(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_array_reduce(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_split(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* Array → scalar conversions (Python row-loops; flag for C impl) */
VecResult vector_array_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_array_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_array_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Hashing / Encoding Functions (~8 kernels)
 * ========================================================================== */

VecResult vector_md5(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_sha1(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_sha256(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_sha512(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_base64_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_base64_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_hex_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_hex_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_base85_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_base85_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Similarity / Distance Functions (~5 kernels; some Python row-loops)
 * ========================================================================== */

VecResult vector_cosine_similarity(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_cosine_distance(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* Text-based similarity (Python row-loops; flag for C impl) */
VecResult vector_cosine_similarity_text(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_cosine_distance_text(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_embed(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * JSON Functions (~2 kernels)
 * ========================================================================== */

VecResult vector_json_extract(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_jsonb_object_keys(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* ============================================================================
 * Other / Utility Functions (~5 kernels)
 * ========================================================================== */

VecResult vector_map_access(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_map_access_string(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_map_access_array(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_extract(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/* Utility (Python row-loops; flag for C impl) */
VecResult vector_if_null(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_if_not_null(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_greatest(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_least(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_humanize(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult vector_concat_ws(void* ctx, const DrakenVector* const* args, uint32_t nargs);

#ifdef __cplusplus
}
#endif
