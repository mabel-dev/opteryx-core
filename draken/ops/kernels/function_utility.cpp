#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: utility and miscellaneous functions.
 * Phase 8i of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ utility function implementations
extern "C" VecResult vector_greatest_impl(const DrakenVector* const* args, uint32_t nargs);
extern "C" VecResult vector_least_impl(const DrakenVector* const* args, uint32_t nargs);
extern "C" VecResult vector_concat_ws_impl(const DrakenVector* delimiter, const DrakenVector* const* args, uint32_t nargs);
extern "C" VecResult vector_if_null_impl(const DrakenVector* value, const DrakenVector* default_val);
extern "C" VecResult vector_if_not_null_impl(const DrakenVector* value, const DrakenVector* result_val);
extern "C" VecResult vector_extract_impl(const DrakenVector* tuple_or_array, const DrakenVector* index);
extern "C" VecResult vector_map_access_impl(const DrakenVector* map, const DrakenVector* key);
extern "C" VecResult vector_humanize_impl(const DrakenVector* value);
extern "C" VecResult vector_type_name_impl(const DrakenVector* value);
extern "C" VecResult vector_is_null_impl(const DrakenVector* value);
extern "C" VecResult vector_is_not_null_impl(const DrakenVector* value);

/**
 * GREATEST(...): return greatest value from arguments
 */
VecResult vector_greatest(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("GREATEST expects at least 1 argument");
        if (!args) return draken_error_sentinel("Arguments are null");
        return vector_greatest_impl(args, nargs);
    });
}

/**
 * LEAST(...): return least value from arguments
 */
VecResult vector_least(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("LEAST expects at least 1 argument");
        if (!args) return draken_error_sentinel("Arguments are null");
        return vector_least_impl(args, nargs);
    });
}

/**
 * CONCAT_WS(delimiter, ...): concatenate with separator
 * First argument is the delimiter, rest are values to concatenate
 */
VecResult vector_concat_ws(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 2) return draken_error_sentinel("CONCAT_WS expects at least 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("Delimiter argument is null");
        // args[0] is delimiter, args[1..nargs-1] are values to concat
        return vector_concat_ws_impl(args[0], args + 1, nargs - 1);
    });
}

/**
 * IF_NULL(value, default): return default if value is null
 * Alias for COALESCE(value, default)
 */
VecResult vector_if_null(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("IF_NULL expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_if_null_impl(args[0], args[1]);
    });
}

/**
 * IF_NOT_NULL(value, result): return result if value is not null
 */
VecResult vector_if_not_null(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("IF_NOT_NULL expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_if_not_null_impl(args[0], args[1]);
    });
}

/**
 * EXTRACT(tuple_or_array, index): extract element at index
 */
VecResult vector_extract(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("EXTRACT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_extract_impl(args[0], args[1]);
    });
}

/**
 * MAP_ACCESS(map, key): access value in map/dict by key
 */
VecResult vector_map_access(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("MAP_ACCESS expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_map_access_impl(args[0], args[1]);
    });
}

/**
 * HUMANIZE(value): convert value to human-readable string
 * e.g., large numbers get formatted with K/M/B suffixes
 */
VecResult vector_humanize(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("HUMANIZE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_humanize_impl(args[0]);
    });
}

/**
 * TYPE_NAME(value): return name of value's type
 */
VecResult vector_type_name(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("TYPE_NAME expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_type_name_impl(args[0]);
    });
}

/**
 * IS_NULL(value): check if value is null
 */
VecResult vector_is_null(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("IS_NULL expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_is_null_impl(args[0]);
    });
}

/**
 * IS_NOT_NULL(value): check if value is not null
 */
VecResult vector_is_not_null(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("IS_NOT_NULL expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_is_not_null_impl(args[0]);
    });
}

}  // extern "C"
