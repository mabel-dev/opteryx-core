#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: array functions.
 * Phase 8e of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ array function implementations
extern "C" VecResult vector_array_concat_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_contains_any_impl(const DrakenVector* array, const DrakenVector* value);
extern "C" VecResult vector_contains_all_impl(const DrakenVector* array, const DrakenVector* value);
extern "C" VecResult vector_array_reduce_impl(const DrakenVector* array, const DrakenVector* operator_fn);
extern "C" VecResult vector_array_length_impl(const DrakenVector* array);
extern "C" VecResult vector_array_reverse_impl(const DrakenVector* array);
extern "C" VecResult vector_array_distinct_impl(const DrakenVector* array);
extern "C" VecResult vector_array_sort_impl(const DrakenVector* array);

/**
 * ARRAY_CONCAT(left, right): concatenate two arrays
 */
VecResult vector_array_concat(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ARRAY_CONCAT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_array_concat_impl(args[0], args[1]);
    });
}

/**
 * CONTAINS_ANY(array, search_value): check if array contains any matching value
 */
VecResult vector_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("CONTAINS_ANY expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_contains_any_impl(args[0], args[1]);
    });
}

/**
 * CONTAINS_ALL(array, search_value): check if array contains all matching values
 */
VecResult vector_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("CONTAINS_ALL expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_contains_all_impl(args[0], args[1]);
    });
}

/**
 * ARRAY_REDUCE(array, operator): reduce array with operator
 * operator: '+', '*', 'min', 'max', 'concat', etc.
 */
VecResult vector_array_reduce(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ARRAY_REDUCE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_array_reduce_impl(args[0], args[1]);
    });
}

/**
 * ARRAY_LENGTH(array): return length of array
 */
VecResult vector_array_length(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("ARRAY_LENGTH expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_array_length_impl(args[0]);
    });
}

/**
 * ARRAY_REVERSE(array): reverse array order
 */
VecResult vector_array_reverse(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("ARRAY_REVERSE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_array_reverse_impl(args[0]);
    });
}

/**
 * ARRAY_DISTINCT(array): remove duplicates from array
 */
VecResult vector_array_distinct(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("ARRAY_DISTINCT expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_array_distinct_impl(args[0]);
    });
}

/**
 * ARRAY_SORT(array): sort array elements
 */
VecResult vector_array_sort(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("ARRAY_SORT expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_array_sort_impl(args[0]);
    });
}

}  // extern "C"
