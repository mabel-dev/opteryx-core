#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: boolean and logical functions.
 * Phase 8d of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ boolean function implementations
extern "C" VecResult vector_coalesce_impl(const DrakenVector* const* args, uint32_t nargs);
extern "C" VecResult vector_iif_impl(const DrakenVector* condition, const DrakenVector* true_val, const DrakenVector* false_val);
extern "C" VecResult vector_nullif_impl(const DrakenVector* val1, const DrakenVector* val2);
extern "C" VecResult vector_allop_eq_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_allop_neq_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_eq_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_neq_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_lt_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_lte_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_gt_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_anyop_gte_impl(const DrakenVector* left, const DrakenVector* right);
extern "C" VecResult vector_in_list_impl(const DrakenVector* value, const DrakenVector* list);
extern "C" VecResult vector_bool_and_chain_impl(const DrakenVector* const* args, uint32_t nargs);
extern "C" VecResult vector_bool_all_true_impl(const DrakenVector* v);

/**
 * COALESCE(...): return first non-NULL value
 */
VecResult vector_coalesce(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("COALESCE expects at least 1 argument");
        if (!args) return draken_error_sentinel("Arguments are null");
        return vector_coalesce_impl(args, nargs);
    });
}

/**
 * IIF(condition, true_value, false_value): inline if-then-else
 */
VecResult vector_iif(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 3) return draken_error_sentinel("IIF expects 3 arguments");
        if (!args || !args[0] || !args[1] || !args[2]) return draken_error_sentinel("Arguments are null");
        return vector_iif_impl(args[0], args[1], args[2]);
    });
}

/**
 * NULLIF(val1, val2): return NULL if val1 == val2, else val1
 */
VecResult vector_nullif(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("NULLIF expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_nullif_impl(args[0], args[1]);
    });
}

/**
 * ALLOP_EQ(array, value): check if all elements equal value
 */
VecResult vector_allop_eq(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ALLOP_EQ expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_allop_eq_impl(args[0], args[1]);
    });
}

/**
 * ALLOP_NEQ(array, value): check if all elements not equal value
 */
VecResult vector_allop_neq(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ALLOP_NEQ expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_allop_neq_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_EQ(array, value): check if any element equals value
 */
VecResult vector_anyop_eq(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_EQ expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_eq_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_NEQ(array, value): check if any element not equals value
 */
VecResult vector_anyop_neq(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_NEQ expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_neq_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_LT(array, value): check if any element less than value
 */
VecResult vector_anyop_lt(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_LT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_lt_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_LTE(array, value): check if any element less than or equal to value
 */
VecResult vector_anyop_lte(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_LTE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_lte_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_GT(array, value): check if any element greater than value
 */
VecResult vector_anyop_gt(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_GT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_gt_impl(args[0], args[1]);
    });
}

/**
 * ANYOP_GTE(array, value): check if any element greater than or equal to value
 */
VecResult vector_anyop_gte(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ANYOP_GTE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_anyop_gte_impl(args[0], args[1]);
    });
}

/**
 * IN_LIST(value, list): check if value is in list
 */
VecResult vector_in_list(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("IN_LIST expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_in_list_impl(args[0], args[1]);
    });
}

/**
 * BOOL_AND_CHAIN(...): AND together all boolean vectors
 */
VecResult vector_bool_and_chain(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("BOOL_AND_CHAIN expects at least 1 argument");
        if (!args) return draken_error_sentinel("Arguments are null");
        return vector_bool_and_chain_impl(args, nargs);
    });
}

/**
 * BOOL_ALL_TRUE(vec): check if all values are true
 */
VecResult vector_bool_all_true(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("BOOL_ALL_TRUE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_bool_all_true_impl(args[0]);
    });
}

}  // extern "C"
