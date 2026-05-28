#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: arithmetic functions.
 * Phase 8a of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ function implementations
extern "C" VecResult vector_abs_impl(const DrakenVector* v);
extern "C" VecResult vector_sign_impl(const DrakenVector* v);
extern "C" VecResult vector_ceil_impl(const DrakenVector* v);
extern "C" VecResult vector_floor_impl(const DrakenVector* v);
extern "C" VecResult vector_round_impl(const DrakenVector* v);
extern "C" VecResult vector_round_digits_impl(const DrakenVector* v, const DrakenVector* digits);
extern "C" VecResult vector_sqrt_impl(const DrakenVector* v);
extern "C" VecResult vector_power_impl(const DrakenVector* base, const DrakenVector* exp);
extern "C" VecResult vector_log_impl(const DrakenVector* v);
extern "C" VecResult vector_trunc_impl(const DrakenVector* v);
extern "C" VecResult vector_random_impl(void);
extern "C" VecResult vector_random_normal_impl(void);
extern "C" VecResult vector_random_strings_impl(const DrakenVector* length, const DrakenVector* seed);

/**
 * ABS(x): absolute value
 */
VecResult vector_abs(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("ABS expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_abs_impl(args[0]);
    });
}

/**
 * SIGN(x): returns -1, 0, or 1
 */
VecResult vector_sign(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SIGN expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_sign_impl(args[0]);
    });
}

/**
 * CEIL(x): ceiling
 */
VecResult vector_ceil(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("CEIL expects at least 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_ceil_impl(args[0]);
    });
}

/**
 * FLOOR(x): floor
 */
VecResult vector_floor(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("FLOOR expects at least 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_floor_impl(args[0]);
    });
}

/**
 * ROUND(x) or ROUND(x, digits)
 */
VecResult vector_round(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("ROUND expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");

        if (nargs == 1) {
            return vector_round_impl(args[0]);
        } else {
            return vector_round_digits_impl(args[0], args[1]);
        }
    });
}

/**
 * SQRT(x): square root
 */
VecResult vector_sqrt(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SQRT expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_sqrt_impl(args[0]);
    });
}

/**
 * POWER(base, exp) or base ^ exp
 */
VecResult vector_power(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("POWER expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_power_impl(args[0], args[1]);
    });
}

/**
 * LOG(x): natural logarithm
 */
VecResult vector_log(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("LOG expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_log_impl(args[0]);
    });
}

/**
 * TRUNC(x): truncate to integer
 */
VecResult vector_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1) return draken_error_sentinel("TRUNC expects at least 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_trunc_impl(args[0]);
    });
}

/**
 * RANDOM(): random value between 0 and 1
 */
VecResult vector_random(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        // RANDOM() takes no arguments or one argument (seed)
        if (nargs > 1) return draken_error_sentinel("RANDOM expects 0 or 1 arguments");
        return vector_random_impl();
    });
}

/**
 * RANDOM_NORMAL(): random normal distribution
 */
VecResult vector_random_normal(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs > 1) return draken_error_sentinel("RANDOM_NORMAL expects 0 or 1 arguments");
        return vector_random_normal_impl();
    });
}

/**
 * RANDOM_STRINGS(length, seed): generate random strings
 */
VecResult vector_random_strings(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("RANDOM_STRINGS expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("First argument is null");

        if (nargs == 1) {
            // Length argument only
            return vector_random_strings_impl(args[0], nullptr);
        } else {
            // Length and seed
            return vector_random_strings_impl(args[0], args[1]);
        }
    });
}

}  // extern "C"
