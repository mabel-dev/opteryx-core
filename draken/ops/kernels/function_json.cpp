#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: JSON functions.
 * Phase 8h of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ JSON function implementations
extern "C" VecResult vector_jsonb_object_keys_impl(const DrakenVector* json_obj);

/**
 * JSONB_OBJECT_KEYS(json_object): extract keys from JSON object
 * Returns array of keys as strings
 */
VecResult vector_jsonb_object_keys(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("JSONB_OBJECT_KEYS expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_jsonb_object_keys_impl(args[0]);
    });
}

}  // extern "C"
