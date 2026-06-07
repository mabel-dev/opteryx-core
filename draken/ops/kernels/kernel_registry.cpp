/**
 * Kernel Registry Implementation — lookup and allocation for C kernels.
 * Phase 9b: Central registry for bytecode builder to resolve C kernels at bind time.
 *
 * Maintains a map from kernel name (string) to function pointer + context allocator.
 * Built at module load time via forward declarations + explicit registration.
 */

#include "ops/kernels/kernel_registry.h"
#include "ops/kernels/error_handling.h"
#include <cstring>
#include <cstdlib>
#include <map>
#include <functional>
#include <string>

// ---------------------------------------------------------------------------
// Forward declarations of C kernel functions (Phase 9a)
// ---------------------------------------------------------------------------
//
// Phase 9a implements:
// - Cast kernels (cast_numeric, cast_string, cast_temporal, cast_dispatch)
// - Binary operation kernels (binary_op_arithmetic, binary_op_other, binary_op_temporal)
// - Extraction kernels (placeholder)
//
// Function kernels (Phase 8a-8i) are deferred to Phase 9a-fn; they require
// nanobind wrapper functions to be ported to extern "C" signatures.

#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/extraction_kernels.h"

// ---------------------------------------------------------------------------
// Kernel Registry Table (Phase 9a)
// ---------------------------------------------------------------------------
//
// Phase 9a: Cast and binary op kernels.
// Function kernels (Phase 8a-8i) deferred to Phase 9a-fn.
//
// Key naming scheme:
// - Cast kernels: Directly use function name (e.g., "draken_cast_int64_to_float64")
// - Binary op kernels: Use function name (e.g., "draken_add", "draken_bitwise_or", "draken_binary_arith")
// - Extraction kernels: Use function name (e.g., "draken_map_access_string")
//
// At bind time (Phase 9b), resolvers will construct these keys from operation types
// and operand types, then look them up in this registry.
//
static std::map<std::string, kernel_fn_t> _kernel_registry = {
    // ========================================================================
    // Cast kernels (31 total)
    // ========================================================================

    // Numeric type-specific casts (cast_numeric.cpp)
    {"draken_cast_int64_to_float64", (kernel_fn_t)&draken_cast_int64_to_float64},
    {"draken_cast_int64_to_string", (kernel_fn_t)&draken_cast_int64_to_string},
    {"draken_cast_int64_to_bool", (kernel_fn_t)&draken_cast_int64_to_bool},
    {"draken_cast_bool_to_float64", (kernel_fn_t)&draken_cast_bool_to_float64},
    {"draken_cast_bool_to_string", (kernel_fn_t)&draken_cast_bool_to_string},
    {"draken_cast_bool_to_int64", (kernel_fn_t)&draken_cast_bool_to_int64},
    {"draken_cast_float64_to_int64", (kernel_fn_t)&draken_cast_float64_to_int64},
    {"draken_cast_float64_to_string", (kernel_fn_t)&draken_cast_float64_to_string},
    {"draken_cast_float64_to_bool", (kernel_fn_t)&draken_cast_float64_to_bool},

    // String type-specific casts (cast_string.cpp)
    {"draken_cast_string_to_int64", (kernel_fn_t)&draken_cast_string_to_int64},
    {"draken_cast_string_to_float64", (kernel_fn_t)&draken_cast_string_to_float64},
    {"draken_cast_string_to_bool", (kernel_fn_t)&draken_cast_string_to_bool},

    // Temporal type-specific casts (cast_temporal.cpp)
    // NOTE: draken_cast_date32_to_string is declared in header but implemented as draken_cast_date_to_string
    // (naming mismatch). Not registered until the naming is aligned.
    {"draken_cast_int64_to_timestamp", (kernel_fn_t)&draken_cast_int64_to_timestamp},
    {"draken_cast_date32_to_int64", (kernel_fn_t)&draken_cast_date32_to_int64},
    {"draken_cast_timestamp_to_int64", (kernel_fn_t)&draken_cast_timestamp_to_int64},
    {"draken_cast_timestamp_to_string", (kernel_fn_t)&draken_cast_timestamp_to_string},
    {"draken_cast_date32_to_timestamp", (kernel_fn_t)&draken_cast_date32_to_timestamp},
    {"draken_cast_timestamp_to_date32", (kernel_fn_t)&draken_cast_timestamp_to_date32},

    // Dispatch helpers (any → target type)
    {"draken_cast_to_float64", (kernel_fn_t)&draken_cast_to_float64},
    {"draken_cast_to_int64", (kernel_fn_t)&draken_cast_to_int64},
    {"draken_cast_to_varchar", (kernel_fn_t)&draken_cast_to_varchar},
    {"draken_cast_to_bool", (kernel_fn_t)&draken_cast_to_bool},
    {"draken_cast_to_date", (kernel_fn_t)&draken_cast_to_date},

    // Parameterized casts
    {"draken_cast_to_decimal", (kernel_fn_t)&draken_cast_to_decimal},
    {"draken_cast_to_array", (kernel_fn_t)&draken_cast_to_array},
    {"draken_cast_to_vector", (kernel_fn_t)&draken_cast_to_vector},
    {"draken_cast_to_varchar_with_length", (kernel_fn_t)&draken_cast_to_varchar_with_length},

    // Identity/passthrough cast
    {"draken_cast_identity", (kernel_fn_t)&draken_cast_identity},

    // ========================================================================
    // Binary operation kernels (16 total)
    // ========================================================================

    // Arithmetic dispatch and individual operations
    {"draken_binary_arith", (kernel_fn_t)&draken_binary_arith},
    {"draken_add", (kernel_fn_t)&draken_add},
    {"draken_subtract", (kernel_fn_t)&draken_subtract},
    {"draken_multiply", (kernel_fn_t)&draken_multiply},
    {"draken_divide", (kernel_fn_t)&draken_divide},
    {"draken_modulo", (kernel_fn_t)&draken_modulo},

    // Bitwise operations
    {"draken_bitwise_or", (kernel_fn_t)&draken_bitwise_or},
    {"draken_bitwise_and", (kernel_fn_t)&draken_bitwise_and},
    {"draken_bitwise_xor", (kernel_fn_t)&draken_bitwise_xor},
    {"draken_bitwise_shift_left", (kernel_fn_t)&draken_bitwise_shift_left},
    {"draken_bitwise_shift_right", (kernel_fn_t)&draken_bitwise_shift_right},

    // String operations
    {"draken_string_concat", (kernel_fn_t)&draken_string_concat},

    // Temporal operations
    {"draken_temporal_interval_op", (kernel_fn_t)&draken_temporal_interval_op},
    {"draken_date_minus_date", (kernel_fn_t)&draken_date_minus_date},
    {"draken_interval_interval_op", (kernel_fn_t)&draken_interval_interval_op},

    // IP address operations
    {"draken_ip_in_cidr", (kernel_fn_t)&draken_ip_in_cidr},

    // ========================================================================
    // Extraction kernels (4 total)
    // ========================================================================

    {"draken_map_access_string", (kernel_fn_t)&draken_map_access_string},
    {"draken_array_map_access", (kernel_fn_t)&draken_array_map_access},
    {"draken_json_extract", (kernel_fn_t)&draken_json_extract},
    {"draken_pointer_extract", (kernel_fn_t)&draken_pointer_extract},
};

// ---------------------------------------------------------------------------
// Registry Lookup Implementation
// ---------------------------------------------------------------------------

bool kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx) {
    if (!name || !out_fn || !out_ctx) {
        return false;
    }

    auto it = _kernel_registry.find(name);
    if (it == _kernel_registry.end()) {
        return false;
    }

    *out_fn = it->second;
    *out_ctx = nullptr;  // No context for simple kernels; caller can allocate if needed
    return true;
}

// ---------------------------------------------------------------------------
// Context Allocators
// ---------------------------------------------------------------------------

cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit) {
    auto* ctx = static_cast<cast_timestamp_ctx*>(malloc(sizeof(cast_timestamp_ctx)));
    if (ctx) {
        ctx->unit = unit;
    }
    return ctx;
}

binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code) {
    auto* ctx = static_cast<binary_op_ctx*>(malloc(sizeof(binary_op_ctx)));
    if (ctx) {
        ctx->op_code = op_code;
    }
    return ctx;
}

extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code) {
    auto* ctx = static_cast<extraction_ctx*>(malloc(sizeof(extraction_ctx)));
    if (ctx) {
        ctx->sub_op_code = sub_op_code;
    }
    return ctx;
}

void kernel_free_context(void* ctx) {
    free(ctx);
}
