/**
 * Kernel Registry Header — maps kernel names to C function pointers.
 * Phase 9b: Registry for bytecode builder to resolve C kernels at bind time.
 *
 * This header provides:
 * - Typedef for kernel function signatures
 * - Exported registry function to lookup kernels by name
 * - Context struct types for parameterized kernels
 *
 * The registry is populated by function declarations in individual phase files.
 * Bytecode builder calls kernel_registry_lookup(name, out_fn, out_ctx) to
 * resolve kernel names at bind time.
 */

#ifndef DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_
#define DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_

#include "ops/kernels/c_kernel_abi.h"
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Kernel function signature: all C kernels follow this pattern.
 * ctx: optional context struct (cast_timestamp_ctx, binary_op_ctx, etc.) or NULL
 * args: pointer array of DrakenVector pointers
 * nargs: argument count
 * Returns: VecResult with data/validity/selection/type/length set
 */
typedef VecResult (*kernel_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/**
 * Lookup kernel by name.
 * Returns: true if found, false otherwise
 * out_fn: filled with kernel function pointer
 * out_ctx: filled with context struct pointer (NULL if no context needed)
 *
 * Name format: "FUNCTION_NAME" or "BC_OPERATION_NAME" (uppercase)
 * Examples: "ABS", "LENGTH", "ADD", "MAP_ACCESS_STRING", "CAST_INT64_TO_FLOAT64"
 *
 * Caller owns the returned context struct (if non-NULL) for the lifetime of bytecode execution.
 */
bool kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx);

/**
 * Allocate context struct for parameterized kernels.
 * Used by bytecode builder to create context structs (cast_timestamp_ctx with unit, etc.).
 * Context is stored in the CompiledBytecode._held_refs list for lifetime management.
 *
 * Context structs are defined in kernel_context.h
 */

cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit);
binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code,
                                          unsigned char left_scale,
                                          unsigned char right_scale,
                                          unsigned char result_scale,
                                          unsigned char result_precision,
                                          unsigned char left_unit,
                                          unsigned char right_unit);
extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code);

// Free allocated context (called during bytecode cleanup)
void kernel_free_context(void* ctx);

#ifdef __cplusplus
}
#endif

#endif  // DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_
