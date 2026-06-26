#pragma once

/**
 * Draken C Kernel ABI — Phase 9a
 *
 * Central header for all bytecode executor kernel access.
 * Includes all category-specific kernel headers and context definitions.
 *
 * Usage (from opteryx/expression/evaluator/evaluation.pyx in Phase 9c):
 *
 *   // Cast example:
 *   #include "draken/ops/kernels/c_kernel_abi.h"
 *   typedef VecResult (*CastKernel)(void* ctx, const DrakenVector* v);
 *   CastKernel kernel_fn = (CastKernel)slot.kernel_fn;
 *   VecResult result = kernel_fn(slot.ctx_ptr, dv_stack[sp]);
 *
 *   // Function example:
 *   #include "draken/ops/kernels/c_kernel_abi.h"
 *   typedef VecResult (*FunctionKernel)(void* ctx, const DrakenVector* const* args, uint32_t nargs);
 *   FunctionKernel kernel_fn = (FunctionKernel)slot.kernel_fn;
 *   VecResult result = kernel_fn(slot.ctx_ptr, args_array, num_args);
 *
 * All kernels return VecResult struct. Error path: data == nullptr.
 * See error_handling.h for thread-local error slot usage.
 */

#include "ops/vec_result.h"
#include "core/buffers.h"

// Context struct definitions
#include "ops/kernels/kernel_context.h"

// Kernel headers by opcode category
#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/extraction_kernels.h"
