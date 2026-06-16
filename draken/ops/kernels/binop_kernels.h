#pragma once
// draken/ops/kernels/binop_kernels.h — unified binary-op dispatch (P9.1).
//
// Single registry kernel for ALL binary operators (architect decision 2026-06-16:
// "single dispatch kernel"). ctx = binary_op_ctx{op_code}. The kernel dispatches
// internally on op_code + operand types.
//
// P9.1a covers INTEGER ARITHMETIC (PLUS/MINUS/MULTIPLY/MODULO/INT_DIVIDE over
// int8/16/32/64, D.6 widen-to-next-power result width, null-correct, all shapes).
// Not-yet-covered combinations (true DIVIDE→float64, float, decimal, bitwise,
// string concat, temporal, IP) return an error sentinel — they land in later
// P9.1 sub-stages. NOT YET WIRED into the executor: the live binop path remains
// draken_arithmetic_dv + the resolve_binary_op closure until the P9.1 flip, when
// every case is covered and this becomes the single dispatch (no beside-fallback).

#include "core/buffers.h"
#include "ops/vec_result.h"

#ifdef __cplusplus
extern "C" {
#endif

VecResult draken_binop(void* ctx, const DrakenVector* left, const DrakenVector* right);

#ifdef __cplusplus
}
#endif
