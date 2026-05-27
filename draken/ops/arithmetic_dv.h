#pragma once
// draken/ops/arithmetic_dv.h — arena-backed binary arithmetic entry
// point for the native eval engine.
//
// Wraps the typed arithmetic kernels (`i64_add`/`i64_sub`/… in
// `int64_arithmetic.h`, `float_add`/… in `float_ops.h`) in a single
// C-linkage function that adopts the result buffers into the supplied
// frame arena.
//
// Stage B coverage: INT64, FLOAT64.
// Returns NULL for any unsupported type/op combination — caller falls
// back to Python-mediated arithmetic (e.g. string concat, decimal,
// temporal arithmetic, cross-type) in those cases.
//
// Op-code convention (matches `BCBinaryOpCode` in
// `opteryx/compiled/expression/compiled_expression.pxd`):
//   1 = PLUS, 2 = MINUS, 3 = MULTIPLY, 4 = DIVIDE, 5 = MODULO
// INT_DIVIDE (6), STRING_CONCAT (7), and the bitwise/shift ops (8+) are
// out of scope for this function — caller routes them elsewhere.

#include <stdint.h>
#include "core/buffers.h"
#include "core/frame_arena.h"

#ifdef __cplusplus
extern "C" {
#endif

// Perform a binary arithmetic op on two DrakenVectors element-wise.
//
// Parameters:
//   op_code  : 1=PLUS, 2=MINUS, 3=MULTIPLY, 4=DIVIDE, 5=MODULO
//   left, right : input vectors (must have the same length and type)
//   n_rows   : expected row count; both inputs must match
//   arena    : per-frame arena to allocate result into
//
// Returns:
//   DrakenVector* : result (INT64 or FLOAT64 depending on inputs),
//                   arena-owned. Valid until `draken_frame_arena_destroy`.
//   NULL          : on unsupported type/op combination, length or type
//                   mismatch, OOM, or NULL inputs. Caller falls back to
//                   Python.
DrakenVector* draken_arithmetic_dv(
    int                op_code,
    DrakenVector*      left,
    DrakenVector*      right,
    uint32_t           n_rows,
    DrakenFrameArena*  arena
);

#ifdef __cplusplus
}
#endif
