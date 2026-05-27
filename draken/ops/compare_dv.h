#pragma once
// draken/ops/compare_dv.h — arena-backed compare entry point for the
// native eval engine.
//
// `draken_compare_dv` wraps the typed compare kernels in a single
// C-linkage function:
//
//   1. Dispatch by left vector's DrakenType to the appropriate kernel.
//   2. Kernel returns VecResult with draken_malloc'd data/validity buffers
//      and a possibly-owned selection.
//   3. Adopt those buffers into the supplied frame arena.
//   4. Allocate a DrakenVector struct from the arena, populate it from
//      VecResult, return its pointer.
//
// Caller does not free the result; `draken_frame_arena_destroy` will.
//
// Coverage:
//   INT64, TIMESTAMP64  (Stage B)
//   FLOAT64             (Stage B)
//   DATE32              (Stage C)
//   VARCHAR, NVARCHAR, VARBINARY  (Stage C)
//   DECIMAL — NOT yet supported; needs scale from logical-type descriptor
//             (which lives on VectorOwner, not DrakenVector). Caller's
//             Python fallback path handles it. See compare_dv.cpp comment.
//
// Returns NULL for any type/op combination not yet covered — caller is
// expected to use the Python-mediated fallback path in those cases.
//
// Op-code convention (matches `int64_compare.h:i64_compare_vector`):
//   0 = EQ, 1 = NE, 2 = GT, 3 = GE, 4 = LT, 5 = LE
// Negation (NOT_EQ etc.) is applied by the caller on the resulting
// BoolVector; this function only handles positive comparisons.
//
// Constant-shape inputs are handled transparently: the underlying kernels
// use uniform `data[selection[i]]` access. A scalar right-hand-side should
// be passed as a Constant-shape DrakenVector (data_length=1, selection
// pointing at draken_zero_sel).

#include <stdint.h>
#include "core/buffers.h"
#include "core/frame_arena.h"

#ifdef __cplusplus
extern "C" {
#endif

// Compare two DrakenVectors element-wise.
//
// Parameters:
//   op_code         : 0=EQ, 1=NE, 2=GT, 3=GE, 4=LT, 5=LE
//   left, right     : input vectors (must have the same `length`)
//   left_type_hint  : BCTypeCode (0=none, 1=date, 2=timestamp). Currently
//                     unused — int64 storage of date/timestamp shares the
//                     same comparison semantics as raw int64. Reserved for
//                     future per-hint behaviour.
//   right_type_hint : as above
//   n_rows          : expected row count; both inputs must match
//   arena           : per-frame arena to allocate result into
//
// Returns:
//   DrakenVector*   : DRAKEN_BOOL result, arena-owned. Pointer remains valid
//                     until `draken_frame_arena_destroy(arena)`.
//   NULL            : on unsupported type/op combination, length mismatch,
//                     OOM, or NULL inputs. Caller falls back to Python.
DrakenVector* draken_compare_dv(
    int                op_code,
    DrakenVector*      left,
    DrakenVector*      right,
    int16_t            left_type_hint,
    int16_t            right_type_hint,
    uint32_t           n_rows,
    DrakenFrameArena*  arena
);

#ifdef __cplusplus
}
#endif
