#pragma once
// Declared-width coercion: a file may store a column NARROWER than the relation
// declares — a legal schema evolution (ALTER COLUMN ... TYPE rewrites no data), a
// directory whose files disagree, or an Iceberg `int` over a parquet INT(16). Values
// of such a column are widened to the declared width before anything reads them as
// the declared type. ONE policy and ONE kernel choice, shared by everything that
// meets a narrow decoded column under a wider plan type: the parquet Source (what it
// emits) and the pass-1 predicate entry (what the decode workers evaluate).

#include "core/buffers.h"
#include "ops/kernels/cast_kernels.h"      // the ladder-widening casts
#include "ops/kernels/error_handling.h"    // draken_error_sentinel

namespace opteryx::engine {

// Strictly up one ladder (signed int / unsigned int / float). Re-checked here
// rather than trusted from the plan: a narrowing can lose a value, and must be
// impossible by construction on this path, not merely unplanned.
inline bool is_ladder_widening(DrakenType from, DrakenType to) {
    auto pos = [](DrakenType t, int& ladder, int& rank) {
        switch (t) {
            case DRAKEN_INT8:    ladder = 0; rank = 0; return true;
            case DRAKEN_INT16:   ladder = 0; rank = 1; return true;
            case DRAKEN_INT32:   ladder = 0; rank = 2; return true;
            case DRAKEN_INT64:   ladder = 0; rank = 3; return true;
            case DRAKEN_UINT8:   ladder = 1; rank = 0; return true;
            case DRAKEN_UINT16:  ladder = 1; rank = 1; return true;
            case DRAKEN_UINT32:  ladder = 1; rank = 2; return true;
            case DRAKEN_UINT64:  ladder = 1; rank = 3; return true;
            case DRAKEN_FLOAT32: ladder = 2; rank = 0; return true;
            case DRAKEN_FLOAT64: ladder = 2; rank = 1; return true;
            default: return false;
        }
    };
    int fl = 0, fr = 0, tl = 0, tr = 0;
    if (!pos(from, fl, fr) || !pos(to, tl, tr)) return false;
    // Strictly up one ladder, or unsigned -> a strictly wider signed int
    // (ladder 1 -> ladder 0; equal rank is equal byte width, so strictly wider
    // only) — the same policy as opteryx's `is_legal_widen`.
    return (fl == tl && tr > fr) || (fl == 1 && tl == 0 && tr > fr);
}

// The cast kernels are shape-preserving (kernel_preserve_shape): they convert
// data_length values and carry the input's selection through, so a dict-shaped
// column widens to a dict-shaped column with widened dictionary values -- the
// uniform access pattern is unchanged. The result owns `data`, `validity` and,
// when `owns_selection`, `selection`; a failure has `data == nullptr`.
inline VecResult ladder_widen(DrakenType to, const DrakenVector* v) {
    // A signed target takes the unsigned-source kernel when the column decoded
    // unsigned (the cross-ladder widening); the signed kernel refuses one.
    const bool from_unsigned = v->type == DRAKEN_UINT8 || v->type == DRAKEN_UINT16 ||
                               v->type == DRAKEN_UINT32 || v->type == DRAKEN_UINT64;
    switch (to) {
        case DRAKEN_INT16:   return from_unsigned ? draken_cast_uint_to_int16(nullptr, v)
                                                  : draken_cast_integer_to_int16(nullptr, v);
        case DRAKEN_INT32:   return from_unsigned ? draken_cast_uint_to_int32(nullptr, v)
                                                  : draken_cast_integer_to_int32(nullptr, v);
        case DRAKEN_INT64:   return from_unsigned ? draken_cast_uint_to_int64(nullptr, v)
                                                  : draken_cast_integer_to_int64(nullptr, v);
        case DRAKEN_UINT16:  return draken_cast_uint_to_uint16(nullptr, v);
        case DRAKEN_UINT32:  return draken_cast_uint_to_uint32(nullptr, v);
        case DRAKEN_UINT64:  return draken_cast_uint_to_uint64(nullptr, v);
        case DRAKEN_FLOAT64: return draken_cast_float_to_float64(nullptr, v);
        default:             return draken_error_sentinel("widen: no kernel for target");
    }
}

}  // namespace opteryx::engine
