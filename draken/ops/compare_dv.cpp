// draken/ops/compare_dv.cpp — implementation of draken_compare_dv.
//
// See compare_dv.h for contract. Dispatches by left vector type to the
// existing kernel (i64_compare_vector / float_compare_vector<double>),
// adopts the kernel-allocated buffers into the arena, packs into an
// arena-allocated DrakenVector struct.

#include "ops/compare_dv.h"

#include "core/alloc.h"
#include "core/frame_arena.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"
#include "ops/float_ops.h"

namespace {

// Build an arena-allocated DrakenVector from a VecResult, adopting all
// owned buffers into the arena's tracking.
//
// On allocation failure of the result struct, frees the VecResult's
// buffers directly (we haven't adopted them yet, so they're still ours
// to clean up) and returns nullptr.
DrakenVector* finalise_result(const VecResult& vr, DrakenFrameArena* arena) {
    DrakenVector* out = static_cast<DrakenVector*>(
        draken_frame_arena_alloc(arena, sizeof(DrakenVector)));
    if (out == nullptr) {
        // Result struct alloc failed; clean up kernel-allocated buffers.
        if (vr.data != nullptr) draken_free(vr.data);
        if (vr.validity != nullptr) draken_free(vr.validity);
        if (vr.owns_selection && vr.selection != nullptr) {
            draken_free(const_cast<uint32_t*>(vr.selection));
        }
        return nullptr;
    }

    out->data         = vr.data;
    out->validity     = vr.validity;
    out->selection    = vr.selection;
    out->data_length  = vr.data_length;
    out->length       = vr.length;
    out->type         = vr.type;

    // Adopt owned buffers into the arena. Selection is only adopted when
    // VecResult declares ownership — otherwise it points at a global
    // (draken_identity_sel / draken_zero_sel) that must never be freed.
    draken_frame_arena_adopt(arena, vr.data);
    if (vr.validity != nullptr) {
        draken_frame_arena_adopt(arena, vr.validity);
    }
    if (vr.owns_selection && vr.selection != nullptr) {
        draken_frame_arena_adopt(arena, const_cast<uint32_t*>(vr.selection));
    }

    return out;
}

}  // namespace


extern "C" DrakenVector* draken_compare_dv(
    int                op_code,
    DrakenVector*      left,
    DrakenVector*      right,
    int16_t            /*left_type_hint*/,
    int16_t            /*right_type_hint*/,
    uint32_t           n_rows,
    DrakenFrameArena*  arena
) {
    // Input validation.
    if (left == nullptr || right == nullptr || arena == nullptr) {
        return nullptr;
    }
    if (left->length != n_rows || right->length != n_rows) {
        return nullptr;
    }
    if (op_code < 0 || op_code > 5) {
        return nullptr;
    }
    // Stage B: both operands must share the supported type. Cross-type
    // (e.g. INT64 vs FLOAT64) goes to Python fallback.
    if (left->type != right->type) {
        return nullptr;
    }

    VecResult vr;
    try {
        switch (left->type) {
            case DRAKEN_INT64:
                vr = draken::ops::i64_compare_vector(*left, *right, op_code);
                break;
            case DRAKEN_FLOAT64:
                vr = draken::ops::float_compare_vector<double>(*left, *right, op_code);
                break;
            default:
                // Not yet covered in Stage B; caller falls back to Python.
                return nullptr;
        }
    } catch (const std::exception&) {
        // Length mismatch or other kernel-detected violation. We already
        // checked length above, so this is a defence-in-depth catch.
        return nullptr;
    }

    return finalise_result(vr, arena);
}
