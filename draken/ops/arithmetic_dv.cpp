// draken/ops/arithmetic_dv.cpp — implementation of draken_arithmetic_dv.
//
// See arithmetic_dv.h for contract. Dispatches by left vector type +
// op_code to the existing typed kernels in `int64_arithmetic.h` and
// `float_ops.h`, adopts the kernel-allocated buffers into the arena,
// packs into an arena-allocated DrakenVector struct.
//
// Op-code mapping (BCBinaryOpCode):
//   1 PLUS, 2 MINUS, 3 MULTIPLY, 4 DIVIDE, 5 MODULO

#include "ops/arithmetic_dv.h"

#include "core/alloc.h"
#include "core/frame_arena.h"
#include "ops/vec_result.h"
#include "ops/int64_arithmetic.h"
#include "ops/float_ops.h"

namespace {

// Same finalisation pattern as compare_dv: allocate a result struct from
// the arena, adopt the VecResult's owned buffers into the arena's
// tracking. Free the kernel-allocated buffers directly only if the
// struct alloc itself fails (we haven't adopted them yet).
DrakenVector* finalise_result(const VecResult& vr, DrakenFrameArena* arena) {
    DrakenVector* out = static_cast<DrakenVector*>(
        draken_frame_arena_alloc(arena, sizeof(DrakenVector)));
    if (out == nullptr) {
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

    draken_frame_arena_adopt(arena, vr.data);
    if (vr.validity != nullptr) {
        draken_frame_arena_adopt(arena, vr.validity);
    }
    if (vr.owns_selection && vr.selection != nullptr) {
        draken_frame_arena_adopt(arena, const_cast<uint32_t*>(vr.selection));
    }

    return out;
}

// INT64 arithmetic dispatch.
VecResult i64_dispatch(int op_code, const DrakenVector& a, const DrakenVector& b) {
    switch (op_code) {
        case 1: return draken::ops::i64_add(a, b);
        case 2: return draken::ops::i64_sub(a, b);
        case 3: return draken::ops::i64_mul(a, b);
        case 4: return draken::ops::i64_div(a, b);
        case 5: return draken::ops::i64_mod(a, b);
        default: throw std::invalid_argument("i64_dispatch: unsupported op");
    }
}

// FLOAT64 arithmetic dispatch. The float kernels are templated on
// <T, DrakenType TAG> — for FLOAT64 inputs the result type stays FLOAT64.
VecResult f64_dispatch(int op_code, const DrakenVector& a, const DrakenVector& b) {
    switch (op_code) {
        case 1: return draken::ops::float_add<double, DRAKEN_FLOAT64>(a, b);
        case 2: return draken::ops::float_sub<double, DRAKEN_FLOAT64>(a, b);
        case 3: return draken::ops::float_mul<double, DRAKEN_FLOAT64>(a, b);
        case 4: return draken::ops::float_div<double, DRAKEN_FLOAT64>(a, b);
        case 5: return draken::ops::float_mod<double, DRAKEN_FLOAT64>(a, b);
        default: throw std::invalid_argument("f64_dispatch: unsupported op");
    }
}

}  // namespace


extern "C" DrakenVector* draken_arithmetic_dv(
    int                op_code,
    DrakenVector*      left,
    DrakenVector*      right,
    uint32_t           n_rows,
    DrakenFrameArena*  arena
) {
    if (left == nullptr || right == nullptr || arena == nullptr) {
        return nullptr;
    }
    if (left->length != n_rows || right->length != n_rows) {
        return nullptr;
    }
    if (op_code < 1 || op_code > 5) {
        // Only PLUS..MODULO are in scope; INT_DIVIDE/STRING_CONCAT/bitwise
        // are routed elsewhere by the caller.
        return nullptr;
    }
    if (left->type != right->type) {
        // Cross-type goes to Python fallback (handles promotion).
        return nullptr;
    }

    VecResult vr;
    try {
        switch (left->type) {
            case DRAKEN_INT64:
                vr = i64_dispatch(op_code, *left, *right);
                break;
            case DRAKEN_FLOAT64:
                vr = f64_dispatch(op_code, *left, *right);
                break;
            default:
                return nullptr;
        }
    } catch (const std::exception&) {
        // Length mismatch or kernel-side overflow / div-by-zero raised.
        // Surface to caller as NULL — the caller's Python fallback will
        // re-execute and produce the appropriate Python-level error.
        return nullptr;
    }

    return finalise_result(vr, arena);
}
