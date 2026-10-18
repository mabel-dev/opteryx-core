// draken/ops/compare_dv.cpp — implementation of draken_compare_dv.
//
// See compare_dv.h for contract. Dispatches by left vector type to the
// existing kernel (i64_compare_vector / float_compare_vector<double>),
// adopts the kernel-allocated buffers into the arena, packs into an
// arena-allocated DrakenVector struct.

#include "ops/compare_dv.h"

#include <cstring>

#include "core/alloc.h"
#include "core/frame_arena.h"
#include "core/vector_alloc.h"   // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/int64_compare.h"
#include "ops/int128_compare.h"  // i128_compare_vector (DECIMAL128)
#include "ops/uuid_ops.h"         // uuid_compare_vector (UUID)
#include "ops/float_ops.h"
#include "ops/fixed_int_ops.h"   // i8/i16/i32 + u8/u16/u32 compare_vector
#include "ops/uint64_compare.h"  // u64_compare_vector (UINT64)
#include "ops/string_compare.h"  // str_compare_vector (VARCHAR/NVARCHAR/VARBINARY)
#include "ops/bool_compare.h"    // bool_compare_vector (DRAKEN_BOOL, bit-packed)

#include <vector>

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
    out->flags        = vr.flags;

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


// The type dispatch, shared by the row path and the compressed (value-array) path
// below. false = this pair is not handled here (the caller declines). Throws on a
// kernel-detected violation.
static bool compare_typed(const DrakenVector& l, const DrakenVector& r, int op_code, VecResult* out) {
    switch (l.type) {
        case DRAKEN_INT64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_DECIMAL:
            // DECIMAL (int64 unscaled) ordering == int64 ordering PROVIDED both
            // operands share one scale — which the binder guarantees for every
            // compare it emits (comparison literals are materialized at the
            // column's own scale; cross-scale columns are cast upstream).
            // TIMESTAMP64 is int64 storage; ordering on the unscaled
            // microseconds-since-epoch value is identical to int64
            // ordering. Same kernel.
            *out = draken::ops::i64_compare_vector(l, r, op_code);
            break;
        case DRAKEN_DECIMAL128:
            // int128 unscaled ordering == DECIMAL128 ordering PROVIDED both
            // operands share one scale — compiled_expression.pyx's mixed-
            // numeric routing (draken_numeric_cmp) guarantees this: a
            // same-type/same-scale DECIMAL128 pair is the only DECIMAL128
            // input that ever reaches here (mismatched pairs route to
            // draken_numeric_cmp instead, same as the DECIMAL case above).
            // Until this case existed every DECIMAL128 comparison declined
            // to nullptr, which on the native ExprFilter (no fallback)
            // raised err_op=11 with no message — the TPC-DS Q04 year-over-
            // year ratio predicate (a division of two DECIMAL128 aggregates
            // compared to another) hit exactly this.
            *out = draken::ops::i128_compare_vector(l, r, op_code);
            break;
        case DRAKEN_UUID:
            // memcmp order of the 16 bytes (uuid_ops.h); literals arrive as UUID
            // constant vectors, parsed at bind time.
            *out = draken::ops::uuid_compare_vector(l, r, op_code);
            break;
        case DRAKEN_INT8:
            *out = draken::ops::i8_compare_vector(l, r, op_code);
            break;
        case DRAKEN_INT16:
            *out = draken::ops::i16_compare_vector(l, r, op_code);
            break;
        case DRAKEN_INT32:
            *out = draken::ops::i32_compare_vector(l, r, op_code);
            break;
        // Unsigned integers compare in their own domain — a value at or above
        // the signed midpoint sits in a negative slot, so routing these through
        // the signed kernels would invert the ordering. The kernels already
        // exist (and are registered in hash.h's dispatch table); this switch
        // simply never wired them in, so every unsigned comparison declined to
        // the fallback and, on the relocated native ExprFilter, raised
        // err_op=11 — which is what forced unsigned predicate inputs to fail
        // the whole scan closed.
        case DRAKEN_UINT8:
            *out = draken::ops::u8_compare_vector(l, r, op_code);
            break;
        case DRAKEN_UINT16:
            *out = draken::ops::u16_compare_vector(l, r, op_code);
            break;
        case DRAKEN_UINT32:
            *out = draken::ops::u32_compare_vector(l, r, op_code);
            break;
        case DRAKEN_UINT64:
            *out = draken::ops::u64_compare_vector(l, r, op_code);
            break;
        case DRAKEN_FLOAT64:
            *out = draken::ops::float_compare_vector<double>(l, r, op_code);
            break;
        case DRAKEN_FLOAT32:
            *out = draken::ops::float_compare_vector<float>(l, r, op_code);
            break;
        case DRAKEN_DATE32:
            // DATE32 is int32 storage (days-since-epoch); ordering on
            // the underlying int32 is identical to date ordering. Same
            // kernel as DRAKEN_INT32.
            *out = draken::ops::i32_compare_vector(l, r, op_code);
            break;
        case DRAKEN_BOOL:
            // BOOL is BIT-PACKED (one bit per stored value), so `data` is a
            // bitmap and `data[selection[i]]` means *bit* selection[i] — no
            // fixed-width kernel can read it. Its own kernel
            // (ops/bool_compare.h) does, over the same uniform access
            // contract. Ordering is FALSE < TRUE. Until this branch existed
            // every bool comparison declined to nullptr, which on the
            // relocated native ExprFilter (no fallback) raised err_op=11 —
            // that is what forced a BOOL predicate input to fail the whole
            // scan closed (`bool_predicate_input`, R5).
            *out = draken::ops::bool_compare_vector(l, r, op_code);
            break;
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY:
            // All three string types share the german-string storage
            // layout. str_compare_vector handles them uniformly via
            // bytewise compare. (NVARCHAR Unicode-aware ordering is a
            // future enhancement; equality + bytewise ordering is the
            // current contract.)
            *out = draken::ops::str_compare_vector(l, r, op_code);
            break;
        // Decimal compare not yet handled — needs scale info from the
        // logical-type descriptor, which lives on VectorOwner not on
        // DrakenVector. Caller's Python fallback path covers it.
        default:
            return false;
    }
    return true;
}

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
    // NULL-literal comparison: SQL three-valued logic says any comparison
    // against a genuine NULL is UNKNOWN — never true/false, and never an
    // error. DRAKEN_NULL (core/buffers.h) is the self-describing "every row
    // null" sentinel an untyped NULL literal materialises to (see
    // compiled_expression.pyx:_materialise_constant_literal). Short-circuit
    // BEFORE the type-match gate below: an untyped NULL never matches the
    // other side's type, so without this it always falls through to
    // "declined" — fine for callers with a Python fallback, but the
    // pure-nogil engine path (ExprFilterOperator) has none and would hard-
    // error instead (err_op=11, no message). A BOOL result with every
    // validity bit CLEARED encodes UNKNOWN for every row: WHERE-filter
    // masking requires the data bit AND the validity bit to survive
    // (cxx_mask/mask_indices), so the row is dropped regardless of the
    // (unset) data bits — exactly UNKNOWN — and this propagates correctly
    // through NOT/AND/OR, unlike folding to a definite constant FALSE would.
    if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
        const uint32_t nbytes = ((n_rows + 7u) >> 3) > 0u ? ((n_rows + 7u) >> 3) : 1u;
        void* data = draken_malloc(nbytes);
        if (data == nullptr) return nullptr;
        std::memset(data, 0, nbytes);
        uint8_t* validity = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (validity == nullptr) {
            draken_free(data);
            return nullptr;
        }
        std::memset(validity, 0, nbytes);
        VecResult vr;
        vr.data           = data;
        vr.validity       = validity;
        vr.selection      = draken_identity_sel(n_rows);
        vr.owns_selection = false;
        vr.data_length    = n_rows;
        vr.length         = n_rows;
        vr.type           = DRAKEN_BOOL;
        vr.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return finalise_result(vr, arena);
    }
    // Stage B: both operands must share the supported type. Cross-type
    // (e.g. INT64 vs FLOAT64) goes to Python fallback — EXCEPT the string
    // family (VARCHAR/NVARCHAR/VARBINARY), which all share the german-string
    // storage layout and are compared bytewise by ONE kernel (str_compare_vector,
    // dispatched below by left->type) regardless of which of the three tags
    // either side carries. Without this, a VARBINARY column compared against a
    // literal (materialised VARCHAR by default) always declined here — silently
    // falling back to the transitional GIL path for a single predicate, or
    // hard-failing with no fallback inside an AND/OR expression on the native
    // engine (which has none).
    bool left_is_string  = left->type  == DRAKEN_VARCHAR || left->type  == DRAKEN_NVARCHAR
                         || left->type == DRAKEN_VARBINARY;
    bool right_is_string = right->type == DRAKEN_VARCHAR || right->type == DRAKEN_NVARCHAR
                         || right->type == DRAKEN_VARBINARY;
    if (left->type != right->type && !(left_is_string && right_is_string)) {
        return nullptr;
    }

    VecResult vr;
    try {
        // DECIMAL128 / UINT64 / UUID / BOOL compares have no compressed path of
        // their own: one side a non-null constant and the other compressed
        // (draken_is_compressed) compares each value of the compressed side ONCE — a
        // values-only view against the constant broadcast to it — and scatters the
        // verdicts to rows, the same dense BOOL the row loop builds. (The other
        // types reduce constant ⇄ compressed to their scalar paths themselves.)
        const bool blind_type = left->type == DRAKEN_DECIMAL128 || left->type == DRAKEN_UINT64
                             || left->type == DRAKEN_UUID || left->type == DRAKEN_BOOL;
        const bool l_const = draken_is_constant(left) && left->validity == nullptr;
        const bool r_const = draken_is_constant(right) && right->validity == nullptr;
        const DrakenVector* xv = nullptr;
        if (blind_type && r_const && draken_is_compressed(left))       xv = left;
        else if (blind_type && l_const && draken_is_compressed(right)) xv = right;
        if (xv != nullptr) {
            const DrakenVector* cv = (xv == left) ? right : left;
            const uint32_t k = xv->data_length;
            DrakenVector xview = *xv;
            xview.selection   = draken_identity_sel(k);
            xview.length      = k;
            xview.data_length = k;
            xview.flags       = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            xview.validity    = nullptr;   // a compare cannot fail; nulls come from xv's rows
            DrakenVector cview = *cv;
            cview.selection = draken_zero_sel(k);
            cview.length    = k;
            cview.flags     = 0u;
            VecResult kv;
            const bool ok = (xv == left) ? compare_typed(xview, cview, op_code, &kv)
                                         : compare_typed(cview, xview, op_code, &kv);
            if (!ok) return nullptr;
            std::vector<uint8_t> db(k > 0u ? k : 1u, 0u);
            const uint8_t* bits = static_cast<const uint8_t*>(kv.data);
            for (uint32_t j = 0u; j < k; ++j) db[j] = (bits[j >> 3] >> (j & 7u)) & 1u;
            draken_free(kv.data);
            if (kv.validity != nullptr) draken_free(kv.validity);
            if (kv.owns_selection) draken_free(const_cast<uint32_t*>(kv.selection));
            vr = draken_is_constant(xv)
                ? draken::ops::cmp_constant_bool_result(db[0] != 0u, xv->validity, n_rows)
                : draken::ops::cmp_dict_bool_result(db.data(), *xv);
            return finalise_result(vr, arena);
        }
        if (!compare_typed(*left, *right, op_code, &vr)) return nullptr;
    } catch (const std::exception&) {
        // Length mismatch or other kernel-detected violation. We already
        // checked length above, so this is a defence-in-depth catch.
        return nullptr;
    }

    return finalise_result(vr, arena);
}
