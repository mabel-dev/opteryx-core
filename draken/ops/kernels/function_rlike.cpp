// draken/ops/kernels/function_rlike.cpp — RLIKE/NOT RLIKE as a genuine
// draken-native kernel.
//
// The pattern operand is ALWAYS a pre-compiled byte-DFA blob, produced at
// plan time by opteryx.compiled.vector_ops.compile_rlike_dfa (RE2's *parser*
// only — see that module's docstring for the blob format and the compiler's
// scope). This kernel never sees raw regex text and has zero RE2 dependency:
// it is a plain transition-table walk, so — unlike RE2 itself — it compiles
// cleanly into both the opteryx_core and standalone rugo wheels, and runs
// with no Python involved at all, satisfying .claude/CLAUDE.md §1/§2's
// "Draken must be able to execute without Python" for this feature.
//
// Signature is the design's func_fn_t (Phase 9a-fn):
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
// Dispatched directly from the nogil DV* VM (BC_FUNCTION's C-native arm) —
// no Python, no nanobind, no GIL. Ctx is a binary_op_ctx (kernel_context.h),
// reused from draken_like: op_code bit0 = negate (RLike vs NotRLike).

#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/kernel_context.h"  // binary_op_ctx
#include "ops/kernels/error_handling.h"
#include "ops/kernels/dfa_walk.h"        // draken_dfa::match — shared byte-DFA walk

namespace {

inline bool rk_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u);
}

// Walk the compiled DFA blob over one subject string. Blob format is defined
// by vector_dfa_compile.pyx's compile_rlike_dfa docstring:
//   u8 version(=1), u8 flags(bit0=anchored_start, bit1=anchored_end),
//   u16 num_states, accept_bitmap, then num_states*256 u16 transitions.
// Returns 1/0 for match/no-match, or -1 for a malformed blob — the blob is
// entirely plan-time-compiler-produced, so -1 means a compiler/kernel format
// drift, not bad user input; the caller fails loud rather than guessing.
inline int rk_dfa_match(const uint8_t* blob, size_t blob_len, const uint8_t* sdata, uint32_t slen) {
    return draken_dfa::match(blob, blob_len, sdata, slen);  // shared walk (dfa_walk.h)
}

}  // namespace

extern "C" {

VecResult draken_rlike(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_rlike: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_rlike: missing bind-time ctx (mode)");
    const int mode = static_cast<const binary_op_ctx*>(ctx)->op_code;
    const bool negate = (mode & 1) != 0;

    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
        return draken_error_sentinel("draken_rlike: string operand required");
    if (p->type != DRAKEN_VARBINARY)
        return draken_error_sentinel("draken_rlike: pattern operand must be a compiled DFA blob");
    if (p->data_length != 1)
        return draken_error_sentinel("draken_rlike: pattern must be a single compiled value");

    const uint32_t n = v->length;
    const size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;

    // NULL pattern -> every row NULL (matches SQL comparison-with-NULL semantics).
    if (p->validity != nullptr && !rk_row_valid(p, 0)) {
        auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (out == nullptr) return draken_error_sentinel("allocation failed");
        std::memset(out, 0, nb_alloc);
        auto* validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0x00, nb_alloc);
        VecResult r{};
        r.data = out;
        r.validity = validity;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length = n;
        r.length = n;
        r.type = DRAKEN_BOOL;
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    }

    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const auto* psa = static_cast<const DrakenStringArena*>(p->data);
    const DrakenStringSlot* pslot = &psa->slots[p->selection[0]];
    const uint8_t* blob = reinterpret_cast<const uint8_t*>(str_data(pslot, psa->arena));
    const size_t blob_len = str_length(pslot);

    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!rk_row_valid(v, i)) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            continue;
        }
        const DrakenStringSlot* vs = &vsa->slots[v->selection[i]];
        const uint8_t* sdata = reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena));
        const uint32_t slen = str_length(vs);
        const int m = rk_dfa_match(blob, blob_len, sdata, slen);
        if (m < 0) {
            draken_free(out);
            if (validity != nullptr) draken_free(validity);
            return draken_error_sentinel("draken_rlike: malformed compiled DFA blob");
        }
        const bool hit = (m != 0);
        if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }

    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // extern "C"
