#pragma once
// draken/ops/int128_compare.h — compare_vector for DRAKEN_DECIMAL128 (int128 unscaled).
//
// UNIFORM PATH ONLY (CLAUDE.md §11 default posture): reads a_data[a.selection[i]]
// OP b_data[b.selection[i]] for every logical row, with no shape (dense/dict/
// constant) discrimination. draken_compare_dv's other type arms (int64_compare.h)
// carry dict/constant fast paths that needed explicit architect ratification
// (§11's "Already ratified" list); this kernel deliberately does not add any —
// it exists to close a functional gap (DECIMAL128 had no compare kernel at all,
// see compare_dv.cpp), not to specialize an existing one.
//
// SCALE CONTRACT: __int128 ordering on the raw unscaled value equals DECIMAL128
// ordering PROVIDED both operands share one scale. compiled_expression.pyx's
// mixed-numeric routing (`draken_numeric_cmp`) already guarantees this: a
// same-type/same-scale DECIMAL128 pair is the ONLY thing routed to
// draken_compare_dv (and thus to this kernel); anything mismatched goes to
// draken_numeric_cmp instead. Same invariant DRAKEN_DECIMAL relies on for its
// i64_compare_vector reuse (see compare_dv.cpp).
//
// NULL SEMANTICS: matches int64_compare.h — output row is null (validity bit 0,
// data bit 0) if EITHER operand row is null. Validity is the AND of both inputs'
// (nullptr when neither operand carries nulls).

#include <stdint.h>

#include "core/buffers.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"   // cmp_alloc_bool_buf / cmp_and_validity (shared helpers)

namespace draken { namespace ops {

struct CmpEq128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a == b; } };
struct CmpNe128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a != b; } };
struct CmpGt128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a >  b; } };
struct CmpGe128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a >= b; } };
struct CmpLt128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a <  b; } };
struct CmpLe128 { static inline bool apply(__int128 a, __int128 b) noexcept { return a <= b; } };

template<typename Op>
static inline VecResult compare_vector_i128_impl(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("i128_compare_vector: operand lengths must match");

    const __int128* a_data = static_cast<const __int128*>(a.data);
    const __int128* b_data = static_cast<const __int128*>(b.data);
    const uint32_t* a_sel  = a.selection;
    const uint32_t* b_sel  = b.selection;

    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try {
        dst = cmp_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }

    if (out_null == nullptr) {
        for (uint32_t i = 0; i < n; ++i) {
            if (Op::apply(a_data[a_sel[i]], b_data[b_sel[i]]))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if ((out_null[i >> 3] >> (i & 7)) & 1u) {
                if (Op::apply(a_data[a_sel[i]], b_data[b_sel[i]]))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult i128_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) {
    switch (op) {
        case 0: return compare_vector_i128_impl<CmpEq128>(a, b);
        case 1: return compare_vector_i128_impl<CmpNe128>(a, b);
        case 2: return compare_vector_i128_impl<CmpGt128>(a, b);
        case 3: return compare_vector_i128_impl<CmpGe128>(a, b);
        case 4: return compare_vector_i128_impl<CmpLt128>(a, b);
        default: return compare_vector_i128_impl<CmpLe128>(a, b);
    }
}

}} // namespace draken::ops
