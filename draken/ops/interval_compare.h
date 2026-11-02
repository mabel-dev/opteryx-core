#pragma once
// draken/ops/interval_compare.h — INTERVAL compare kernel.
//
// An interval orders by its normalised length (core/interval_slot.h):
//   total_us = months × INTERVAL_MONTH_US + us     (1 month = 30 days, as Postgres)
// computed in int128 so no (months, us) pair can overflow. Equality is the same
// normalised equality — `INTERVAL '1' MONTH = INTERVAL '30' DAY` is TRUE, as in Postgres.

#include <cstdint>
#include <stdexcept>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/interval_slot.h"
#include "core/vector_alloc.h"   // draken_identity_sel
#include "ops/int64_compare.h"   // cmp_alloc_bool_buf, cmp_and_validity
#include "ops/vec_result.h"

namespace draken { namespace ops {

static inline __int128 interval_key(const DrakenVector& v, uint32_t i) noexcept {
    const DrakenIntervalSlot& s = static_cast<const DrakenIntervalSlot*>(v.data)[v.selection[i]];
    return static_cast<__int128>(s.months) * INTERVAL_MONTH_US + s.us;
}

struct IvEq { static inline bool apply(__int128 a, __int128 b) noexcept { return a == b; } };
struct IvNe { static inline bool apply(__int128 a, __int128 b) noexcept { return a != b; } };
struct IvGt { static inline bool apply(__int128 a, __int128 b) noexcept { return a >  b; } };
struct IvGe { static inline bool apply(__int128 a, __int128 b) noexcept { return a >= b; } };
struct IvLt { static inline bool apply(__int128 a, __int128 b) noexcept { return a <  b; } };
struct IvLe { static inline bool apply(__int128 a, __int128 b) noexcept { return a <= b; } };

// Null if EITHER operand row is null (matches int64_compare.h / uuid_ops.h).
template<typename Op>
static inline VecResult interval_compare_vector_impl(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("interval_compare_vector: operand lengths must match");
    if (b.type != DRAKEN_INTERVAL)
        throw std::invalid_argument("interval_compare_vector: both operands must be INTERVAL");

    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try {
        dst = cmp_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }
    for (uint32_t i = 0; i < n; ++i) {
        if (out_null != nullptr && !((out_null[i >> 3] >> (i & 7)) & 1u)) continue;
        if (Op::apply(interval_key(a, i), interval_key(b, i)))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
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

// op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le.
static inline VecResult interval_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) {
    switch (op) {
        case 0: return interval_compare_vector_impl<IvEq>(a, b);
        case 1: return interval_compare_vector_impl<IvNe>(a, b);
        case 2: return interval_compare_vector_impl<IvGt>(a, b);
        case 3: return interval_compare_vector_impl<IvGe>(a, b);
        case 4: return interval_compare_vector_impl<IvLt>(a, b);
        case 5: return interval_compare_vector_impl<IvLe>(a, b);
        default: throw std::invalid_argument("interval_compare_vector: unknown op code");
    }
}

}} // namespace draken::ops
