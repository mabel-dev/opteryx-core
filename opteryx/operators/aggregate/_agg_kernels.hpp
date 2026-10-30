// Hand-written C++ header for the exact-quantile aggregate state (QuantileState).
// Underscore prefix distinguishes from Cython-generated .cpp files.
//
// All types are in namespace opteryx::ungrouped.
// All methods are inline in the header — no separate .cpp needed.

#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>

#include "engine/agg_budgets.hpp"   // kMedianBytes — shared with variables.py's report
#include "ops/float_ops.h"          // draken::ops::fp_total_lt — THE ratified float order,
                                    // imported not re-derived (see finalize_quantile)


namespace opteryx { namespace ungrouped {

// ---------------------------------------------------------------------------
// QuantileState — buffers non-null doubles, computes an exact quantile via
// std::nth_element on finalize. The one state behind the exact-quantile family:
// MEDIAN (p = 0.5) and PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x [DESC]).
//
// Memory guard: a GLOBAL byte budget across every QuantileState buffer, not a
// per-group value cap. An exact quantile inherently buffers every non-null input
// value, so the real OOM risk is the TOTAL across all groups — a per-group
// cap bounded nothing (group count is unbounded) while refusing ordinary
// group sizes. The budget is charged on capacity growth (amortized — one
// atomic op per doubling, never per append) and released on free. Past the
// budget, `overflowed` latches and appends are refused — the engine must
// check and raise (fail loud; a query author who wants approximate opts in
// by name via APPROX_PERCENTILE, the budget never silently decides).
//
// The counter is a per-shared-object static (inline function local). The
// native engine is the only thing that compiles this header, so there is
// exactly one instance and every exact-quantile buffer in the process accounts
// against it. The budget is the family's, not MEDIAN's alone: MEDIAN is
// PERCENTILE_CONT(0.5) under another spelling, and one buffer may serve both. A second .so including this header would get its own counter
// and its own budget — check that before adding an includer.
// ---------------------------------------------------------------------------
constexpr int64_t kMedianBudgetFloorBytes = opteryx::agg_budgets::kMedianFloorBytes;  // 256MB
constexpr int64_t kMedianBudgetBytes      = opteryx::agg_budgets::kMedianBytes;       // 2GB

inline std::atomic<int64_t>& median_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
}

// The CURRENT ceiling, which starts at the floor and ratchets on demand.
//
// Process-wide, like the counter it guards — the two must have the same scope
// or the guard means nothing. That has a consequence worth knowing: while two
// MEDIAN queries overlap they share both the pool and the escalation, so a
// large one raises the ceiling for a small one, and the small one is what dies
// if the total blows 2GB. It resets to the floor whenever the counter returns
// to zero (see _release), so the escalation is effectively per-query for
// non-overlapping queries and shared only for genuinely concurrent ones.
inline std::atomic<int64_t>& median_budget_ceiling() noexcept {
    static std::atomic<int64_t> ceiling{kMedianBudgetFloorBytes};
    return ceiling;
}

// Raise the ceiling by doubling until it covers `needed`, or refuse. Returns
// false only when the hard ceiling genuinely cannot cover the demand — that is
// the fail-loud path, and the caller latches `overflowed` on it.
inline bool median_budget_escalate(int64_t needed) noexcept {
    std::atomic<int64_t>& ceiling = median_budget_ceiling();
    int64_t current = ceiling.load(std::memory_order_relaxed);
    while (current < needed) {
        if (current >= kMedianBudgetBytes) return false;
        int64_t next = current * 2;
        if (next > kMedianBudgetBytes) next = kMedianBudgetBytes;
        // compare_exchange_weak reloads `current` on failure, so a losing
        // racer re-tests against whatever the winner installed rather than
        // doubling a second time on top of it.
        ceiling.compare_exchange_weak(current, next, std::memory_order_relaxed);
    }
    return true;
}

struct QuantileState {
    double* buf = nullptr;
    size_t  size = 0;
    size_t  cap = 0;
    bool    overflowed = false;

    QuantileState() noexcept = default;
    QuantileState(const QuantileState&) = delete;
    QuantileState& operator=(const QuantileState&) = delete;

    QuantileState(QuantileState&& o) noexcept
        : buf(o.buf), size(o.size), cap(o.cap), overflowed(o.overflowed) {
        o.buf = nullptr; o.size = 0; o.cap = 0;
    }

    QuantileState& operator=(QuantileState&& o) noexcept {
        if (this != &o) {
            _release();
            buf = o.buf; size = o.size; cap = o.cap;
            overflowed = o.overflowed;
            o.buf = nullptr; o.size = 0; o.cap = 0;
        }
        return *this;
    }

    ~QuantileState() noexcept { _release(); }

    inline void _release() noexcept {
        if (buf) {
            std::free(buf);
            const int64_t bytes =
                static_cast<int64_t>(cap) * static_cast<int64_t>(sizeof(double));
            if (median_budget_used().fetch_sub(bytes) - bytes == 0) {
                // Last buffer gone: hand the next query the floor rather than
                // whatever ceiling this one ratcheted to.
                median_budget_ceiling().store(kMedianBudgetFloorBytes,
                                              std::memory_order_relaxed);
            }
            buf = nullptr; size = 0; cap = 0;
        }
    }

    // Capacity growth. The budget charges CAPACITY, not values, so the growth
    // curve IS the memory profile — and a 64-slot first allocation made the
    // floor, not the data, the dominant cost whenever groups are small.
    // GROUP BY over a high-cardinality key is the ordinary case that broke:
    // 1e8 rows in 1e8 groups buffers 763MB of doubles and charged 47.7GB, a
    // 64x multiplier on a query holding one value per group.
    //
    // So: start at ONE slot (a singleton group is charged for exactly what it
    // holds), jump straight to 8 on the first growth to skip the churn a
    // 1,2,4 ramp would cost every small group, double while small, then grow
    // by a quarter past kMedianGeoLimit. Doubling is what overshoots large
    // groups — h2o g6's ~10,000-value groups each took a 16,384-slot buffer,
    // 39% waste — and a 1.25x tail bounds that without making small groups
    // pay realloc traffic for it.
    //
    // Worst case across the shapes measured (g6, GROUP BY user_id at 500k/10M
    // /100M distinct, one big group, 200 groups): 1.60x raw, against 64x for
    // the previous curve. Still geometric, so appends stay amortized O(1);
    // a 10-value group now takes 3 reallocs rather than 1, and a query that
    // used to be refused outright now runs.
    static constexpr size_t kMedianGeoLimit = 4096;

    inline size_t _next_cap(size_t current) const noexcept {
        if (current == 0) return 1;
        if (current == 1) return 8;
        return current < kMedianGeoLimit ? current * 2 : current + (current >> 2);
    }

    inline bool _grow(size_t need) noexcept {
        size_t new_cap = _next_cap(cap);
        while (new_cap < need) new_cap = _next_cap(new_cap);
        return _set_cap(new_cap);
    }

    // Reallocate to exactly `new_cap` (> cap) slots, charging the budget first.
    inline bool _set_cap(size_t new_cap) noexcept {
        int64_t delta = static_cast<int64_t>(new_cap - cap)
                        * static_cast<int64_t>(sizeof(double));
        const int64_t used = median_budget_used().fetch_add(delta) + delta;
        if (used > median_budget_ceiling().load(std::memory_order_relaxed)
                && !median_budget_escalate(used)) {
            median_budget_used().fetch_sub(delta);
            overflowed = true;
            return false;
        }
        double* nb = (double*)std::realloc(buf, new_cap * sizeof(double));
        if (!nb) {
            median_budget_used().fetch_sub(delta);
            return false;
        }
        buf = nb;
        cap = new_cap;
        return true;
    }

    inline bool append(double v) noexcept {
        if (size >= cap && !_grow(size + 1)) return false;
        buf[size++] = v;
        return true;
    }

    // Room for exactly `need` values in ONE reallocation — the bulk merge's reserve,
    // sized from the buffers it is about to copy, never the geometric curve above.
    inline bool reserve_exact(size_t need) noexcept {
        return need <= cap || _set_cap(need);
    }

    // Append every value of `src` with one memcpy. The caller has reserved the room
    // (reserve_exact), so this never allocates; `src` is left as it was.
    inline void append_reserved(const QuantileState& src) noexcept {
        if (src.size == 0) return;
        std::memcpy(buf + size, src.buf, src.size * sizeof(double));
        size += src.size;
    }

    // Compute the p-quantile in place. Mutates buf via std::nth_element, but every
    // call selects over the WHOLE buffer, so one buffer may answer several
    // quantiles, in any order (the shared-buffer specs rely on this).
    // Returns 0.0 if size==0; caller must check size first.
    //
    // CONTINUOUS (linear) interpolation — PERCENTILE_CONT, and MEDIAN at p = 0.5 —
    // written with DuckDB's quantile_cont arithmetic so the two agree to the bit:
    //     RN = p * (n - 1);   lo = value at rank floor(RN);   hi = at rank ceil(RN)
    //     d = RN - floor(RN);  result = fma(lo, 1 - d, hi * d)
    // where rank counts in the requested order. DuckDB writes `lo * (1 - d) + hi * d`
    // and leaves the rounding to its compiler: its ARM build contracts that into
    // exactly the fma above (MEASURED, DuckDB 1.5.4: 556 of 556 random
    // interpolations bit-equal, against 464 for `lo + d * (hi - lo)`), while a
    // build without contraction rounds the product sum twice. The fma is spelled out
    // here so the answer is the same on every target, not a property of a flag.
    // `descending` (ORDER BY x DESC) ranks from the top, NOT by rewriting p as
    // 1 - p: (1 - p) * (n - 1) and (n - 1) - p * (n - 1) round differently, and the
    // answer would move. MEDIAN's former `(lo + hi) * 0.5` is replaced by the
    // formula above (an architect decision, 2026-10-23): at most an ulp apart.
    //
    // QUANTILES OVER NaN — RATIFIED SEMANTICS.
    // A quantile is an ORDER STATISTIC, and the order is the one float_ops.h
    // locked on 2026-05-22 and MIN/MAX already answer under: NaN is a VALUE
    // (validity bit set, not a null) that ranks above every other value,
    // including +inf. So a NaN in the input does NOT poison the result — it
    // sorts to the top and reaches the answer only when it is genuinely one
    // of the middle elements. 2000 values of which 24 are NaN: the 24 sit at
    // the top and the median interpolates the 1000th and 1001st smallest,
    // exactly as if they were any other large values. When exactly one of the
    // two selected values is NaN the interpolation is NaN — that is IEEE
    // arithmetic on the selected pair, not NaN propagation through the selection.
    // DESC ranks from the top, so there the NaNs come first.
    //
    // The comparator MUST be fp_total_lt. Raw `<` is not a strict weak
    // ordering in the presence of NaN (every comparison with NaN is false),
    // so std::nth_element's precondition is violated and the partition
    // depends on the order values happened to arrive in — which varies with
    // morsel scheduling. That produced a silently wrong AND unstable MEDIAN.
    //
    // `p` is in [0, 1]; the sinks refuse anything else before a value is buffered.
    inline double finalize_quantile(double p, bool descending) noexcept {
        if (size == 0) return 0.0;
        constexpr auto total_lt = [](double a, double b) noexcept {
            return draken::ops::fp_total_lt<double>(a, b);
        };
        const double rn = p * static_cast<double>(size - 1);
        const double frn = std::floor(rn);
        const double crn = std::ceil(rn);
        const size_t lo_rank = static_cast<size_t>(frn);
        const size_t lo_idx = descending ? size - 1 - lo_rank : lo_rank;
        std::nth_element(buf, buf + lo_idx, buf + size, total_lt);
        const double lo = buf[lo_idx];
        if (crn == frn) return lo;
        // hi is the NEXT value in the requested order. nth_element left every value
        // ranked below lo_idx in [0, lo_idx) and every value above it after, so hi is
        // the smallest of the upper partition (ascending) or the largest of the lower
        // one (descending). Both partitions are non-empty: crn > frn means lo_rank < n-1.
        const double hi = descending
            ? *std::max_element(buf, buf + lo_idx, total_lt)
            : *std::min_element(buf + lo_idx + 1, buf + size, total_lt);
        const double d = rn - frn;
        return std::fma(lo, 1.0 - d, hi * d);
    }
};

}} // namespace opteryx::ungrouped
