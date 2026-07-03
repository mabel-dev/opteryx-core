#pragma once
// src/cpp/engine/scan_aggregate_demo.hpp — slice 5b: REAL scanned data, REAL C++-native
// NULL-aware numeric aggregate Sink, REAL production output edge.
//
// Extends slice 2 (synthetic int32 grouped count) + slice 5a (real scan + filter) with
// the missing piece: a genuine reduction over a REAL on-disk numeric column, correctly
// skipping NULLs via the DrakenVector validity bitmap (slice 2's synthetic column was
// always all-valid — real columns aren't). Mirrors scan_filter_demo.hpp's shape: the
// caller (a thin Cython edge) pre-pulls real morsels via the existing native scan's
// next_morsel(), hands them to this pure-C++ entry point, which runs the parallel
// SUM/COUNT entirely in C++ — no Python anywhere in the run.

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <vector>

#include "executor.hpp"

namespace opteryx::engine {

// ---- SOURCE: hands out pre-pulled real morsels, atomic claim (dynamic assignment). -----
struct AggVecSourceGlobal : GlobalSourceState { std::atomic<size_t> next{0}; };
struct AggVecSource : Source {
    const std::vector<MorselPtr>* morsels;
    explicit AggVecSource(const std::vector<MorselPtr>* m) : morsels(m) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<AggVecSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        size_t idx = static_cast<AggVecSourceGlobal&>(gs).next.fetch_add(1);
        if (idx >= morsels->size()) return SourceResult::FINISHED;
        out = (*morsels)[idx];
        return SourceResult::HAVE_MORE;
    }
};

// ---- NULL-aware uniform column access (data[selection[i]], per CLAUDE.md §11). ---------
inline bool agg_is_valid(const DrakenVector& v, uint32_t logical_row) {
    if (v.validity == nullptr) return true;  // no bitmap => every row valid
    return (v.validity[logical_row >> 3] >> (logical_row & 7)) & 1u;
}

// Types this demo's numeric dispatch handles. Checked ONCE per morsel (not per row —
// see agg_check_supported_type) so the hot per-row loop never branches on an unsupported
// type; a fail-loud error is raised before any row is read, never a silent wrong answer
// (CLAUDE.md §1 "no silent degradation" is non-negotiable even in proof-harness code).
inline bool agg_type_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            return true;
        default:
            return false;
    }
}

inline double agg_read_as_double(const DrakenVector& v, uint32_t logical_row) {
    uint32_t phys = v.selection[logical_row];
    switch (v.type) {
        case DRAKEN_INT8:    return static_cast<double>(static_cast<const int8_t*>(v.data)[phys]);
        case DRAKEN_INT16:   return static_cast<double>(static_cast<const int16_t*>(v.data)[phys]);
        case DRAKEN_INT32:   return static_cast<double>(static_cast<const int32_t*>(v.data)[phys]);
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v.data)[phys]);
        case DRAKEN_FLOAT32: return static_cast<double>(static_cast<const float*>(v.data)[phys]);
        case DRAKEN_FLOAT64: return static_cast<double>(static_cast<const double*>(v.data)[phys]);
        default:
            // Caller MUST check agg_type_supported() before the per-row loop — reaching
            // here is an internal invariant break, not a normal-data case.
            std::abort();
    }
}

// ---- SINK: NULL-aware SUM/COUNT over a real numeric column. Local accumulate
//      (lock-free) -> combine (one mutex-guarded add per worker, not the hot path) ->
//      finalize (the merged sum/count; AVG is sum/count at the caller). ------------------
struct SumCountLocal : LocalSinkState { double sum = 0.0; int64_t count = 0; };
struct SumCountGlobal : GlobalSinkState {
    std::atomic<int64_t> count{0};
    std::mutex sum_mtx;     // combine-only contact, not the per-row hot path
    double sum = 0.0;
    double result_sum = 0.0;
    int64_t result_count = 0;
};
struct SumCountSink : Sink {
    size_t col_idx;
    explicit SumCountSink(size_t idx) : col_idx(idx) {}
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<SumCountGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<SumCountLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx& err) override {
        const DrakenVector& v = in->columns[col_idx].view;
        if (!agg_type_supported(v.type)) {
            err.code = 1;
            err.msg = "SumCountSink: unsupported column type for this demo's numeric "
                      "dispatch (e.g. DECIMAL) — fail loud, never a silent wrong answer";
            return SinkResult::CONTINUE;
        }
        auto& l = static_cast<SumCountLocal&>(ls);
        for (uint32_t i = 0; i < v.length; ++i) {
            if (agg_is_valid(v, i)) {
                l.sum += agg_read_as_double(v, i);
                l.count += 1;
            }
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<SumCountGlobal&>(gs);
        auto& l = static_cast<SumCountLocal&>(ls);
        g.count += l.count;
        std::lock_guard<std::mutex> lk(g.sum_mtx);
        g.sum += l.sum;
    }
    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<SumCountGlobal&>(gs);
        g.result_sum = g.sum;
        g.result_count = g.count.load();
    }
};

struct AggDemoStats {
    double sum = 0.0;
    int64_t count = 0;
};

// Entry point: runs `morsels` (real, pre-pulled scan output) through
// AggVecSource -> SumCountSink at degree `dop`. Pure C++, no Python — the only Python in
// this proof is the EDGE that pulled `morsels` via next_morsel() before calling this.
inline AggDemoStats run_sum_count(const std::vector<MorselPtr>& morsels, size_t col_idx,
                                  int dop, ErrCtx& err) {
    AggVecSource src(&morsels);
    SumCountSink snk(col_idx);
    Pipeline p;
    p.source = &src;
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err);
    auto& g = *static_cast<SumCountGlobal*>(gsink.get());
    return AggDemoStats{g.result_sum, g.result_count};
}

}  // namespace opteryx::engine
