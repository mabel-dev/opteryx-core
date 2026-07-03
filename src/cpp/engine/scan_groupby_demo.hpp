#pragma once
// src/cpp/engine/scan_groupby_demo.hpp — slice 5c: REAL scanned data, REAL C++-native
// GROUP BY key -> SUM/COUNT aggregate Sink (combines slice 2's hash-map breaker pattern
// with slice 5b's NULL-aware numeric reduction). Both the key column and the aggregated
// column are real on-disk numeric columns; NULLs in the aggregated column are skipped.
//
// Pipeline: AggVecSource(real pre-pulled morsels) -> GroupSumCountSink: per-worker local
// hash map (key -> {sum, count}), lock-free; combine = merge maps under ONE mutex per
// worker (not the per-row hot path); finalize = nothing further (the merged map IS the
// result). Mirrors the morsel-driven breaker contract proven in slice 2/5b.

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "executor.hpp"
#include "scan_aggregate_demo.hpp"  // agg_is_valid / agg_read_as_double, AggVecSource

namespace opteryx::engine {

struct GroupAgg {
    double sum = 0.0;
    int64_t count = 0;
};

struct GroupSumCountLocal : LocalSinkState {
    std::unordered_map<int64_t, GroupAgg> m;
};
struct GroupSumCountGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int64_t, GroupAgg> result;
};
struct GroupSumCountSink : Sink {
    size_t key_col_idx;
    size_t val_col_idx;
    GroupSumCountSink(size_t key_idx, size_t val_idx)
        : key_col_idx(key_idx), val_col_idx(val_idx) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GroupSumCountGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<GroupSumCountLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx& err) override {
        const DrakenVector& kv = in->columns[key_col_idx].view;
        const DrakenVector& vv = in->columns[val_col_idx].view;
        if (!agg_type_supported(kv.type) || !agg_type_supported(vv.type)) {
            err.code = 1;
            err.msg = "GroupSumCountSink: unsupported column type for this demo's "
                      "numeric dispatch (e.g. DECIMAL) — fail loud, never a silent "
                      "wrong answer";
            return SinkResult::CONTINUE;
        }
        auto& m = static_cast<GroupSumCountLocal&>(ls).m;
        uint32_t n = kv.length;
        for (uint32_t i = 0; i < n; ++i) {
            // A NULL key row is dropped (matches SQL GROUP BY: NULL keys group together
            // in real engines, but this demo's scope is non-null grouping keys only).
            if (!agg_is_valid(kv, i)) continue;
            int64_t key = static_cast<int64_t>(agg_read_as_double(kv, i));
            GroupAgg& acc = m[key];
            if (agg_is_valid(vv, i)) {
                acc.sum += agg_read_as_double(vv, i);
                acc.count += 1;
            }
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<GroupSumCountGlobal&>(gs);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& kv : static_cast<GroupSumCountLocal&>(ls).m) {
            GroupAgg& acc = g.result[kv.first];
            acc.sum += kv.second.sum;
            acc.count += kv.second.count;
        }
    }
    void finalize(GlobalSinkState&, ErrCtx&) override { /* result already merged */ }
};

// Flat, Cython-friendly result row (POD — no unordered_map/unique_ptr marshalling
// needed across the extern boundary).
struct GroupRow {
    int64_t key;
    double sum;
    int64_t count;
};

// Entry point: runs `morsels` through AggVecSource -> GroupSumCountSink at degree `dop`,
// then flattens the merged per-key map into `out` (Cython-friendly POD rows). Pure C++,
// no Python in the run — the flattening happens after the parallel run completes, on the
// single calling thread.
inline void run_group_sum_count(const std::vector<MorselPtr>& morsels, size_t key_col_idx,
                                size_t val_col_idx, int dop, ErrCtx& err,
                                std::vector<GroupRow>& out) {
    AggVecSource src(&morsels);
    GroupSumCountSink snk(key_col_idx, val_col_idx);
    Pipeline p;
    p.source = &src;
    p.sink = &snk;
    auto gsink = run_pipeline(p, dop, err);
    if (err.code != 0) return;
    auto& g = *static_cast<GroupSumCountGlobal*>(gsink.get());
    out.reserve(g.result.size());
    for (auto& kv : g.result) {
        out.push_back(GroupRow{kv.first, kv.second.sum, kv.second.count});
    }
}

}  // namespace opteryx::engine
