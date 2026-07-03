#pragma once
// src/cpp/engine/scan_join_demo.hpp — slice 5d: REAL scanned data on BOTH sides of a
// hash join (build pipeline's Sink builds the hash table; probe pipeline's Operator
// looks up each probe row and fans out matches via HAVE_MORE — extends slice 4's
// synthetic int32 join to genuine on-disk numeric columns, NULL-aware).
//
//   Build:  AggVecSource(real build-side morsels) -> HashBuildSink   [finalize: hash map]
//   Probe:  AggVecSource(real probe-side morsels) -> JoinOperator -> CountSink
//
// A NULL join key (either side) never matches — standard SQL equi-join semantics.

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "executor.hpp"
#include "scan_aggregate_demo.hpp"  // agg_is_valid / agg_read_as_double / agg_type_supported, AggVecSource

namespace opteryx::engine {

// ---- BUILD SINK: key -> count of build rows with that key (an inner-join existence/
//      multiplicity table; sufficient for COUNT(*) verification — the join's degree per
//      key is what fans out on the probe side). Local per-worker map (lock-free) ->
//      combine (merge under one mutex, not the per-row hot path) -> finalize no-op (the
//      merged map IS the result, read-only by the probe operator after). -----------------
struct JoinBuildLocal : LocalSinkState {
    std::unordered_map<int64_t, int64_t> m;  // key -> build-side row count for that key
};
struct JoinBuildGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int64_t, int64_t> table;
};
struct HashBuildSink : Sink {
    size_t key_col_idx;
    explicit HashBuildSink(size_t idx) : key_col_idx(idx) {}
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<JoinBuildGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<JoinBuildLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx& err) override {
        const DrakenVector& kv = in->columns[key_col_idx].view;
        if (!agg_type_supported(kv.type)) {
            err.code = 1;
            err.msg = "HashBuildSink: unsupported join-key column type";
            return SinkResult::CONTINUE;
        }
        auto& m = static_cast<JoinBuildLocal&>(ls).m;
        for (uint32_t i = 0; i < kv.length; ++i) {
            if (!agg_is_valid(kv, i)) continue;  // NULL key never matches (SQL semantics)
            int64_t key = static_cast<int64_t>(agg_read_as_double(kv, i));
            m[key] += 1;
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<JoinBuildGlobal&>(gs);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& kv : static_cast<JoinBuildLocal&>(ls).m) g.table[kv.first] += kv.second;
    }
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

// ---- JOIN OPERATOR: probes the shared (read-only, finalized) build table. One probe
//      row whose key matches a build key with multiplicity M fans out into M "match"
//      output rows via HAVE_MORE (re-driven with the SAME input until drained) — proven
//      in slice 4; here the table holds counts, not row lists, since only the MATCH
//      COUNT is verified (no payload column needed). Output: one int64 column = 1 per
//      matched (build_row, probe_row) pair, batched (matches slice 4's HAVE_MORE proof,
//      not a single giant morsel). ---------------------------------------------------
struct JoinOpState : OperatorState {
    MorselPtr pending_in;
    uint32_t row = 0;
    int64_t remaining_for_row = 0;  // matches left to emit for the CURRENT probe row
};
struct JoinOperator : Operator {
    size_t probe_key_col_idx;
    const std::unordered_map<int64_t, int64_t>* table;
    JoinOperator(size_t idx, const std::unordered_map<int64_t, int64_t>* t)
        : probe_key_col_idx(idx), table(t) {}

    std::unique_ptr<OperatorState> make_state() override { return std::make_unique<JoinOpState>(); }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out, ErrCtx& err) override {
        auto& st = static_cast<JoinOpState&>(st_);
        if (st.pending_in != in) { st.pending_in = in; st.row = 0; st.remaining_for_row = 0; }

        const DrakenVector& kv = in->columns[probe_key_col_idx].view;
        if (!agg_type_supported(kv.type)) {
            err.code = 1;
            err.msg = "JoinOperator: unsupported probe join-key column type";
            return OpResult::NEED_INPUT;
        }
        uint32_t n = kv.length;

        std::vector<int64_t> match_marks;  // one entry (value 1) per matched pair
        match_marks.reserve(8);

        while (st.row < n) {
            if (st.remaining_for_row == 0) {
                if (!agg_is_valid(kv, st.row)) { ++st.row; continue; }  // NULL probe key
                int64_t key = static_cast<int64_t>(agg_read_as_double(kv, st.row));
                auto it = table->find(key);
                st.remaining_for_row = (it == table->end()) ? 0 : it->second;
                if (st.remaining_for_row == 0) { ++st.row; continue; }
            }
            // Push ONE mark at a time and advance st.row the INSTANT this row's matches
            // are exhausted — BEFORE checking the batch cap. Advancing only after the
            // inner loop naturally exited (the old shape) meant a cap that landed exactly
            // on a row's last match returned HAVE_MORE with st.row still pointing at that
            // ALREADY-FINISHED row; re-entry then re-looked-up and re-matched it — a
            // silent 1-in-64 double-count (found via the small reproducible test: 3174
            // extra / 200000 = 1.587%, matching 1/64 = 1.5625% almost exactly).
            match_marks.push_back(1);
            --st.remaining_for_row;
            if (st.remaining_for_row == 0) {
                ++st.row;
            }
            if (match_marks.size() >= 64) {
                out = make_marks_morsel(match_marks);
                return OpResult::HAVE_MORE;
            }
        }
        if (!match_marks.empty()) {
            out = make_marks_morsel(match_marks);
            return OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }

    // Test-only allocation: malloc'd, intentionally never freed (process-lifetime proof
    // binary) — see scan_filter_demo.hpp's NumericFilterOperator for the same convention.
    static MorselPtr make_marks_morsel(const std::vector<int64_t>& marks) {
        uint32_t n = static_cast<uint32_t>(marks.size());
        int64_t* data = static_cast<int64_t*>(std::malloc(n * sizeof(int64_t)));
        uint32_t* sel = static_cast<uint32_t*>(std::malloc(n * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) { data[i] = marks[i]; sel[i] = i; }
        auto m = std::make_shared<CxxMorsel>();
        CxxColumn col;
        col.view.data = data; col.view.selection = sel;
        col.view.data_length = n; col.view.length = n;
        col.view.validity = nullptr; col.view.type = DRAKEN_INT64; col.view.flags = DRAKEN_SEL_IDENTITY;
        col.own = nullptr;
        m->columns.push_back(std::move(col));
        m->names = {"match"};
        return m;
    }
};

// ---- TERMINAL SINK: counts matched rows. -----------------------------------------------
struct CountLocal : LocalSinkState { int64_t count = 0; };
struct CountGlobal : GlobalSinkState { std::atomic<int64_t> count{0}; int64_t result = -1; };
struct CountSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<CountGlobal>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override { return std::make_unique<CountLocal>(); }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx&) override {
        static_cast<CountLocal&>(ls).count += in->num_rows();
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        static_cast<CountGlobal&>(gs).count += static_cast<CountLocal&>(ls).count;
    }
    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<CountGlobal&>(gs);
        g.result = g.count.load();
    }
};

// Entry point: drives the build pipeline to completion, then the probe pipeline through
// JoinOperator -> CountSink, both at degree `dop`. Returns the matched row count. Pure
// C++, no Python in either run — the only Python is the EDGE that pre-pulled both sides'
// real morsels via next_morsel() before calling this.
inline int64_t run_hash_join_count(const std::vector<MorselPtr>& build_morsels,
                                   size_t build_key_col_idx,
                                   const std::vector<MorselPtr>& probe_morsels,
                                   size_t probe_key_col_idx, int dop, ErrCtx& err) {
    AggVecSource bsrc(&build_morsels);
    HashBuildSink bsink(build_key_col_idx);
    Pipeline bp; bp.source = &bsrc; bp.sink = &bsink;
    auto bg = run_pipeline(bp, dop, err);
    if (err.code != 0) return -1;
    auto& table = static_cast<JoinBuildGlobal*>(bg.get())->table;

    AggVecSource psrc(&probe_morsels);
    JoinOperator join(probe_key_col_idx, &table);
    CountSink csink;
    Pipeline pp; pp.source = &psrc; pp.operators = {&join}; pp.sink = &csink;
    auto pg = run_pipeline(pp, dop, err);
    if (err.code != 0) return -1;
    return static_cast<CountGlobal*>(pg.get())->result;
}

}  // namespace opteryx::engine
