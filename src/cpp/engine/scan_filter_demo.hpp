#pragma once
// src/cpp/engine/scan_filter_demo.hpp — slice 5a: REAL data, REAL output queue.
//
// Proves the pure C++ engine processes morsels produced by the EXISTING native scan
// (real on-disk Parquet encodings) through a real numeric filter into the REAL
// `MorselQueue` (the production output edge opteryx's cursor drains) — verified against
// actual opteryx query results. The morsels themselves are supplied pre-pulled (the
// caller — a thin Cython edge — loops the existing scan's `next_morsel()`, itself
// already-native compiled code, and hands the vector in); the PARALLEL RUN here is
// 100% C++, no Python, writing straight into the real `MorselQueue`.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "executor.hpp"
#include "morsel_queue.hpp"  // the REAL production output queue (src/cpp/morsel_queue.hpp)
#include "core/alloc.h"        // draken_malloc/draken_free — the owned-buffer allocator
#include "core/vector_owner.h" // VectorOwner — for view/own consistency (see NumericFilterOperator)
#include "native_sort.hpp"     // gather_rows — the engine's one row-compaction path
#include "core/string_slot.h"  // DrakenStringSlot — gather_typed's DRAKEN_VARCHAR case

namespace opteryx::engine {

// ---- SOURCE: hands out pre-pulled real morsels, atomic claim (dynamic assignment). -----
struct VecSourceGlobal : GlobalSourceState { std::atomic<size_t> next{0}; };
struct VecSource : Source {
    const std::vector<MorselPtr>* morsels;
    explicit VecSource(const std::vector<MorselPtr>* m) : morsels(m) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<VecSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        size_t idx = static_cast<VecSourceGlobal&>(gs).next.fetch_add(1);
        if (idx >= morsels->size()) return SourceResult::FINISHED;
        out = (*morsels)[idx];
        return SourceResult::HAVE_MORE;
    }
};

// ---- OPERATOR: numeric `column[col_idx] > threshold` filter on a REAL column — reads
//      the uniform data[selection[i]] access pattern over whatever real encoding the
//      scan produced (dense/dict/constant are all DATA_LENGTH/SELECTION shaped the same
//      way; this reads the value at each logical row uniformly, per CLAUDE.md §11).
//      Dispatches on the real DrakenType tag the column carries. Builds a NEW morsel
//      with the survivor rows selected via a fresh dense identity selection (a real
//      "compact" pass — exactly what a production filter operator does). -----------------
// Comparison operators a SimplePredicate can carry — the closed set of simple,
// single-column `column OP literal` comparisons the query planner recognizes
// (opteryx/managers/execution/parallel_engine.py's _find_native_filter_eligible
// maps "Gt"/"GtEq"/"Lt"/"LtEq"/"Eq"/"NotEq" to these 1:1).
enum class CompareOp : uint8_t { Gt, GtEq, Lt, LtEq, Eq, NotEq };

struct SimplePredicate {
    size_t col_idx;
    CompareOp op;
    double threshold;
};

struct NumericFilterOperator : Operator {
    std::vector<SimplePredicate> predicates;  // ANDed together

    // Multi-predicate constructor — the real (non-demo) shape.
    explicit NumericFilterOperator(std::vector<SimplePredicate> preds)
        : predicates(std::move(preds)) {}
    // Single `column > threshold` convenience constructor — kept for the
    // existing demo/proof-harness callers (run_filter_to_queue,
    // real_filter_pipeline.hpp), unchanged.
    NumericFilterOperator(size_t idx, double t)
        : predicates{SimplePredicate{idx, CompareOp::Gt, t}} {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }

    static bool compare(double v, CompareOp op, double threshold) {
        switch (op) {
            case CompareOp::Gt:    return v > threshold;
            case CompareOp::GtEq:  return v >= threshold;
            case CompareOp::Lt:    return v < threshold;
            case CompareOp::LtEq:  return v <= threshold;
            case CompareOp::Eq:    return v == threshold;
            case CompareOp::NotEq: return v != threshold;
        }
        return false;
    }

    static double read_as_double(const DrakenVector& v, uint32_t logical_row) {
        uint32_t phys = v.selection[logical_row];
        switch (v.type) {
            case DRAKEN_INT8:    return static_cast<double>(static_cast<const int8_t*>(v.data)[phys]);
            case DRAKEN_INT16:   return static_cast<double>(static_cast<const int16_t*>(v.data)[phys]);
            case DRAKEN_INT32:   return static_cast<double>(static_cast<const int32_t*>(v.data)[phys]);
            case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v.data)[phys]);
            case DRAKEN_FLOAT32: return static_cast<double>(static_cast<const float*>(v.data)[phys]);
            case DRAKEN_FLOAT64: return static_cast<double>(static_cast<const double*>(v.data)[phys]);
            // DRAKEN_DECIMAL is int64-backed (precision<=18) — the RAW unscaled
            // value, read bit-identically to DRAKEN_INT64. Comparing it against a
            // double threshold is only correct because the caller
            // (_find_native_aggregate_eligible in parallel_engine.py) rescales the
            // literal to this SAME unscaled-integer domain before building the
            // SimplePredicate — this function never rescales anything itself.
            case DRAKEN_DECIMAL: return static_cast<double>(static_cast<const int64_t*>(v.data)[phys]);
            default:             return 0.0;  // unsupported type for this demo filter
        }
    }

    static bool is_valid(const DrakenVector& v, uint32_t logical_row) {
        if (v.validity == nullptr) return true;  // NULL bitmap absent => all valid
        return (v.validity[logical_row >> 3] >> (logical_row & 7)) & 1u;
    }

    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out, ErrCtx& err) override {
        uint32_t n = in->columns[predicates[0].col_idx].view.length;
        std::vector<uint32_t> survivors;
        survivors.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            bool ok = true;
            for (const SimplePredicate& p : predicates) {
                const DrakenVector& v = in->columns[p.col_idx].view;
                if (!is_valid(v, i) || !compare(read_as_double(v, i), p.op, p.threshold)) {
                    ok = false;
                    break;
                }
            }
            if (ok) survivors.push_back(i);
        }
        if (survivors.empty()) return OpResult::NEED_INPUT;

        // Survivor compaction = the engine's ONE row gather (native_sort.hpp). The
        // hand-rolled compaction this replaces carried three real defects: it read
        // strings as a raw slot array (the canonical layout is data ->
        // DrakenStringArena — buffers.h; SIGSEGV on any filtered VARCHAR morsel),
        // it dropped validity on NON-predicate columns (a surviving row's NULL in
        // another column silently became a value), and it lost the logical-type
        // descriptor (DECIMAL/TIMESTAMP materialization failed downstream).
        std::vector<MorselPtr> ms{in};
        std::vector<uint32_t> row_m(n, 0), row_r(n);
        for (uint32_t i = 0; i < n; ++i) row_r[i] = i;
        out = gather_rows(ms, survivors, 0, survivors.size(), row_m, row_r, in->names, err);
        if (err.code != 0 || out == nullptr) return OpResult::NEED_INPUT;
        return OpResult::EMIT;
    }
};

// ---- SINK: writes straight into the REAL production MorselQueue. No local/global merge
//      needed (the queue itself is the thread-safe merge point — MPMC, real backpressure).
struct QueueSinkGlobal : GlobalSinkState {
    MorselQueue* q;
    std::atomic<long long> rows_out{0};
    explicit QueueSinkGlobal(MorselQueue* qq) : q(qq) {}
};
struct QueueSink : Sink {
    MorselQueue* q;
    explicit QueueSink(MorselQueue* qq) : q(qq) {}
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<QueueSinkGlobal>(q);
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<LocalSinkState>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState& gs, LocalSinkState&, ErrCtx&) override {
        auto& g = static_cast<QueueSinkGlobal&>(gs);
        // put() returns false only when the queue is closed (consumer abandoned,
        // e.g. LIMIT early-exit) — a normal termination signal, not a fault, so a
        // dropped morsel here is correct and must not be counted as emitted.
        if (g.q->put(in)) {
            g.rows_out += in->num_rows();
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) override {}
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

struct DemoStats {
    long long rows_in = 0;
    long long rows_out = 0;
};

// Entry point: runs `morsels` (real, pre-pulled scan output) through
// VecSource -> NumericFilterOperator -> QueueSink(real out_q), at degree `dop`. Pure C++,
// no Python — the only Python in this proof is the EDGE that pulled `morsels` via
// next_morsel() before calling this, and the edge that drains `out_q` after.
inline DemoStats run_filter_to_queue(const std::vector<MorselPtr>& morsels, size_t col_idx,
                                     double threshold, int dop, MorselQueue* out_q,
                                     ErrCtx& err) {
    DemoStats stats;
    for (const MorselPtr& m : morsels) stats.rows_in += m->num_rows();

    VecSource src(&morsels);
    NumericFilterOperator filt(col_idx, threshold);
    QueueSink snk(out_q);
    Pipeline p;
    p.source = &src;
    p.operators = {&filt};
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err);
    stats.rows_out = static_cast<QueueSinkGlobal*>(gsink.get())->rows_out.load();
    return stats;
}

}  // namespace opteryx::engine
