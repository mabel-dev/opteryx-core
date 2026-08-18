#pragma once
// src/cpp/engine/native_queue_sink.hpp — the engine's terminal sink, plus the two
// numeric-read helpers the ungrouped aggregate shares with it.
//
// QueueSink writes straight into the REAL production MorselQueue — the output edge
// opteryx's cursor drains. Every plan's LAST pipeline ends here (an invariant
// engine.hpp asserts).
//
// This file was `scan_filter_demo.hpp`, the slice-5a proof that the pure C++ engine
// could drive real scan morsels through a filter into the real queue. That proof has
// long since been superseded by the actual engine; its demo scaffolding (VecSource,
// DemoStats, run_filter_to_queue) and NumericFilterOperator — whose only wiring,
// Engine::add_filter, was itself dead — have been removed. What survived is the one
// piece that was never scaffolding (QueueSink) and the two vector-read helpers
// native_aggregate.hpp genuinely uses.

#include <atomic>
#include <cstdint>
#include <memory>

#include "executor.hpp"
#include "morsel_queue.hpp"  // the REAL production output queue (src/cpp/morsel_queue.hpp)

namespace opteryx::engine {

// Read one row of a fixed-width numeric vector as a double, via the uniform
// data[selection[i]] access (buffers.h). Returns 0.0 for a type it does not handle —
// callers gate on their own supported-type check first.
//
// DRAKEN_DECIMAL is int64-backed (precision <= 18) and read bit-identically to
// DRAKEN_INT64 — i.e. the RAW unscaled value. Any caller comparing that against a
// literal must rescale the literal into the same unscaled-integer domain first; this
// function never rescales anything itself.
inline double vec_read_as_double(const DrakenVector& v, uint32_t logical_row) {
    uint32_t phys = v.selection[logical_row];
    switch (v.type) {
        case DRAKEN_INT8:    return static_cast<double>(static_cast<const int8_t*>(v.data)[phys]);
        case DRAKEN_INT16:   return static_cast<double>(static_cast<const int16_t*>(v.data)[phys]);
        case DRAKEN_INT32:   return static_cast<double>(static_cast<const int32_t*>(v.data)[phys]);
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v.data)[phys]);
        case DRAKEN_FLOAT32: return static_cast<double>(static_cast<const float*>(v.data)[phys]);
        case DRAKEN_FLOAT64: return static_cast<double>(static_cast<const double*>(v.data)[phys]);
        case DRAKEN_DECIMAL: return static_cast<double>(static_cast<const int64_t*>(v.data)[phys]);
        default:             return 0.0;
    }
}

inline bool vec_row_is_valid(const DrakenVector& v, uint32_t logical_row) {
    if (v.validity == nullptr) return true;  // NULL bitmap absent => all valid
    return (v.validity[logical_row >> 3] >> (logical_row & 7)) & 1u;
}

// ---- SINK: writes straight into the REAL production MorselQueue. No local/global merge
//      needed (the queue itself is the thread-safe merge point — MPMC, real backpressure).
// Ownership: SHARED, deliberately. This used to be a raw `MorselQueue*` whose
// lifetime was decided by PyMorselQueue.__dealloc__ — i.e. by Python refcounting,
// which cannot see these sinks or the consumer and does not count them. A producer
// inside q_.enqueue(), or the consumer inside wait_dequeue_timed(), was therefore
// standing on memory Python was entitled to free: close() sets a flag and drains,
// but never waits for an in-flight caller to leave. Holding a shared_ptr makes the
// queue outlive every real user regardless of when Python drops its reference —
// last one out frees it.
struct QueueSinkGlobal : GlobalSinkState {
    std::shared_ptr<MorselQueue> q;
    std::atomic<long long> rows_out{0};
    explicit QueueSinkGlobal(std::shared_ptr<MorselQueue> qq) : q(std::move(qq)) {}
};
struct QueueSink : Sink {
    std::shared_ptr<MorselQueue> q;
    explicit QueueSink(std::shared_ptr<MorselQueue> qq) : q(std::move(qq)) {}
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

}  // namespace opteryx::engine
