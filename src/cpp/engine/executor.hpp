#pragma once
// src/cpp/engine/executor.hpp — the morsel-driven parallel pipeline executor.
//
// Runs ONE pipeline at degree `dop`. dop worker threads each pull DISJOINT morsels from
// the source (dynamic assignment = load balance), push them through the operator chain,
// and sink into thread-LOCAL state (lock-free); each worker combines its local into the
// shared global once at the end; finalize runs once. Serial is just dop=1 — no separate
// path. No Python: the loop, operators and states are all C++.
//
// Slice 1 uses std::thread per worker; the shared CppThreadPool is wired in a later slice.

#include <memory>
#include <thread>
#include <vector>

#include "operator.hpp"

namespace opteryx::engine {

// Run `p` at degree `dop`. Returns the finalized GLOBAL SINK state (the caller — the next
// pipeline's source, or the output-queue drain — reads the result from it). The first
// non-OK status any worker hit lands in `err` (0 == OK); on error finalize is skipped.
inline std::unique_ptr<GlobalSinkState>
run_pipeline(Pipeline& p, int dop, ErrCtx& err) {
    if (dop < 1) dop = 1;
    std::unique_ptr<GlobalSourceState> gsrc = p.source->make_global();
    std::unique_ptr<GlobalSinkState>   gsink = p.sink->make_global();
    std::vector<ErrCtx> errs(static_cast<size_t>(dop));

    auto worker = [&](int w) {
        ErrCtx& e = errs[static_cast<size_t>(w)];
        std::unique_ptr<LocalSourceState> lsrc = p.source->make_local(*gsrc);
        std::unique_ptr<LocalSinkState>   lsink = p.sink->make_local(*gsink);
        std::vector<std::unique_ptr<OperatorState>> op_states;
        op_states.reserve(p.operators.size());
        for (Operator* op : p.operators) op_states.push_back(op->make_state());

        MorselPtr in, out;
        while (true) {
            SourceResult sr = p.source->get_morsel(*gsrc, *lsrc, in, e);
            if (e.code != 0) return;
            if (sr == SourceResult::FINISHED) break;

            bool dropped = false;
            for (size_t i = 0; i < p.operators.size(); ++i) {
                OpResult orr = p.operators[i]->execute(in, *op_states[i], out, e);
                if (e.code != 0) return;
                if (orr == OpResult::NEED_INPUT) { dropped = true; break; }
                // (HAVE_MORE handled in a later slice; slice 1 operators are EMIT-only.)
                in = std::move(out);
            }
            if (dropped) continue;

            p.sink->sink(in, *gsink, *lsink, e);
            if (e.code != 0) return;
        }
        p.sink->combine(*gsink, *lsink, e);
    };

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(dop));
    for (int w = 0; w < dop; ++w) threads.emplace_back(worker, w);
    for (std::thread& t : threads) t.join();

    for (ErrCtx& e : errs) {
        if (e.code != 0) { err = e; return gsink; }  // skip finalize on any worker error
    }
    p.sink->finalize(*gsink, err);
    return gsink;
}

}  // namespace opteryx::engine
