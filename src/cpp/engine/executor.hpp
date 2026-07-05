#pragma once
// src/cpp/engine/executor.hpp — the morsel-driven parallel pipeline executor.
//
// Runs ONE pipeline at degree `dop`. dop worker threads each pull DISJOINT morsels from
// the source (dynamic assignment = load balance), push them through the operator chain,
// and sink into thread-LOCAL state (lock-free); each worker combines its local into the
// shared global once at the end; finalize runs once. Serial is just dop=1 — no separate
// path. No Python: the loop, operators and states are all C++.
//
// Two dispatch backends share one worker body (`run_worker`):
//   - std::thread per call (demo/test-only call sites — no Python pool to plumb through).
//   - the caller's persistent BSThreadPoolBridge (the real, non-demo cutover path).
// A fresh std::thread-per-run-pipeline-call fan-out of `dop` threads that each touch
// Python on first use was found to deadlock CPython 3.14t's free-threaded runtime at
// dop=8: several brand-new OS threads calling PyGILState_Ensure() for the first time
// concurrently can race inside CPython's own new_threadstate()/stop_the_world(), a
// deadlock in the interpreter's thread-attach path, not in this file's locking. A
// pool whose worker threads persist for the life of the query attaches each thread to
// Python at most once and reuses it across pipeline runs, instead of re-running that
// race on every single pipeline invocation.

#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "operator.hpp"
#include "../bs_pool_bridge_c.h"

namespace opteryx::engine {

// Monotonic nanosecond clock for the per-operator telemetry. CLOCK_MONOTONIC is the
// same source the transitional Cython drive loop used; ~20ns per read, taken per morsel
// (not per row), so it is noise against a morsel's kernel work.
inline uint64_t telem_now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
         + static_cast<uint64_t>(ts.tv_nsec);
}

// rows * columns * 8 — the byte estimate the Python operator model reports (bytes_in/out).
inline uint64_t telem_nbytes(const MorselPtr& m) {
    if (!m) return 0;
    return static_cast<uint64_t>(m->num_rows()) * static_cast<uint64_t>(m->num_columns())
         * 8ull;
}

// Per-worker state, shared by both dispatch backends below.
struct WorkerCtx {
    Pipeline*           p;
    GlobalSourceState*  gsrc;
    GlobalSinkState*    gsink;
    std::vector<ErrCtx>* errs;
    int                 w;
};

// One worker's full body: claim disjoint morsels from the source (dynamic assignment =
// load balance), push each through the operator chain, sink into thread-LOCAL state
// (lock-free), combine local into shared global once at the end.
inline void run_worker(WorkerCtx* ctx) {
    Pipeline& p = *ctx->p;
    ErrCtx& e = (*ctx->errs)[static_cast<size_t>(ctx->w)];
    std::unique_ptr<LocalSourceState> lsrc = p.source->make_local(*ctx->gsrc);
    std::unique_ptr<LocalSinkState>   lsink = p.sink->make_local(*ctx->gsink);
    std::vector<std::unique_ptr<OperatorState>> op_states;
    op_states.reserve(p.operators.size());
    for (Operator* op : p.operators) op_states.push_back(op->make_state());

    // Push `m` through operators[stage:] into the sink. An operator may EMIT zero
    // times (NEED_INPUT — fully consumed, drop), once (EMIT), or MANY times
    // (HAVE_MORE — re-driven with the SAME input/state until EMIT/NEED_INPUT). This
    // is what lets ONE input morsel fan out to many outputs (join fan-out, UNNEST)
    // without losing any but the first batch.
    const std::memory_order relaxed = std::memory_order_relaxed;
    std::function<void(MorselPtr, size_t)> push = [&](MorselPtr m, size_t stage) {
        if (e.code != 0) return;
        if (stage == p.operators.size()) {
            OpStats& ss = p.sink->stats;
            ss.calls.fetch_add(1, relaxed);
            ss.rows_in.fetch_add(m ? m->num_rows() : 0, relaxed);
            ss.bytes_in.fetch_add(telem_nbytes(m), relaxed);
            uint64_t t0 = telem_now_ns();
            p.sink->sink(m, *ctx->gsink, *lsink, e);
            ss.exec_ns.fetch_add(telem_now_ns() - t0, relaxed);
            return;
        }
        OpStats& os = p.operators[stage]->stats;
        os.calls.fetch_add(1, relaxed);
        os.rows_in.fetch_add(m ? m->num_rows() : 0, relaxed);
        os.bytes_in.fetch_add(telem_nbytes(m), relaxed);
        MorselPtr out;
        while (true) {
            uint64_t t0 = telem_now_ns();
            OpResult orr = p.operators[stage]->execute(m, *op_states[stage], out, e);
            os.exec_ns.fetch_add(telem_now_ns() - t0, relaxed);  // SELF time — excludes forward
            if (e.code != 0) return;
            if (orr == OpResult::NEED_INPUT) return;       // consumed, no output
            os.rows_out.fetch_add(out ? out->num_rows() : 0, relaxed);
            os.bytes_out.fetch_add(telem_nbytes(out), relaxed);
            push(out, stage + 1);                          // EMIT or HAVE_MORE: forward
            if (e.code != 0) return;
            if (orr == OpResult::EMIT) return;              // this input is drained
            // HAVE_MORE: loop — re-call execute() with the SAME `m` for the next batch.
        }
    };

    OpStats& src = p.source->stats;
    MorselPtr in;
    while (true) {
        if (p.halt != nullptr && p.halt->load(std::memory_order_relaxed)) break;
        uint64_t t0 = telem_now_ns();
        SourceResult sr = p.source->get_morsel(*ctx->gsrc, *lsrc, in, e);
        src.exec_ns.fetch_add(telem_now_ns() - t0, relaxed);
        if (e.code != 0) return;
        if (sr == SourceResult::FINISHED) break;
        src.calls.fetch_add(1, relaxed);
        src.rows_out.fetch_add(in ? in->num_rows() : 0, relaxed);
        src.bytes_out.fetch_add(telem_nbytes(in), relaxed);
        push(in, 0);
        if (e.code != 0) return;
    }
    p.sink->combine(*ctx->gsink, *lsink, e);
}

// Native-task entry matching BSThreadPoolBridge::submit_native's `void(*)(void*)` shape.
inline void run_worker_task(void* raw) {
    run_worker(static_cast<WorkerCtx*>(raw));
}

// Shared setup: global states + per-worker contexts. `dispatch(ctxs)` runs all of them
// to completion (join or pool-wait — whichever backend the caller chose), then this
// checks errors and finalizes exactly as before.
template <typename DispatchFn>
inline std::unique_ptr<GlobalSinkState>
run_pipeline_impl(Pipeline& p, int dop, ErrCtx& err, DispatchFn&& dispatch) {
    if (dop < 1) dop = 1;
    std::unique_ptr<GlobalSourceState> gsrc = p.source->make_global();
    std::unique_ptr<GlobalSinkState>   gsink = p.sink->make_global();
    std::vector<ErrCtx> errs(static_cast<size_t>(dop));
    std::vector<WorkerCtx> ctxs(static_cast<size_t>(dop));
    for (int w = 0; w < dop; ++w) {
        ctxs[static_cast<size_t>(w)] = WorkerCtx{&p, gsrc.get(), gsink.get(), &errs, w};
    }

    dispatch(ctxs);

    for (ErrCtx& e : errs) {
        if (e.code != 0) { err = e; return gsink; }  // skip finalize on any worker error
    }
    p.sink->finalize(*gsink, err);
    return gsink;
}

// Demo/test-only backend: a fresh std::thread per worker, joined before returning. Fine
// at low dop (no Python pool available in these call sites); NOT the real cutover path —
// see the free-threaded deadlock note above before raising dop here.
inline std::unique_ptr<GlobalSinkState>
run_pipeline(Pipeline& p, int dop, ErrCtx& err) {
    return run_pipeline_impl(p, dop, err, [](std::vector<WorkerCtx>& ctxs) {
        std::vector<std::thread> threads;
        threads.reserve(ctxs.size());
        for (WorkerCtx& ctx : ctxs) threads.emplace_back(run_worker, &ctx);
        for (std::thread& t : threads) t.join();
    });
}

// Real cutover backend: submit `dop` native tasks to the caller's persistent
// BSThreadPoolBridge and block on wait_native() — no new OS threads, no new
// PyGILState_Ensure() races. `pool` must outlive this call (the caller owns it for the
// life of the query, per CppThreadPool's existing contract elsewhere in this codebase).
//
// `pool` is an OPAQUE BSThreadPoolBridge* — dispatched via bs_pool_submit_native/
// bs_pool_wait_native (bs_pool_bridge_c.h), NEVER by calling BSThreadPoolBridge's C++
// methods directly on the pointer from this file. BSThreadPoolBridge is a header-only,
// non-virtual class with no separate .cpp; this executor.hpp is compiled into
// _operators.so (-std=c++20 -DNB_FREE_THREADED), while the pool object itself is
// constructed by thread_pool.so (-std=c++17). Those two independently-compiled copies
// of BS::thread_pool do not agree on BS::move_only_function's layout (its C++23
// feature-test branch differs by standard version), so calling detach_task() through
// _operators.so's own copy against an object built by thread_pool.so's copy corrupted
// the pool's internal task queue — reproduced as a real SIGSEGV. Routing through the
// bs_pool_bridge_c.h symbols (resolved via RTLD_GLOBAL to thread_pool.so's own compiled
// code, the same pattern draken_native.so uses for draken_vector_unwrap et al.) ensures
// the pool is only ever touched by the code that was compiled against its true layout.
inline std::unique_ptr<GlobalSinkState>
run_pipeline(Pipeline& p, int dop, ErrCtx& err, void* pool) {
    return run_pipeline_impl(p, dop, err, [pool](std::vector<WorkerCtx>& ctxs) {
        for (WorkerCtx& ctx : ctxs) bs_pool_submit_native(pool, &run_worker_task, &ctx);
        bs_pool_wait_native(pool);
    });
}

}  // namespace opteryx::engine
